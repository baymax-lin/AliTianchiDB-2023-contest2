#pragma once

#include <assert.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <filesystem>
#include <map>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include "../source/zstd/lib/zstd.h"
#include "./struct/ColumnValue.h"
#include "Easyuomap.h"
#include "Hasher.hpp"
#include "LRU.h"
#include "Root.h"
#include "TSDBEngine.hpp"
#include "TSDBIndex.h"
#include "TSDBNode.h"
#include "TSDBTypes.h"
#include "common.h"
#include "config.h"
#include "lz4.h"

#define COLUMNFAMILY_INT_OR_DOUBLE 0
#define COLUMNFAMILY_STR 1

extern volatile bool lruStat;

typedef struct CompressIndex {
    size_t offset;
    size_t size : 31;
    size_t compressed : 1;
} CompressIndex;

// typedef struct IndexHeader {
//     uint64_t blockNum : 32;
//     // uint64_t blockNum : 32;
// } IndexHeader;

static void compressFile_orDie(const char* fname,
                               const char* outName,
                               int cLevel,
                               int nbThreads) {
    fprintf(stdout,
            "Starting compression of %s with level %d, using %d threads\n",
            fname, cLevel, nbThreads);

    /* Open the input and output files. */
    FILE* const fin = fopen_orDie(fname, "rb");
    FILE* const fout = fopen_orDie(outName, "wb");
    /* Create the input and output buffers.
     * They may be any size, but we recommend using these functions to size
     * them. Performance will only suffer significantly for very tiny
     buffers.
     */
    size_t const buffInSize = ZSTD_CStreamInSize();
    void* const buffIn = malloc_orDie(buffInSize);
    size_t const buffOutSize = ZSTD_CStreamOutSize();
    void* const buffOut = malloc_orDie(buffOutSize);

    /* Create the context. */
    ZSTD_CCtx* const cctx = ZSTD_createCCtx();
    CHECK(cctx != NULL, "ZSTD_createCCtx() failed!");

    /* Set any parameters you want.
     * Here we set the compression level, and enable the checksum.
     */
    CHECK_ZSTD(ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, cLevel));
    CHECK_ZSTD(ZSTD_CCtx_setParameter(cctx, ZSTD_c_checksumFlag, 1));
    if (nbThreads > 1) {
        size_t const r =
            ZSTD_CCtx_setParameter(cctx, ZSTD_c_nbWorkers, nbThreads);
        if (ZSTD_isError(r)) {
            fprintf(stderr,
                    "Note: the linked libzstd library doesn't support "
                    "multithreading. "
                    "Reverting to single-thread mode. \n");
        }
    }

    /* This loop read from the input file, compresses that entire chunk,
     * and writes all output produced to the output file.
     */
    size_t const toRead = buffInSize;
    for (;;) {
        size_t read = fread_orDie(buffIn, toRead, fin);
        /* Select the flush mode.
         * If the read may not be finished (read == toRead) we use
         * ZSTD_e_continue. If this is the last chunk, we use ZSTD_e_end.
         * Zstd optimizes the case where the first flush mode is ZSTD_e_end,
         * since it knows it is compressing the entire source in one pass.
         */
        int const lastChunk = (read < toRead);
        ZSTD_EndDirective const mode = lastChunk ? ZSTD_e_end : ZSTD_e_continue;
        /* Set the input buffer to what we just read.
         * We compress until the input buffer is empty, each time flushing
         the
         * output.
         */
        ZSTD_inBuffer input = {buffIn, read, 0};
        int finished;
        do {
            /* Compress into the output buffer and write all of the output to
             * the file so we can reuse the buffer next iteration.
             */
            ZSTD_outBuffer output = {buffOut, buffOutSize, 0};
            size_t const remaining =
                ZSTD_compressStream2(cctx, &output, &input, mode);
            CHECK_ZSTD(remaining);
            fwrite_orDie(buffOut, output.pos, fout);
            /* If we're on the last chunk we're finished when zstd returns 0,
             * which means its consumed all the input AND finished the frame.
             * Otherwise, we're finished when we've consumed all the input.
             */
            finished = lastChunk ? (remaining == 0) : (input.pos == input.size);
        } while (!finished);
        CHECK(input.pos == input.size,
              "Impossible: zstd only returns 0 when the input is completely "
              "consumed!");

        if (lastChunk) {
            break;
        }
    }

    ZSTD_freeCCtx(cctx);
    fclose_orDie(fout);
    fclose_orDie(fin);
    free(buffIn);
    free(buffOut);
}

// static char* createOutFilename_orDie(const char* filename) {
//     size_t const inL = strlen(filename);
//     size_t const outL = inL + 5;
//     void* const outSpace = malloc_orDie(outL);
//     memset(outSpace, 0, outL);
//     strcat((char*)outSpace, filename);
//     strcat((char*)outSpace, ".zst");
//     return (char*)outSpace;
// }

static void decompressFile_orDie(const char* fname, const char* outName) {
    fprintf(stdout, "Starting decompression of %s \n", fname);

    FILE* const fin = fopen_orDie(fname, "rb");
    size_t const buffInSize = ZSTD_DStreamInSize();
    void* const buffIn = malloc_orDie(buffInSize);
    // FILE* const fout = stdout;
    FILE* const fout = fopen_orDie(outName, "wb");
    size_t const buffOutSize = ZSTD_DStreamOutSize(); /* Guarantee to
                               successfully flush at least one complete
                               compressed block in all circumstances. */
    void* const buffOut = malloc_orDie(buffOutSize);

    ZSTD_DCtx* const dctx = ZSTD_createDCtx();
    CHECK(dctx != NULL, "ZSTD_createDCtx() failed!");

    /* This loop assumes that the input file is one or more concatenated zstd
     * streams. This example won't work if there is trailing non-zstd data at
     * the end, but streaming decompression in general handles this case.
     * ZSTD_decompressStream() returns 0 exactly when the frame is completed,
     * and doesn't consume input after the frame.
     */
    size_t const toRead = buffInSize;
    size_t read;
    size_t lastRet = 0;
    int isEmpty = 1;
    while ((read = fread_orDie(buffIn, toRead, fin))) {
        isEmpty = 0;
        ZSTD_inBuffer input = {buffIn, read, 0};
        /* Given a valid frame, zstd won't consume the last byte of the frame
         * until it has flushed all of the decompressed data of the frame.
         * Therefore, instead of checking if the return code is 0, we can
         * decompress just check if input.pos < input.size.
         */
        while (input.pos < input.size) {
            ZSTD_outBuffer output = {buffOut, buffOutSize, 0};
            /* The return code is zero if the frame is complete, but there
            may
             * be multiple frames concatenated together. Zstd will
             automatically
             * reset the context when a frame is complete. Still, calling
             * ZSTD_DCtx_reset() can be useful to reset the context to a
             clean
             * state, for instance if the last decompression call returned an
             * error.
             */
            size_t const ret = ZSTD_decompressStream(dctx, &output, &input);
            CHECK_ZSTD(ret);
            fwrite_orDie(buffOut, output.pos, fout);
            lastRet = ret;
        }
    }

    if (isEmpty) {
        fprintf(stderr, "input is empty\n");
        exit(1);
    }

    if (lastRet != 0) {
        /* The last return value from ZSTD_decompressStream did not end on a
         * frame, but we reached the end of the file! We assume this is an
         * error, and the input was truncated.
         */
        fprintf(stderr, "EOF before end of stream: %zu\n", lastRet);
        exit(1);
    }

    ZSTD_freeDCtx(dctx);
    fclose_orDie(fin);
    fclose_orDie(fout);
    free(buffIn);
    free(buffOut);
}

class ColumnFamily {
   public:
    std::string directory;
    const std::string dbName = "db";
    const std::string blockIndexName = "blkIndex";
    const std::string compressName = "compress";
    std::string name;
    int cfType;

    int fd1, fd2, fd3;  // fd1: dbfile, fd2: bIndex file, fd3: compress file
    char *maddr1, *maddr2;
    size_t offset1;
    uint32_t count;
    std::mutex lock;

    size_t mmapSize1, mmapSize2;

    uint32_t curBlockId;
    // size_t curBlockOffset;

    char* dbBuffer;
    size_t bufOffset;
    // char* compressBuf;
    size_t compressBufSize = ZSTD_compressBound(BlockBufferSize);

    LRUCache<ListNode>* lru;

    std::vector<CompressIndex> comressIndex;

    size_t statF1 = 0;
    size_t statF2 = 0;
    size_t statF3 = 0;

    ColumnFamily(const std::string& directory_,
                 const std::string& _name,
                 int _cfType) {
        cfType = _cfType;
        name = _name;
        directory = directory_ + "/" + _name;
        // create Directory
        std::filesystem::path cfPath = std::filesystem::path(directory);
        std::filesystem::create_directories(cfPath);

        std::string dbFile = directory + "/" + dbName;
        std::string bIndexFile = directory + "/" + blockIndexName;

        // malloc buffer
        dbBuffer = new char[BlockBufferSize];
        // compressBuf = new char[compressBufSize];
        bufOffset = 0;
        offset1 = 0;

        // open file and mmap db/bIndex
        fd1 = open(dbFile.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        fd2 = open(bIndexFile.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        assert(fd1 >= 0);
        assert(fd2 >= 0);

        size_t fileLen1 = lseek(fd1, 0, SEEK_END);
        size_t fileLen2 = lseek(fd2, 0, SEEK_END);

        if (fileLen1 == 0) {
            char temp = 0;
            mmapSize1 = fileReserveSize;
            int r = pwrite(fd1, &temp, 1, mmapSize1 - 1);
            assert(r != -1);
        } else {
            offset1 = fileLen1;
            mmapSize1 = fileLen1;
        }

        if (fileLen2 == 0) {
            char temp = 0;
            mmapSize2 = fileReserveSize;
            int r = pwrite(fd2, &temp, 1, mmapSize2 - 1);
            assert(r != -1);
        } else {
            // offset2 = fileLen2;
            mmapSize2 = fileLen2;
        }

        maddr1 = (char*)mmap(NULL, mmapSize1, PROT_READ | PROT_WRITE,
                             MAP_SHARED, fd1, 0);
        assert(maddr1 != (void*)-1);

        maddr2 = (char*)mmap(NULL, mmapSize2, PROT_READ | PROT_WRITE,
                             MAP_SHARED, fd2, 0);
        assert(maddr2 != (void*)-1);

        // recovery offset/blockId...
        curBlockId = fileLen2 / sizeof(CompressIndex);

        // At least written one index yet
        if (fileLen2 >= sizeof(CompressIndex)) {
            CompressIndex temp =
                *(CompressIndex*)(maddr2 + fileLen2 - sizeof(CompressIndex));
            // decompress last block and copy to current dbbuffer..
            if (curBlockId >= 1) {
                curBlockId--;

                size_t r = ZSTD_decompress(dbBuffer, BlockBufferSize,
                                           maddr1 + temp.offset, temp.size);

                // memcpy(dbBuffer, maddr1 + temp.offset, temp.size);
                // size_t r = temp.size;

                bufOffset = r;
                offset1 = temp.offset;
            } else {
                bufOffset = 0;
                offset1 = fileLen1;
            }
        }
        lru = nullptr;

        if (EN_LRU) {
            if (_cfType == COLUMNFAMILY_INT_OR_DOUBLE) {
                lru = new LRUCache<ListNode>(RnColruCapcity);
            } else if (_cfType == COLUMNFAMILY_STR) {
                lru = new LRUCache<ListNode>(StrColLruCapcity);
            }
        }
    }
    ~ColumnFamily() {
        // store data in buffer (compressed first) to db and index
        if (bufOffset > 0) {
            flushBuffer();
        }

        // release buffer
        delete[] dbBuffer;

        // unmmap db and bIndex
        if (maddr1) {
            munmap((void*)maddr1, mmapSize1);
        }

        if (maddr2) {
            munmap((void*)maddr2, mmapSize2);
        }

        // truncate file
        ftruncate(fd1, offset1);
        ftruncate(fd2, curBlockId * sizeof(CompressIndex));
        // printf("%s size: %ld\n", directory.c_str(), offset1);

        // close file
        close(fd1);
        close(fd2);

        if (lru)
            delete lru;

        // printf("%s: hit:(%ld/%ld %f) fromBuf: %ld, size: %fM\n",
        //        directory.c_str(), statF2, statF1, statF2 * 1.0 / statF1,
        //        statF3, offset1 * 1.0 / 1024 / 1024);
    }

    int resizeFile(size_t expectSize, int fd, char*& maddr, size_t& mmapSize) {
        assert(fd >= 0);
        munmap((void*)maddr, mmapSize);
        maddr = nullptr;
        // mmapSize = 2 * expectSize;
        mmapSize = expectSize;
        mmapSize = mmapSize + 4096 - (mmapSize % 4096);  // align 4k
        char temp = 0;
        int r = pwrite(fd, &temp, 1, mmapSize - 1);
        assert(r != -1);
        maddr = (char*)mmap(NULL, mmapSize, PROT_READ | PROT_WRITE, MAP_SHARED,
                            fd, 0);

        assert(maddr != (void*)-1);
        return 0;
    }
    size_t writeDB(char* buf, size_t size) {
        if (offset1 + size >= mmapSize1) {
            resizeFile(offset1 + size + fileReserveSize, fd1, maddr1,
                       mmapSize1);
        }
        memcpy(maddr1 + offset1, buf, size);
        offset1 += size;
        return 0;
    }
    int appendBlockIndex(size_t blockOffset,
                         size_t blockContentSize,
                         int compressd = 1) {
        size_t offset = curBlockId * sizeof(CompressIndex);
        if (offset + sizeof(CompressIndex) >= mmapSize2) {
            resizeFile(offset + fileReserveSize, fd2, maddr2, mmapSize2);
        }
        CompressIndex temp;
        temp.offset = blockOffset, temp.size = blockContentSize,
        temp.compressed = compressd;
        *(CompressIndex*)(maddr2 + offset) = temp;

        comressIndex.push_back(std::move(temp));
        curBlockId++;
        return 0;
    }

    void persistCompressIndex(int fd, size_t offset) {}

    void recoveryCompressIndex(int fd, size_t offset) {}

    inline bool checkBuffer() {
        // if (cfType == COLUMNFAMILY_INT_OR_DOUBLE)
        return bufOffset + MinReserveBufferSize <= BlockBufferSize;
    }
    void flushBuffer() {
        char* compressBuf = new char[compressBufSize];

        size_t r = ZSTD_compress(compressBuf, compressBufSize, dbBuffer,
                                 bufOffset, zstd_level);
        // memcpy(compressBuf, dbBuffer, bufOffset);
        // size_t r = bufOffset;

        appendBlockIndex(offset1, r);
        writeDB(compressBuf, r);
        bufOffset = 0;

        delete[] compressBuf;
        return;
    }
    size_t add(void* buf, size_t size) {
        assert(bufOffset + size <= BlockBufferSize);
        memcpy(dbBuffer + bufOffset, buf, size);
        bufOffset += size;
        return 0;
    }

    size_t get(char* buf,
               uint32_t blockId,
               size_t offset,
               size_t size = 0,
               bool enableLRU = true) {
        size_t cpySize;
        if (blockId < curBlockId) {
            char* cache = nullptr;

            if (EN_LRU && enableLRU) {
                lru->lock.lock();
                ListNode* node = lru->get(blockId);
                if (node) {
                    cache = node->value;
                    cpySize = (size == 0 ? node->size : size);
                }
            }

            if (lruStat)
                statF1++;
            if (cache) {
                if (lruStat)
                    statF2++;
                memcpy(buf, cache + offset, cpySize);
                if (EN_LRU && enableLRU)
                    lru->lock.unlock();
            } else {
                if (EN_LRU && enableLRU)
                    lru->lock.unlock();

                assert(blockId * sizeof(CompressIndex) <= mmapSize2);

                CompressIndex temp =
                    *(CompressIndex*)(maddr2 + blockId * sizeof(CompressIndex));

                // decompress buffer is unique. So we need to lock this
                // operation
                assert(temp.offset + temp.size <= offset1);
                // decompressBuf
                char* decodeBuffer = new char[compressBufSize];
                size_t r = 0;

                r = ZSTD_decompress(decodeBuffer, compressBufSize,
                                    maddr1 + temp.offset, temp.size);
                // memcpy(decodeBuffer, maddr1 + temp.offset, temp.size);
                // r = temp.size;

                assert(r <= BlockBufferSize);
                cpySize = (size == 0 ? r : size);
                // Only return actual raw data without timestamp
                memcpy(buf, decodeBuffer + offset, cpySize);

                if (EN_LRU && enableLRU)
                    lru->put(blockId, decodeBuffer, r);
                delete decodeBuffer;
            }

        } else {
            cpySize = (size == 0 ? bufOffset : size);
            // Only return actual raw data without timestamp
            memcpy(buf, dbBuffer + offset, cpySize);
            if (lruStat)
                statF3++;
        }

        return cpySize;
    }

    size_t getBuf(char* buf) {
        memcpy(buf, dbBuffer, bufOffset);
        return bufOffset;
    }

    // add column data directly without compression
    size_t addDirect(char* buf, size_t size) {
        writeDB(buf, size);
        return 0;
    }
    // get column data directly without decompression
    char* getDirectRO(size_t offset, size_t size) { return maddr1 + offset; }

    int compressFile() {
        std::string dbFile = directory + "/" + dbName;
        std::string compressFile = directory + "/" + dbName;
        compressFile_orDie(dbFile.c_str(), compressFile.c_str(), zstd_level, 1);
        remove(dbFile.c_str());
        return 0;
    }
    int decompressFile() {
        std::string dbFile = directory + "/" + dbName;
        std::string compressFile = directory + "/" + dbName;
        decompressFile_orDie(compressFile.c_str(), dbFile.c_str());
        remove(compressFile.c_str());
        return 0;
    }
};