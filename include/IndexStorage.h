#pragma once

#include <assert.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <algorithm>
#include <atomic>
#include <filesystem>
#include <map>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include "../source/zstd/lib/zstd.h"
#include "Hasher.hpp"
#include "Root.h"
#include "TSDBEngine.hpp"
#include "TSDBIndex.h"
#include "TSDBNode.h"
#include "TSDBTypes.h"
#include "config.h"
#include "lz4.h"
#include "struct/Row.h"

class IndexStorage {
   private:
    int fd;
    char* buffer;
    char* maddr;
    size_t bufOffset;
    size_t mmapSize = IndexFileIncSpace;

   public:
    std::mutex lock;
    std::string filename;
    uint32_t partId;
    size_t offset;

    IndexStorage(std::string& directory, uint32_t id) {
        offset = bufOffset = 0;
        buffer = new char[IndexStorageBufferSize];

        partId = id;
        filename = directory + "/" + std::to_string(partId);

        fd = open(filename.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        assert(fd >= 0);

        size_t fileLen = lseek(fd, 0, SEEK_END);
        if (fileLen == 0) {
            char temp = 0;
            mmapSize = IndexFileIncSpace;
            int r = pwrite(fd, &temp, 1, mmapSize - 1);
            assert(r != -1);
        } else {
            offset = fileLen;
            mmapSize = fileLen;
        }

        maddr = (char*)mmap(NULL, mmapSize, PROT_READ | PROT_WRITE, MAP_SHARED,
                            fd, 0);
        assert(maddr != (void*)-1);
    }
    ~IndexStorage() {
        if (bufOffset > 0) {
            flushBuffer();
        }

        if (maddr) {
            munmap((void*)maddr, mmapSize);
        }

        delete[] buffer;

        ftruncate(fd, offset);
    }

   private:
    // In index file, each block beginning position has N
    // bytes(CompressSizeByte) to mark compressed size
#define CompressSizeByte sizeof(size_t)

    void flushBuffer() {
        char* compressBuf = new char[BlockCompressBufferUpperBound];
        size_t r = ZSTD_compress(compressBuf, BlockCompressBufferUpperBound,
                                 buffer, bufOffset, zstd_level);
        // appendBlockIndex(offset1, r);
        writeoOut((char*)(&r), CompressSizeByte);
        writeoOut(compressBuf, r);
        bufOffset = 0;
        delete[] compressBuf;
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

    size_t writeoOut(char* buf, size_t size) {
        if (offset + size > mmapSize) {
            resizeFile(offset + size + IndexFileIncSpace, fd, maddr, mmapSize);
        }
        memcpy(maddr + offset, buf, size);
        offset += size;
        return 0;
    }

   public:
    size_t add(void* buf, size_t size) {
        if (!checkBuffer())
            flushBuffer();

        memcpy(buffer + bufOffset, buf, size);
        bufOffset += size;
        return 0;
    }

    char* get(size_t offset) { return maddr + offset; }

    /**
     * return decompressed data size
     * params:
     * buf: Given buffer to store decompressed data
     * blockStartOffset: offset position is the beginning position of a block
     * nextBlockStart: next block beginning position, if there is not next
     * block, the value is set to 0.
     */
    size_t getBlockData(char* buf,
                        size_t blockStartOffset,
                        size_t& nextBlockStart) {
        size_t compressedSize = *(size_t*)(maddr + blockStartOffset);
        size_t r = ZSTD_decompress(buf, BlockBufferSize,
                                   maddr + blockStartOffset + CompressSizeByte,
                                   compressedSize);
        size_t next = blockStartOffset + compressedSize + CompressSizeByte;

        if (next >= offset)
            nextBlockStart = 0;
        else
            nextBlockStart = next;

        return r;
    }

    inline size_t storageSize() { return offset; }

    inline bool checkBuffer() {
        // if (cfType == COLUMNFAMILY_INT_OR_DOUBLE)
        return bufOffset + IndexFileMinReserve <= IndexStorageBufferSize;
    }
};