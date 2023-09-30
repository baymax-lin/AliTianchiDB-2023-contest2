//
// You should modify this file.
//
// Refer TSDBEngineSample.h to ensure that you have understood
// the interface semantics correctly.
//

#pragma once

#include <omp.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <map>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include "ColumnFamily.h"
#include "Easyuomap.h"
#include "Hasher.hpp"
#include "IndexStorage.h"
#include "RTCompressDB.h"
#include "TSDBEngine.hpp"
#include "TSDBIndex.h"
#include "VinIDStorage.h"

// #include <tbb/concurrent_hash_map.h>

namespace LindormContest {

class TSDBEngineImpl : public TSDBEngine {
   public:
    /**
     * This constructor's function signature should not be modified.
     * Our evaluation program will call this constructor.
     * The function's body can be modified.
     */
    explicit TSDBEngineImpl(const std::string& dataDirPath);

    int connect() override;

    int createTable(const std::string& tableName,
                    const Schema& schema) override;

    int shutdown() override;

    int write(const WriteRequest& wReq) override;

    int executeLatestQuery(const LatestQueryRequest& pReadReq,
                           std::vector<Row>& pReadRes) override;

    int executeTimeRangeQuery(const TimeRangeQueryRequest& trReadReq,
                              std::vector<Row>& trReadRes) override;

    int executeAggregateQuery(const TimeRangeAggregationRequest& aggregationReq,
                              std::vector<Row>& aggregationRes) override;

    int executeDownsampleQuery(const TimeRangeDownsampleRequest& downsampleReq,
                               std::vector<Row>& downsampleRes) override;

    ~TSDBEngineImpl() override;

   public:
    // Protect global map, such as outFiles, vinMutex defined below.
    // std::mutex globalMutex;
    // Append new written row to this file. Cache the output stream for each
    // file. One file for a vin.
    // std::unordered_map<Vin, std::ofstream*, VinHasher, VinHasher> outFiles;
    // One locker for a vin. Lock the vin's locker when reading or writing
    // process touches the vin.
    // std::unordered_map<Vin, std::mutex*, VinHasher, VinHasher> vinMutex;
    // How many columns is defined in schema for the sole table.
    int columnsNum;
    // The column's type for each column.
    ColumnType* columnsType;
    // The column's name for each column.
    std::string* columnsName;
    DbSchema* dbSchema;

    // std::unordered_map<Vin, TSDBIndex*, VinHasher, VinHasher> glbIndex;
    LindormContest::EasyUnorderedMap* eMap;
    std::mutex gLock;
    volatile bool reqLock = true;
    bool firstLoad = true;

    CompressDB* dbParts[DatabasePartNum];

    std::string indexDirectory;
    std::atomic<int64_t> queryCount = 0;
    size_t totalDownsampleCount = 0;
    volatile bool infoShown = false;

    // analyze thread
    std::unordered_set<size_t> concurrencyVinset;
    std::mutex vinsetLock;
    int maxConcurrencyVin = 0;
    std::thread* timerThread = nullptr;

   private:
    bool shutdownCalled = false;
    uint64_t getReqCols(const std::set<std::string>& requestedColumns);

    void recovery();

    void informVin(const Vin& vin) {
        vinsetLock.lock();
        concurrencyVinset.insert(VinHasher()(vin));
        maxConcurrencyVin =
            std::max(maxConcurrencyVin, (int)concurrencyVinset.size());

        vinsetLock.unlock();
    }

    void timerStat();
    /**
     * Read a Column family
     *    Read each block in this cf
     *    Read tail buffer in this cf
     *    TO REBUILD INDEX
     *
     *  cf: given column family
     *  dbSchema: database schenma
     *  reInsertIndex: whether insert new index and update latest Or not
     *  (Necessary if insert first CF)
     *  sortIndex: whether resort index when joined column number ==
     * index.size (Required if insert last CF)
     */
    int rebuildColumnFamilyIndex(int pid,
                                 ColumnFamily* cf,
                                 DbSchema* dbSchema,
                                 bool reInsertIndex,
                                 bool sortIndex) {
        char* buf = new char[BlockCompressBufferUpperBound];

        DbSchema* schema = dbSchema;

        printf("pid: %d, blknum: %d\n", pid, cf->curBlockId);
        for (int i = 0; i < cf->curBlockId + 1; ++i) {
            size_t bufSize, offset = 0;
            if (i == cf->curBlockId) {  // current dbbuffer( no compressed yet)
                bufSize = cf->getBuf(buf);
            } else {
                bufSize = cf->get(buf, i, 0, 0, false);
            }
            if (i % 250 == 0)
                printf("pid: %d, idx: %d, blksize: %ld\n", pid, i, bufSize);
            while (offset < bufSize) {
                // if (offset % 200000 == 0)
                //     std::cout << "point break: " << offset << std::endl;
                size_t prev_offset = offset;

                VinID vid = *(VinID*)(buf + offset);
                offset += sizeof(VinID);

                Timestamp ts = *(Timestamp*)(buf + offset);
                offset += sizeof(Timestamp);

                size_t contentOffset = offset;

                int cfType = cf->cfType;
                if (cfType == COLUMNFAMILY_INT_OR_DOUBLE) {
                    offset += schema->rnSize;  // if cf is INT_OR_DOUBLE type,
                                               // plus a fixed offset
                } else if (cfType == COLUMNFAMILY_STR) {
                    for (int cI = 0; cI < schema->cfTypeNum[COLUMNFAMILY_STR];
                         ++cI) {
                        int32_t strLen = *(int32_t*)(buf + offset);
                        offset += strLen + sizeof(int32_t);
                    }
                }
                auto& vin = schema->vinMap[vid];
                auto it = eMap->query(vin);
                assert(it);

                int idx = it->nextRefIndex(reInsertIndex);
                auto& ref = it->serial[idx];
                size_t wrSize = 0;
                if (cfType == COLUMNFAMILY_INT_OR_DOUBLE) {
                    ref.blkRnId = i;
                    ref.rnOffset = contentOffset;
                    wrSize +=
                        dbSchema->rnSize + sizeof(Timestamp) + sizeof(VinID);

                } else if (cfType == COLUMNFAMILY_STR) {
                    ref.blkStrId = i;
                    ref.strOffset = contentOffset;
                    ref.strSize = offset - contentOffset;
                    wrSize += ref.strSize + sizeof(Timestamp) + sizeof(VinID);
                }

                it->addSize(wrSize);

                if (reInsertIndex) {
                    ref.timestamp = ts;
                    // it->updateLatest(idx);
                }
                if (!reInsertIndex && sortIndex &&
                    (it->getJoinCount() % it->getIndexCount() == 0)) {
                    it->postProcess();
                }
            }
        }

        delete[] buf;

        return 0;
    }

    int rebuildDataBaseIndex() {
        // double start = omp_get_wtime();

        auto tstart = std::chrono::steady_clock::now();
        for (int cfnum = 0; cfnum < ColumnFamilyNum; ++cfnum) {
#pragma omp parallel for
            // num_threads(DatabasePartNum)
            for (int i = 0; i < DatabasePartNum; ++i) {
                // printf("%d column, %d db\n", cfnum, i);
                // assert(dbParts[i]);
                // std::cout << dbParts[i] << std::endl;
                rebuildColumnFamilyIndex(i, dbParts[i]->cf[cfnum], dbSchema,
                                         cfnum == 0,
                                         cfnum == ColumnFamilyNum - 1);
            }

            auto t1 = std::chrono::steady_clock::now();
            double elapsedS =
                std::chrono::duration_cast<std::chrono::milliseconds>(t1 -
                                                                      tstart)
                    .count() *
                1.0 / 1000;

            std::cout << "rebuild cf " << cfnum << " elapsed " << elapsedS
                      << "s." << std ::endl;
        }

        return 0;
    }

    void PartitionAllVins(std::vector<std::vector<TSDBIndex*>>& partitions,
                          EasyUnorderedMap* eMap) {
        assert(partitions.size() == DatabasePartNum);
        for (int i = 0; i < eMap->cap; ++i) {
            auto& t = eMap->hashSpace[i];
            if (t.count > 0) {
                if (t.count >= 1) {
                    TSDBIndex* index = t.first.hIndex;
                    uint16_t partId = index->getAssignPart();
                    partitions[partId].emplace_back(index);
                }

                for (auto it = t.nodevec.begin(); it != t.nodevec.end(); ++it) {
                    TSDBIndex* index = it->hIndex;
                    uint16_t partId = index->getAssignPart();
                    partitions[partId].emplace_back(index);
                }
            }
        }
    }
    union UnionHeader {
        LindormContest::RowIndex rIndex;
        struct {
            VinID vinID;
            // uint32_t rowCount;
            uint64_t rowCount : 20;
            uint64_t dataSize : 32;
        } header;
    };
    void storeVinIndex(IndexStorage* indexStorage, TSDBIndex* index) {
        UnionHeader ustruct;
        ustruct.header.vinID = index->getVinID();
        ustruct.header.rowCount = index->serial.size();
        ustruct.header.dataSize = index->dataSize;
        indexStorage->add(&ustruct, sizeof(UnionHeader));
        for (auto& f : index->serial) {
            indexStorage->add(&f, sizeof(LindormContest::RowIndex));
        }
    }

    // thread-safe
    void storeIndex() {
        assert(sizeof(UnionHeader) == sizeof(LindormContest::RowIndex));

        IndexStorage* indexStorages[DatabasePartNum];
        for (int i = 0; i < DatabasePartNum; ++i) {
            indexStorages[i] = new IndexStorage(indexDirectory, i);
        }

        std::vector<std::vector<TSDBIndex*>> vIndexParts(DatabasePartNum);
        PartitionAllVins(vIndexParts, eMap);
// omp for parallel
#pragma omp parallel for
        // num_threads(4)
        for (int i = 0; i < DatabasePartNum; ++i) {
            for (auto& vIndex : vIndexParts[i])
                storeVinIndex(indexStorages[i], vIndex);
        }

        for (int i = 0; i < DatabasePartNum; ++i) {
            delete indexStorages[i];
        }
    }

    // thread-safe
    void recoveryIndexFromIndexStorage(IndexStorage* indexStorage,
                                       DbSchema* dbSchema,
                                       EasyUnorderedMap* eMap) {
        size_t nowOffset = 0;
        size_t nextOffset = 0;
        if (indexStorage->storageSize() == 0) {
            printf("index file %d is empty!\n", indexStorage->partId);
            return;
        }

        char* buffer = new char[BlockBufferSize];
        VinID vinID = 0;
        uint32_t rowCount = 0;
        size_t dataSize = 0;
        TSDBIndex* vIndex = nullptr;  // pointer of vin index struct
        do {
            nowOffset = nextOffset;
            size_t blockSize =
                indexStorage->getBlockData(buffer, nowOffset, nextOffset);
            assert(blockSize % sizeof(LindormContest::RowIndex) == 0);
            size_t num = blockSize / sizeof(LindormContest::RowIndex);

            LindormContest::RowIndex* ptrRowIndex =
                (LindormContest::RowIndex*)buffer;
            for (int i = 0; i < num; ++i) {
                if (rowCount == 0) {
                    UnionHeader ustruct = *(UnionHeader*)(ptrRowIndex + i);
                    vinID = ustruct.header.vinID;
                    rowCount = ustruct.header.rowCount;
                    dataSize = ustruct.header.dataSize;
                    vIndex = dbSchema->vinIndexMap[vinID];
                    vIndex->setSize(dataSize);
                    assert(vIndex);
                } else {
                    vIndex->serial.emplace_back(*(ptrRowIndex + i));
                    rowCount--;
                    // All index of this Vin has been inserted. do post process
                    if (rowCount == 0 && vIndex) {
                        vIndex->postProcess();
                    }
                }
            }

        } while (nextOffset != 0);

        delete[] buffer;
    }

    int rebuildDBIndex() {
        // double start = omp_get_wtime();

        IndexStorage* indexStorages[DatabasePartNum];
        for (int i = 0; i < DatabasePartNum; ++i) {
            indexStorages[i] = new IndexStorage(indexDirectory, i);
        }

        auto tstart = std::chrono::steady_clock::now();

#pragma omp parallel for
        // num_threads(4)
        for (int i = 0; i < DatabasePartNum; ++i) {
            recoveryIndexFromIndexStorage(indexStorages[i], dbSchema, eMap);
        }

        auto t1 = std::chrono::steady_clock::now();
        double elapsedS =
            std::chrono::duration_cast<std::chrono::milliseconds>(t1 - tstart)
                .count() *
            1.0 / 1000;

        for (int i = 0; i < DatabasePartNum; ++i) {
            delete indexStorages[i];
        }

        std::cout << "rebuild index elapsed " << elapsedS << "s." << std ::endl;

        return 0;
    }

    void printInfo();

};  // End class TSDBEngineImpl.

}  // namespace LindormContest
