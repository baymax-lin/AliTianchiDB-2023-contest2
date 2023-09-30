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
#include "Hasher.hpp"
#include "Root.h"
#include "TSDBEngine.hpp"
#include "TSDBNode.h"
#include "TSDBTypes.h"
#include "config.h"
#include "struct/Row.h"

namespace LindormContest {

// blkId: 20b

typedef struct RNColumnIndex {
    uint32_t blkRnId : 20;
    uint32_t viewId : 12;
    uint32_t rnOffset;

    RNColumnIndex(uint32_t _blkId, uint32_t _viewId, uint32_t _offs) {
        blkRnId = _blkId;
        viewId = _viewId;
        rnOffset = _offs;
    }
    bool operator<(const RNColumnIndex& t1) {
        if (blkRnId != t1.blkRnId) {
            return blkRnId < t1.blkRnId;
        } else {
            return rnOffset < t1.rnOffset;
        }
    }

} RNColumnIndex;

typedef struct RowIndex {
    uint32_t blkRnId : 20;
    uint32_t strSize : 12;
    CTimestamp timestamp;     // 16
    uint64_t rnOffset : 22;   // 20
    uint64_t strOffset : 22;  // 20
    // uint64_t strSize : 12;    // 9
    uint64_t blkStrId : 20;

    bool operator<(const RowIndex& t1) { return timestamp < t1.timestamp; }
    bool operator<(const CTimestamp& t) {
        // uint32_t t0 = uint32_t(t);
        // return timestamp < t0;
        return timestamp < t;
    }
} RowIndex;

class TSDBIndex {
   private:
    // TSDBBlock block;
    VinID vinID;
    // std::vector<TSDBBlock> blocks;
    std::string root;
    std::string directory;
    Vin vin;
    std::atomic<int> latestBlockId;

    std::atomic<Timestamp> latestTs = 0;

    volatile int latestBufCap;

   public:
    uint32_t assignPart;
    std::mutex lock;
    uint16_t latestColIndex[ColumnMaxSize];
    volatile Timestamp latestCacheTs;
    LindormContest::Row fullRowCache;
    LindormContest::Row prevQueryRowCache;
    volatile uint64_t prevQueryReqCols;
    volatile bool cacheReady;
    volatile bool cacheSteady;
    volatile bool sorted;
    // std::atomic<bool> cacheSteady;
    volatile int latestSize;
    char* latestBuffer;
    volatile char* latestCacheAddr;
    std::atomic<int> num;
    // volatile

    // Timestamp latestTs = 0;
    std::mutex idxLock;
    std::atomic<int> latestIndex;
    std::vector<RowIndex> serial;                // sort by time
    uint16_t serialReindex[queryRangeSortSize];  // sort by block and offset
    size_t dataSize = 0;

    size_t joinCount = 0;

   public:
    TSDBIndex(const std::string& _root, const Vin& _vin, const VinID& _vinID);
    ~TSDBIndex();
    // void resizeBlocks(int n);

    void setAssignPart(uint32_t partId) { assignPart = partId; }
    uint32_t getAssignPart() { return assignPart; }
    VinID getVinID() { return vinID; }
    Vin& getVin() { return vin; }
    int getIndexLatestBlock() {
        return latestBlockId.load(std::memory_order_acquire);
    }
    // std::string getFilePath(const Vin& vin, int blockId);
    bool cacheLatest() {
        return latestCacheTs >= latestTs.load(std::memory_order_acquire) &&
               cacheReady;
    }

    RowIndex latest() {
        // lock.lock();
        // int latIdx = latestIndex.load(std::memory_order_acquire);
        // if (latIdx == -1 || serial.size() == 0) {
        //     lock.unlock();
        //     return RowIndex();
        // }

        // auto res = serial[latIdx];
        // lock.unlock();
        // return res;

        int latIdx = latestIndex.load(std::memory_order_acquire);
        if (latIdx == -1 || serial.size() == 0)
            return RowIndex();
        return serial[latIdx];
    }

    /***
     * return next index reference
     */
    int nextRefIndex(bool firstCF = true) {
        if (firstCF) {
            joinCount++;
            serial.emplace_back(std::move(RowIndex()));
            return serial.size() - 1;
        } else {
            uint32_t idx = joinCount % serial.size();
            joinCount++;
            // return serial[idx];
            return idx;
        }
    }
    /**
     * re-search latest index
     * internal function and require lock when concurrency
     */
    void reSearchLatestIndex() {
        int rlatestIndex = 0;
        Timestamp latestTime = 0;
        for (int i = 0; i < serial.size(); ++i) {
            auto& r = serial[i];
            if (r.timestamp > latestTime) {
                latestTime = r.timestamp;
                rlatestIndex = i;
            }
        }
        latestIndex.store(rlatestIndex, std::memory_order_release);
    }
    /**
     * join new index to index serial
     * REQ SUPPORT CONCURRENCY
     */
    int joinIndex(LindormContest::RowIndex& rIndex) {
        lock.lock();
        joinCount++;
        serial.emplace_back(std::move(rIndex));
        int idx = serial.size() - 1;
        if (serial.size() == queryRangeSortSize) {
            postProcess();
        }
        lock.unlock();
        return idx;
    }

    int getLatestData(LindormContest::Row& row,
                      LindormContest::Vin vin,
                      uint32_t* reqCols,
                      uint32_t nReqColNum,
                      ColumnType* columnsType) {
        char* buf = latestBuffer;
        row.vin = std::move(vin);
        row.timestamp = *(Timestamp*)(buf);

        int cI = 0;
        // std::cout << "### " << row.timestamp << std::endl;
        for (auto it = row.columns.begin(); it != row.columns.end(); ++it) {
            uint32_t col = reqCols[cI];

            ColumnType cType = columnsType[col];
            char* cur = buf + latestColIndex[col];

            if (cType == COLUMN_TYPE_INTEGER) {
                // std::cout << "1 " << std::endl;
                // printf("%d %d %d\n", col, latestColIndex[col], 1);
                int32_t intVal = *(int32_t*)cur;
                it->second = std::move(ColumnValue(intVal));

            } else if (cType == COLUMN_TYPE_DOUBLE_FLOAT) {
                // std::cout << "2 " << std::endl;
                // printf("%d %d %d\n", col, latestColIndex[col], 2);
                double_t doubleVal = *(double_t*)cur;
                it->second = std::move(ColumnValue(doubleVal));

            } else if (cType == COLUMN_TYPE_STRING) {
                uint32_t strLen = *(uint32_t*)cur;
                // std::cout << "3 " << strLen << std::endl;
                assert(strLen < 16384);
                // printf("%d %d %d %ld\n", col, latestColIndex[col], 3,
                // strLen);

                cur += sizeof(uint32_t);
                it->second = std::move(ColumnValue(cur, strLen));
            }
            cI++;
        }

        return 0;
    }

    inline void addSize(size_t s) { dataSize += s; }
    inline void setSize(size_t s) { dataSize = s; }

    /***
     * sort serial data and find latest index.
     */
    void postProcess() {
        sort(serial.begin(), serial.end());
        // because we resort all index. Therefore it's necessary to find new
        // latest index
        reSearchLatestIndex();
        sorted = true;

        assert(queryRangeSortSize >= serial.size());
        // reindex by block and offset.
        for (int i = 0; i < serial.size(); ++i) {
            serialReindex[i] = i;
        }
        std::sort(serialReindex, serialReindex + serial.size(),
                  [&](int i, int j) {
                      if (serial[i].blkRnId != serial[j].blkRnId) {
                          serial[i].rnOffset < serial[j].rnOffset;
                      }
                      return serial[i].blkRnId < serial[j].blkRnId;
                  });
    }

    size_t getJoinCount() { return joinCount; }
    // void clearJoinCount()
    size_t getIndexCount() { return serial.size(); }

    void updateLatestTime(Timestamp ts);
    void updateLatest(int rIndex);

    // reqCols: req column marked by bit (1)
    int getLatestCache(char* buf);
    int flushLatestCache(char* buf,
                         size_t size,
                         Timestamp ts,
                         uint16_t* colIndex);

    int filterColumns(uint64_t reqCols, Row& rowInput, Row& rowOutput);
    int getLatestRowCache(uint64_t reqCols, Row& row);
    // int getLatestPartRowCache(Row& row);
};

};  // namespace LindormContest

static inline uint32_t partByVin(const LindormContest::Vin& vin,
                                 LindormContest::TSDBIndex* vinIndex) {
#if CONCENTRATED_MODE == 1
    assert(vinIndex);
    return vinIndex->getVinID();
    // size_t h = LindormContest::VinHasher()(vin);
    // return h % DatabasePartNum;
#else
    size_t h = LindormContest::VinHasher()(vin);
    return h % DatabasePartNum;
#endif
}