
#include "TSDBIndex.h"
#include <assert.h>
#include <filesystem>
#include <fstream>
#include <sstream>
#include "TSDBEngineImpl.h"
#include "config.h"

namespace LindormContest {
TSDBIndex::TSDBIndex(const std::string& _root,
                     const Vin& _vin,
                     const VinID& _vinID) {
    root = _root;
    vin = _vin;
    vinID = _vinID;
    directory = root;
    latestBlockId.store(INT32_MIN, std::memory_order_relaxed);

    latestTs = 0;
    latestSize = 0;
    latestBufCap = 1024;
    latestBuffer = new char[latestBufCap];
    latestCacheTs = 0;
    cacheReady = false;
    cacheSteady = false;
    sorted = false;
    num = 0;

    latestIndex = -1;
    // if (vinID >= vinMaxSize) {
    //     std::cout << "Error TSDBIndex@@@ vinid: " << _vinID << std::endl;
    // }
}

TSDBIndex::~TSDBIndex() {
    delete[] latestBuffer;
}

void TSDBIndex::updateLatestTime(Timestamp ts) {
    Timestamp t = latestTs.load(std::memory_order_acquire);
    Timestamp tmax = std::max(ts, t);
    while (!latestTs.compare_exchange_weak(t, tmax)) {
        t = latestTs.load(std::memory_order_acquire);
        tmax = std::max(ts, t);
    }
}

void TSDBIndex::updateLatest(int rindx) {
    int now = latestIndex.load(std::memory_order_acquire);

    if (now == 0) {
        latestIndex.store(rindx, std::memory_order_release);
        return;
    }
    int tmax = serial[now].timestamp >= serial[rindx].timestamp ? now : rindx;
    while (!latestIndex.compare_exchange_weak(now, tmax)) {
        now = latestIndex.load(std::memory_order_acquire);
        tmax = serial[now].timestamp >= serial[rindx].timestamp ? now : rindx;
    }
    // std::cout << "update latest: " << now << " " << tmax << std::endl;
}

int TSDBIndex::flushLatestCache(char* buf,
                                size_t size,
                                Timestamp ts,
                                uint16_t* colIndex) {
    if (serial.size() == queryRangeSortSize) {
        lock.lock();
        if (cacheSteady) {
            lock.unlock();
            return 0;
        }

        if (size > latestBufCap) {
            delete[] latestBuffer;
            latestBufCap = size;
            latestBuffer = new char[latestBufCap];
        }
        memcpy((void*)latestBuffer, buf, size);

        latestSize = size;
        latestCacheTs = ts;
        cacheReady = true;

        cacheSteady = true;
        memcpy(latestColIndex, colIndex, sizeof(latestColIndex));
        lock.unlock();
    }

    return 0;

    // return 0;
}

int TSDBIndex::getLatestCache(char* buf) {
    // if (latestSize == 0)
    // return -1;
    // assert(latestSize > 0);
    // lock.lock();

    memcpy(buf, (void*)latestBuffer, latestSize);
    // lock.unlock();
    return 0;

    // return 0;
}

int TSDBIndex::filterColumns(uint64_t reqCols, Row& rowInput, Row& rowOutput) {
    int i = 0;
    rowOutput = rowInput;
    auto it = rowOutput.columns.begin();
    while (it != rowOutput.columns.end()) {
        if ((reqCols & (1ULL << i)) == 0) {
            rowOutput.columns.erase(it++);
        } else
            it++;
        ++i;
    }
    return 0;
}

int TSDBIndex::getLatestRowCache(uint64_t reqCols, Row& row) {
    // if (num != queryRangeSortSize)
    // lock.lock();
    // if (reqCols == prevQueryReqCols) {
    // }
    //     row = prevQueryRowCache;
    // } else {
    filterColumns(reqCols, fullRowCache, row);
    // }
    // if (num != queryRangeSortSize)
    // lock.unlock();

    // for(int i=0;i<col)

    return 0;
}

};  // namespace LindormContest