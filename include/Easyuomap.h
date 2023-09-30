#pragma once

#include <list>
#include "Locks.h"
#include "TSDBEngine.hpp"
#include "TSDBIndex.h"
#include "struct/Vin.h"
// support concurrency

namespace LindormContest {

typedef struct UmapNode {
    // std::atomic< Vin vin;
    // size_t vinHash;
    // std::atomic<size_t> vinHash;
    // std::atomic<TSDBIndex*> hIndex;
    size_t vinHash;
    TSDBIndex* hIndex;
    // VinId id;
    UmapNode() { hIndex = nullptr; }
    // UmapNode(const Vin& _vin, TSDBIndex* _p) { vin = _vin, hIndex = _p; }
    UmapNode(size_t _vinHash, TSDBIndex* _p) {
        vinHash = _vinHash, hIndex = _p;
        // vinHash.store(_vinHash, std::memory_order_release);
        // hIndex.store(_p, std::memory_order_release);
    }
    UmapNode(const UmapNode& um) {
        vinHash = um.vinHash, hIndex = um.hIndex;
        // vinHash.store(_vinHash, std::memory_order_release);
        // hIndex.store(_p, std::memory_order_release);
        // vinHash.store(um.vinHash, std::memory_order_release);
        // hIndex.store(um.hIndex, std::memory_order_release);
    }
    ~UmapNode() {
        // if (hIndex)
        //     delete hIndex;
    }
} UmapNode;
typedef struct UmapSlot {
    UmapNode first;
    std::mutex lock;
    // SpinLock lock;
    volatile uint16_t count;
    // std::list<UmapNode> nodeList;
    std::vector<UmapNode> nodevec;
    UmapSlot() {
        count = 0;
        // nodeList = nullptr;
    }
    ~UmapSlot() {
        // 释放index 树
        if (first.hIndex) {
            if (first.hIndex) {
                delete first.hIndex;
                first.hIndex = nullptr;
            }
        }

        // if (nodeList) {
        for (auto it = nodevec.begin(); it != nodevec.end(); ++it) {
            if (it->hIndex) {
                delete it->hIndex;
                it->hIndex = nullptr;
            }
        }

        // delete nodeList;
        // }
    }
} UmapSlot;

// tempate
class EasyUnorderedMap {
   public:
    size_t cap;
    UmapSlot* hashSpace;
    std::string root;
    std::atomic<uint32_t> size;

    EasyUnorderedMap(int _cap, std::string _root) {
        cap = _cap;
        hashSpace = new UmapSlot[cap];
        root = _root;
        size = 0;
    }
    ~EasyUnorderedMap() {
        // std::cout << "emap size: " << size << std::endl;
        delete[] hashSpace;
    }

    /**
     * recovery function using "insert"
     */
    TSDBIndex* insert(const Vin& vin, VinID setID, bool autoInc) {
        // auto hf = VinHasher();

        size_t vinHash = VinHasher()(vin);
        size_t hv = vinHash % cap;

        auto& x = hashSpace[hv];
        std::string indexPath =
            root + "/" + std::string(vin.vin, LindormContest::VIN_LENGTH);

        TSDBIndex* ret = nullptr;
        x.lock.lock();

        VinID id;
        if (autoInc) {
            id = size.fetch_add(1);
        } else {
            id = setID;
            size++;
        }

        if (x.count == 0) {
            x.first.vinHash = vinHash;
            // x.first.vinHash.store(vinHash, std::memory_order_release);
            ret = x.first.hIndex = new TSDBIndex(indexPath, vin, id);
        } else {
            // x.nodeList = new std::list<UmapNode>();
            ret = new TSDBIndex(indexPath, vin, id);
            x.nodevec.emplace_back(std::move(UmapNode(vinHash, ret)));
        }
        x.count++;
        x.lock.unlock();

        return ret;
    }

    TSDBIndex* insertSpdTest(const Vin& vin) {
        // auto hf = VinHasher();

        size_t vinHash = VinHasher()(vin);
        size_t hv = vinHash % cap;

        auto& x = hashSpace[hv];

        TSDBIndex* ret = nullptr;
        x.lock.lock();

        if (x.count == 0) {
            x.first.vinHash = vinHash;
            // x.first.vinHash.store(vinHash, std::memory_order_release);
            // ret = x.first.hIndex = new TSDBIndex(indexPath, vin, id);
            ret = nullptr;
        } else {
            // x.nodeList = new std::list<UmapNode>();
            // ret = new TSDBIndex(indexPath, vin, id);
            ret = nullptr;
            x.nodevec.emplace_back(std::move(UmapNode(vinHash, ret)));
        }
        x.count++;
        x.lock.unlock();

        return ret;
    }

    TSDBIndex* query(const Vin& vin) {
        size_t vinHash = VinHasher()(vin);
        size_t hv = vinHash % cap;

        auto& x = hashSpace[hv];
        TSDBIndex* ret = nullptr;
        // x.lock.lock();
        if (x.count > 0) {
            if (x.first.vinHash == vinHash) {
                ret = x.first.hIndex;
                // } else if (x.nodeList) {
            } else {
                for (auto it = x.nodevec.begin(); it != x.nodevec.end(); ++it) {
                    if (it->vinHash == vinHash) {
                        ret = it->hIndex;
                        break;
                    }
                }
            }
        }
        // x.lock.unlock();
        return ret;
    }

    /**
     * If not found, insert imediately and atomicly.
     * This function is used for first loading
     */
    // std::mutex llk;
    TSDBIndex* queryInsert(const Vin& vin, bool& newInsert) {
        size_t vinHash = VinHasher()(vin);
        size_t hv = vinHash % cap;

        auto& x = hashSpace[hv];
        TSDBIndex* ret = nullptr;
        newInsert = false;
        x.lock.lock();
        if (x.count > 0) {
            if (x.first.vinHash == vinHash) {
                ret = x.first.hIndex;
                // } else if (x.nodeList) {
            } else {
                for (auto it = x.nodevec.begin(); it != x.nodevec.end(); ++it) {
                    if (it->vinHash == vinHash) {
                        ret = it->hIndex;
                        break;
                    }
                }
            }
        }
        // NOT FOUND
        if (!ret) {
            newInsert = true;
            std::string indexPath =
                root + "/" + std::string(vin.vin, LindormContest::VIN_LENGTH);

            VinID id = size.fetch_add(1);
            if (x.count == 0) {
                ret = x.first.hIndex = new TSDBIndex(indexPath, vin, id);
                x.first.vinHash = vinHash;

            } else {
                ret = new TSDBIndex(indexPath, vin, id);
                x.nodevec.emplace_back(UmapNode(vinHash, ret));
            }
            // assert(ret);
            if (ret) {
                // size++;
                x.count++;
            }
        }
        assert(ret);
        // std::cout << vin.vin << " " << (uint64_t)ret << std::endl;
        x.lock.unlock();

        return ret;
    }
};

}  // namespace LindormContest
