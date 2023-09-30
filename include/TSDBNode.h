#pragma once
#include <assert.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <atomic>
#include <filesystem>
#include <map>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include "Hasher.hpp"
#include "TSDBTypes.h"

namespace LindormContest {

// typedef struct TSDBMetaIndex {
//     CTimestamp timestamp;

// } TSDBMetaIndex;

// typedef struct TSDBNode {
//     CTimestamp timestamp;
//     // uint32_t timestamp;
//     // int offset, size;
//     uint32_t offset : 22;  // simple file maxsize is 2MB
//     uint32_t size : 10;    // each row maxsize is 1KB
//     // int offset;
//     // int size;
//     // TSDBNode(Timestamp ts, uint32_t _offset, uint32_t _size) {
//     //     timestamp = ts;
//     //     offset = _offset, size = _size;
//     // }
//     TSDBNode() { offset = size = timestamp = 0; }
//     bool operator<(const TSDBNode& t1) { return timestamp < t1.timestamp; }
//     bool operator<(const CTimestamp& t) {
//         // uint32_t t0 = uint32_t(t);
//         // return timestamp < t0;
//         return timestamp < t;
//     }
// } TSDBNode;

// typedef struct RowContent {
//     Timestamp timestamp;
//     char* row;
//     uint16_t check;
// } RowContent;

// typedef struct PageMeta {
//     unsigned int size : 17;       // real page size
//     unsigned int check : 14;      //
//     unsigned int compressed : 1;  // compressed or not
// } PageMeta;

}  // namespace LindormContest