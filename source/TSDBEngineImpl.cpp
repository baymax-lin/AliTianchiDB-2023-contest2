//
// You should modify this file.
//
// Refer TSDBEngineSample.cpp to ensure that you have understood
// the interface semantics correctly.
//
#include "TSDBEngineImpl.h"
#include <malloc.h>
#include <sys/io.h>
#include <exception>
#include <filesystem>
#include <fstream>
#include <sstream>
#include "omp.h"

int latest_cache_hit = 0;
int latest_query_count = 0;
double time1 = 0, time2 = 0;
double time3 = 0, time4 = 0;
size_t sumLatestQuery1 = 0, sumLatestQuery2 = 0, latestQ1 = 0;
size_t sumRangeQuery1 = 0, sumRangeQuery2 = 0, rangeQ1 = 0;

volatile bool lruStat = false;

volatile int timerStopSig = 0;
volatile uint32_t maxStrSize = 0;

typedef struct {
    uint32_t virtualMem;
} processMem_t;

int memParseLine(char* line) {
    // This assumes that a digit will be found and the line ends in " Kb".
    int i = strlen(line);
    const char* p = line;
    while (*p < '0' || *p > '9')
        p++;
    line[i - 3] = '\0';
    i = atoi(p);
    return i;
}

processMem_t GetProcessMemory() {
    FILE* file = fopen("/proc/self/status", "r");
    char line[128];
    processMem_t processMem;

    while (fgets(line, 128, file) != NULL) {
        if (strncmp(line, "VmSize:", 7) == 0) {
            processMem.virtualMem = memParseLine(line);
            break;
        }

        // if (strncmp(line, "VmRSS:", 6) == 0) {
        //	processMem.physicalMem = parseLine(line);
        //	break;
        // }
    }
    fclose(file);
    return processMem;
}
bool testMem(size_t mm) {
    char* f = new char[mm];
    if (f) {
        delete[] f;
        // sleep(1);
        return true;
    } else
        return false;
}

size_t memoryAsk() {
    size_t L = 1ULL * 1024 * 1024;
    size_t R = 64ULL * 1024 * 1024;

    while (L < R) {
        size_t mid = L + (R - L) / 2;
        bool test = testMem(mid);
        // std::cout << L << " " << R << " " << mid << std::endl;
        if (test) {
            L = mid + 1;
        } else
            R = mid;
    }
    return L;
}

namespace LindormContest {

inline std::string int64ToStr(int64_t num) {
    std::ostringstream oss;
    oss << num;
    return oss.str();
}

inline std::ostream& operator<<(std::ostream& os, const Vin& vin) {
    std::string vinStr(vin.vin, vin.vin + VIN_LENGTH);
    os << vinStr;
    return os;
}

inline void swapRow(Row& lhs, Row& rhs) {
    std::swap(lhs.vin, rhs.vin);
    std::swap(lhs.timestamp, rhs.timestamp);
    lhs.columns.swap(rhs.columns);
}

void TSDBEngineImpl::timerStat() {
    static int C = 0;
    while (1) {
        sleep(10);
        if (timerStopSig)
            break;

        if (concurrencyVinset.size() > 0) {
            printf("%d concurrency Vin: %d; Q1: %d, Q2: %d, Q3: %d\n", C,
                   (int)concurrencyVinset.size(), dbSchema->queryCount,
                   dbSchema->queryAggCount, dbSchema->queryDownsampleCount);
        }
        // for (int i = 0; i < DatabasePartNum; ++i) {
        //     printf("L%d: %d %d | ", i, dbParts[i]->cf[0]->lru->getSize(),
        //            dbParts[i]->cf[1]->lru->getSize());
        // }
        // printf("\n");
        C++;
        vinsetLock.lock();
        concurrencyVinset.clear();
        vinsetLock.unlock();
    }
}

/**
 * This constructor's function signature should not be modified.
 * Our evaluation program will call this constructor.
 * The function's body can be modified.
 */
TSDBEngineImpl::TSDBEngineImpl(const std::string& dataDirPath)
    : TSDBEngine(dataDirPath),
      columnsNum(-1),
      columnsName(nullptr),
      columnsType(nullptr) {
    eMap = new EasyUnorderedMap(vinMaxSize * 6, dataDirPath);
    // dbSchema = new DbSchema()
    dbSchema = nullptr;
    for (int i = 0; i < DatabasePartNum; ++i) {
        dbParts[i] = nullptr;
    }

    indexDirectory = dataDirPath + "/index";
    std::filesystem::path cfPath = std::filesystem::path(indexDirectory);
    std::filesystem::create_directories(cfPath);

    timerThread = new std::thread(&TSDBEngineImpl::timerStat, this);
    timerThread->detach();

    return;
}

int TSDBEngineImpl::connect() {
    timerStopSig = 0;
    latest_cache_hit = latest_query_count = 0;
    reqLock = true;
    processMem_t m0 = GetProcessMemory();
    std::cout << "Before connect Mem: " << m0.virtualMem / 1024.0 << "MB"
              << std::endl;

    // Read schema.
    std::ifstream schemaFin;

    schemaFin.open(getDataPath() + "/schema", std::ios::in);
    std::cout << schemaFin.is_open() << std::endl;
    if (!schemaFin.is_open() || !schemaFin.good()) {
        std::cout << "Connect new database with empty pre-written data"
                  << std::endl;

        firstLoad = true;

        schemaFin.close();

        return 0;
    }

    firstLoad = false;

    schemaFin >> columnsNum;
    if (columnsNum <= 0) {
        std::cerr << "Unexpected columns' num: [" << columnsNum << "]"
                  << std::endl;
        schemaFin.close();
        throw std::exception();
    }
    std::cout << "Found pre-written data with columns' num: [" << columnsNum
              << "]" << std::endl;

    dbSchema = new DbSchema(columnsNum);

    for (int i = 0; i < columnsNum; ++i) {
        // schemaFin >> columnsName[i];
        schemaFin >> dbSchema->columnsName[i];

        int32_t columnTypeInt;
        schemaFin >> columnTypeInt;
        // columnsType[i] = (ColumnType)columnTypeInt;
        dbSchema->columnsType[i] = (ColumnType)columnTypeInt;
    }

    // initialize
    dbSchema->initColuimnFamilyIndex();

    columnsType = dbSchema->columnsType;
    columnsName = dbSchema->columnsName;

    for (int i = 0; i < DatabasePartNum; ++i) {
        dbParts[i] = new CompressDB(dataDirPath + "/data", i, dbSchema);
    }

    recovery();

    processMem_t m = GetProcessMemory();
    std::cout << "Mem: " << m.virtualMem / 1024.0 << "MB" << std::endl;

    return 0;
}

void TSDBEngineImpl::recovery() {
    std::cout << "Recovery start" << std::endl;
    // read vin-ID map info
    VinIDStorage vinIDStorage(getDataPath());
    vinIDStorage.vopen();
    int vN = vinIDStorage.readNum();
    std::cout << "vin-id map size: " << vN << std::endl;
    for (int i = 0; i < vN; ++i) {
        Vin vin;
        VinID vinID;
        vinIDStorage.readVinIDMap(i, vinID, vin);
        dbSchema->vinMap[vinID] = vin;
        // std::cout << i << " " << vin << std::endl;
        auto it = eMap->insert(vin, vinID, false);
        dbSchema->vinIndexMap[vinID] = it;
        assert(it);
        it->setAssignPart(partByVin(vin, it));
    }
    // std::cout << "Recovery emap size: " << eMap->size << std::endl;
    vinIDStorage.vclose();
    int cnt = 0;

    std::cout << "load vinMap OK" << std::endl;

    // Before rebuild index, eMap must be initilized first
    // rebuildDataBaseIndex();
    rebuildDBIndex();
    std::cout << "Recovery finished" << std::endl;
    return;
}

int TSDBEngineImpl::createTable(const std::string& tableName,
                                const Schema& schema) {
    columnsNum = (int32_t)schema.columnTypeMap.size();

    if (dbSchema)
        delete dbSchema;
    for (int i = 0; i < DatabasePartNum; ++i) {
        // dbParts[i] = new CompressDB(dataDirPath, i, dbSchema);
        if (dbParts[i])
            delete dbParts[i];
    }
    dbSchema = new DbSchema(columnsNum);

    int i = 0;
    for (auto it = schema.columnTypeMap.cbegin();
         it != schema.columnTypeMap.cend(); ++it) {
        dbSchema->columnsName[i] = it->first;
        dbSchema->columnsType[i++] = it->second;
    }

    columnsName = dbSchema->columnsName;
    columnsType = dbSchema->columnsType;

    // initialize
    dbSchema->initColuimnFamilyIndex();

    for (int i = 0; i < DatabasePartNum; ++i) {
        dbParts[i] = new CompressDB(dataDirPath + "/data", i, dbSchema);
    }

    return 0;
}

// int aaa = 0;
int TSDBEngineImpl::write(const WriteRequest& writeRequest) {
    assert(dbSchema);
    // static int Xcount = 0;
    // char buf[MaxRowSize];
    // size_t bufSize = 0;
    // if (Xcount++ == 0) {
    //     std::cout << "Re-upsert" << std::endl;
    // }
    // std::cout << aaa++ << std::endl;
    for (const Row& row : writeRequest.rows) {
        bool newInsert = false;
        TSDBIndex* vinIndex = eMap->queryInsert(row.vin, newInsert);
        if (newInsert) {
            vinIndex->setAssignPart(partByVin(row.vin, vinIndex));
        }

        size_t offset = 0;

        VinID vid = vinIndex->getVinID();

        RowIndex rIndex;
        int pid = vinIndex->getAssignPart();
        if (pid >= DatabasePartNum || pid < 0) {
            printf("PPPid:%d\n", pid);
        }

        size_t wrSize = dbParts[pid]->write(row, vid, rIndex);
        vinIndex->addSize(wrSize);
        int idx = vinIndex->joinIndex(rIndex);

        vinIndex->updateLatest(idx);
    }

    return 0;
}

// super optim version
int TSDBEngineImpl::executeLatestQuery(const LatestQueryRequest& pReadReq,
                                       std::vector<Row>& pReadRes) {
    // try {
    //     static uint64_t M = 0;

    assert(dbSchema);

    auto ttt1 = std::chrono::steady_clock::now();

    char buf[MaxRowSize];
    uint16_t colIndex[ColumnMaxSize];
    uint64_t reqCols = 0;
    uint32_t vecReqCols[ColumnMaxSize];
    uint32_t nCols = 0;
    Row rowTemplate;
    LindormContest::ColumnValue emptyValue;
    pReadRes.reserve(pReadReq.vins.size());

    int cI = 0;
    if (pReadReq.requestedColumns.size() == 0) {
        for (int i = 0; i < columnsNum; ++i) {
            std::string& cName = columnsName[i];
            reqCols |= (1ULL << i);
            rowTemplate.columns.emplace(cName, std::move(emptyValue));
            vecReqCols[nCols++] = i;
        }
    } else {
        for (auto it = pReadReq.requestedColumns.begin();
             it != pReadReq.requestedColumns.end(); ++it) {
            while (cI < columnsNum && columnsName[cI] != *it) {
                cI++;
            }
            if (cI < columnsNum) {
                reqCols |= (1ULL << cI);
                rowTemplate.columns.emplace(columnsName[cI],
                                            std::move(emptyValue));
                vecReqCols[nCols++] = cI;
                cI++;
            }
        }
    }

    bool needStrColumn = (dbSchema->strColumnsU64 & reqCols);

    auto ttt2 = std::chrono::steady_clock::now();

    // if (M <= 10) {
    //     printf("executeLatestQuery. %d\n", pReadReq.vins.size());
    //     M++;
    // }
    for (int i = 0; i < pReadReq.vins.size(); ++i) {
        auto& vin = pReadReq.vins[i];

        Row row(rowTemplate);
        // TSDBIndex* vinIndex = getTSDBIndex(vin, false);
        TSDBIndex* vinIndex = eMap->query(vin);
        latest_query_count++;
        if (vinIndex) {
            uint32_t pid = vinIndex->getAssignPart();
            // uint32_t pid = partByVin(vin);
            // std::cout << "R1: " << vin << " " << vinIndex <<  std::endl;
            // if (M <= 10) {
            //     printf(
            //         "executeLatestQuery, pid1 %d pid2 %d, needstr %d, "
            //         "serial %d, cache %d, ncol %d"
            //         "\n",
            //         pid, partByVin(vin), needStrColumn,
            //         vinIndex->serial.size(), vinIndex->cacheSteady, nCols);
            // }

            if (vinIndex->cacheSteady) {
                latest_cache_hit++;
                // vinIndex->getLatestCache(buf);

                // RowFromIndexBytes(vin, vinIndex->latestBuffer,
                // vecReqCols,
                //                   nCols, row, vinIndex->latestColIndex);
                vinIndex->getLatestData(row, vin, vecReqCols, nCols,
                                        columnsType);
            } else {
                RowIndex latest;
                latest = vinIndex->latest();

                // if (M <= 10) {
                //     printf("L index: %d %d %d %d %d %ld \n", latest.blkRnId,
                //            latest.blkStrId, latest.rnOffset,
                //            latest.strOffset, latest.strSize,
                //            latest.timestamp);
                // }

                // assert(latest.blkStrId < 10000 && latest.blkRnId < 10000);
                // assert(latest.strOffset <= BlockBufferSize &&
                //        latest.rnOffset <= BlockBufferSize);
                // assert(latest.strSize < 1024);

                // MAKE SURE PARAM 'TIME' in function db.read() is VALID for
                // JUDGEMENT!
                // dbParts[pid]->read(row, vin, latest, vecReqCols);

                if (vinIndex->serial.size() != queryRangeSortSize) {
                    dbParts[pid]->read(row, vin, latest, vecReqCols,
                                       needStrColumn);
                } else {
                    vinIndex->lock.lock();
                    if (vinIndex->cacheSteady) {
                        vinIndex->lock.unlock();
                        dbParts[pid]->read(row, vin, latest, vecReqCols,
                                           needStrColumn);
                    } else {
                        // read all column family for cache latest
                        dbParts[pid]->read(row, vin, latest, vecReqCols, true,
                                           vinIndex->latestBuffer,
                                           vinIndex->latestColIndex);
                        vinIndex->cacheSteady = true;
                        vinIndex->lock.unlock();
                    }
                }
            }

            // pReadRes.emplace_back(std::move(row));
            pReadRes.push_back(std::move(row));
        }
    }

    auto ttt3 = std::chrono::steady_clock::now();
    time1 += std::chrono::duration_cast<std::chrono::milliseconds>(ttt2 - ttt1)
                 .count() *
             1.0 / 1000;
    time2 += std::chrono::duration_cast<std::chrono::milliseconds>(ttt3 - ttt2)
                 .count() *
             1.0 / 1000;

    sumLatestQuery1++;
    sumLatestQuery2 += pReadReq.vins.size();
    if (pReadReq.vins.size() == 1)
        latestQ1++;

    // } catch (exception& e) {
    //     cout << "executeLatestQuery exception: " << e.what() << endl;
    //     perror("executeLatestQuery");
    // }

    return 0;
}

int TSDBEngineImpl::executeTimeRangeQuery(
    const TimeRangeQueryRequest& trReadReq,
    std::vector<Row>& trReadRes) {
    // try {
    assert(dbSchema);

    lruStat = false;
    static uint64_t M = 0;
    dbSchema->queryCount++;
    if (M < dbSchema->queryRangeWindow) {
        dbSchema->queryRange +=
            trReadReq.timeUpperBound - trReadReq.timeLowerBound;
        M++;
    }

    informVin(trReadReq.vin);
    auto ttt1 = std::chrono::steady_clock::now();

    char buf[MaxRowSize];
    uint64_t reqCols = 0;
    uint32_t vecReqCols[ColumnMaxSize];
    uint32_t nCols = 0;
    // uint16_t colIndex[ColumnMaxSize];
    Row rowTemplate;
    LindormContest::ColumnValue emptyValue;

    // for (int cI = 0; cI < columnsNum; ++cI) {
    //     std::string& cName = columnsName[cI];
    //     int n = trReadReq.requestedColumns.count(cName);
    //     if (n || trReadReq.requestedColumns.size() == 0) {
    //         reqCols |= (1ULL << cI);
    //         rowTemplate.columns.emplace(cName, std::move(emptyValue));
    //         vecReqCols[nCols++] = cI;
    //     }
    // }

    int cI = 0;
    if (trReadReq.requestedColumns.size() == 0) {
        for (int i = 0; i < columnsNum; ++i) {
            std::string& cName = columnsName[i];
            reqCols |= (1ULL << i);
            rowTemplate.columns.emplace(cName, std::move(emptyValue));
            vecReqCols[nCols++] = i;
        }
    } else {
        for (auto it = trReadReq.requestedColumns.begin();
             it != trReadReq.requestedColumns.end(); ++it) {
            while (cI < columnsNum && columnsName[cI] != *it) {
                cI++;
            }
            if (cI < columnsNum) {
                reqCols |= (1ULL << cI);
                rowTemplate.columns.emplace(columnsName[cI],
                                            std::move(emptyValue));
                vecReqCols[nCols++] = cI;
                cI++;
            }
        }
    }
    bool needStrColumn = (dbSchema->strColumnsU64 & reqCols);

    auto ttt2 = std::chrono::steady_clock::now();

    int f =
        std::min(int(trReadReq.timeUpperBound - trReadReq.timeLowerBound), 64);
    // nodes.reserve(f);
    trReadRes.reserve(f);

    auto& vin = trReadReq.vin;
    TSDBIndex* vinIndex = eMap->query(vin);
    if (vinIndex) {
        CTimestamp tsL = trReadReq.timeLowerBound;
        CTimestamp tsR = trReadReq.timeUpperBound;

        if (vinIndex->serial.size() == queryRangeSortSize) {
            int index = lower_bound(vinIndex->serial.begin(),
                                    vinIndex->serial.end(), tsL) -
                        vinIndex->serial.begin();
            for (int i = index; i < vinIndex->serial.size(); ++i) {
                auto& n = vinIndex->serial[i];
                if (n.timestamp >= tsL && n.timestamp < tsR) {
                    Row row(rowTemplate);

                    uint32_t pid = vinIndex->getAssignPart();
                    // uint32_t pid = partByVin(vin);
                    // MAKE SURE PARAM 'TIME' in function db.read() is VALID
                    // for JUDGEMENT!
                    dbParts[pid]->read(row, vin, n, vecReqCols, needStrColumn);
                    trReadRes.push_back(std::move(row));
                } else
                    break;
            }
        } else {
            for (auto& n : vinIndex->serial) {
                if (n.timestamp >= tsL && n.timestamp < tsR) {
                    Row row(rowTemplate);

                    // vinIndex->block->query(n.offset, n.size, buf, true);
                    // RowFromBytes(vin, buf, n.size, vecReqCols, nCols,
                    // row,
                    //              nullptr);

                    // uint32_t pid = partByVin(vin);
                    uint32_t pid = vinIndex->getAssignPart();
                    // MAKE SURE PARAM 'TIME' in function db.read() is VALID
                    // for JUDGEMENT!
                    dbParts[pid]->read(row, vin, n, vecReqCols, needStrColumn);

                    trReadRes.push_back(std::move(row));
                }
            }
        }
    }

    auto ttt3 = std::chrono::steady_clock::now();

    sumRangeQuery1++;
    sumRangeQuery2 += trReadRes.size();
    if (trReadRes.size() == 1)
        rangeQ1++;

    time3 += std::chrono::duration_cast<std::chrono::milliseconds>(ttt2 - ttt1)
                 .count() *
             1.0 / 1000;
    time4 += std::chrono::duration_cast<std::chrono::milliseconds>(ttt3 - ttt2)
                 .count() *
             1.0 / 1000;

    return 0;
}

int TSDBEngineImpl::executeAggregateQuery(
    const TimeRangeAggregationRequest& aggregationReq,
    std::vector<Row>& aggregationRes) {
    // try {
    lruStat = true;
    static uint64_t M = 0;
    dbSchema->queryAggCount++;
    if (M < dbSchema->queryRangeWindow) {
        dbSchema->queryRangeAgg +=
            aggregationReq.timeUpperBound - aggregationReq.timeLowerBound;
        M++;
    }
    //     if (M <= 10) {
    //         printf(
    //             "%ld executeAggregateQuery, timeRange: %ld\n", M,
    //             aggregationReq.timeUpperBound -
    //             aggregationReq.timeLowerBound);
    //         M++;
    //     }

    Row rowTemplate;
    uint32_t aggregateCount = 0;

    auto cschema = dbSchema->columnTypeByName[aggregationReq.columnName];
    auto colIndex = cschema.first;
    auto ctype = cschema.second;

    if (aggregationReq.aggregator == LindormContest::MAX) {
        dbSchema->queriedMAX[colIndex]++;
    } else
        dbSchema->queriedAVG[colIndex]++;

    informVin(aggregationReq.vin);

    assert(ctype == LindormContest::COLUMN_TYPE_DOUBLE_FLOAT ||
           ctype == LindormContest::COLUMN_TYPE_INTEGER);

    std::vector<LindormContest::RNColumnIndex> indexs;
    uint32_t xrange =
        (aggregationReq.timeUpperBound - aggregationReq.timeLowerBound) / 1000;
    // indexs.reserve(std::min(xrange, 64U));
    indexs.reserve(xrange);

    auto vin = aggregationReq.vin;
    TSDBIndex* vinIndex = eMap->query(vin);
    // Must be transformed to be further compare (or will be error because
    // CTimestamp is 4B while Timestamp is 8B)
    CTimestamp tsL = aggregationReq.timeLowerBound;
    CTimestamp tsR = aggregationReq.timeUpperBound;

    if (vinIndex) {
        assert(vinIndex->sorted);

        for (int k = 0; k < vinIndex->serial.size(); ++k) {
            int c = vinIndex->serialReindex[k];
            auto& s = vinIndex->serial[c];
            if (tsL <= s.timestamp && s.timestamp < tsR) {
                aggregateCount++;
                indexs.emplace_back(std::move(
                    LindormContest::RNColumnIndex(s.blkRnId, 0, s.rnOffset)));
            }
        }
        // for (auto& s : vinIndex->serial) {
        //     if (tsL <= s.timestamp && s.timestamp < tsR) {
        //         aggregateCount++;
        //         indexs.emplace_back(std::move(
        //             LindormContest::RNColumnIndex(s.blkRnId, 0,
        //             s.rnOffset)));
        //     }
        // }

        if (aggregateCount) {
            // build template
            rowTemplate.vin = vin;
            rowTemplate.timestamp = aggregationReq.timeLowerBound;
            if (ctype == LindormContest::COLUMN_TYPE_DOUBLE_FLOAT) {
                rowTemplate.columns.insert(std::move(
                    std::make_pair(aggregationReq.columnName,
                                   LindormContest::ColumnValue(0.00))));
            } else if (ctype == LindormContest::COLUMN_TYPE_INTEGER) {
                rowTemplate.columns.insert(
                    std::move(std::make_pair(aggregationReq.columnName,
                                             LindormContest::ColumnValue(0))));
            }

            aggregationRes.emplace_back(std::move(rowTemplate));

            uint32_t pid = vinIndex->getAssignPart();

            dbParts[pid]->aggregate(aggregationRes, vin,
                                    aggregationReq.columnName, ctype, indexs,
                                    aggregationReq.aggregator);
        }
    }

    // } catch (exception& e) {
    //     cout << "executeAggregateQuery exception: " << e.what() << endl;
    //     perror("executeAggregateQuery");
    // }

    return 0;
}

int TSDBEngineImpl::executeDownsampleQuery(
    const TimeRangeDownsampleRequest& downsampleReq,
    std::vector<Row>& downsampleRes) {
    queryCount++;
    totalDownsampleCount++;
    // try {

    lruStat = true;
    informVin(downsampleReq.vin);
    static uint64_t M = 0;
    dbSchema->queryDownsampleCount++;
    if (M < dbSchema->queryRangeWindow) {
        dbSchema->queryRangeDownsample +=
            downsampleReq.timeUpperBound - downsampleReq.timeLowerBound;
        M++;
    }

    Row rowTemplate;
    uint32_t downsampleCount[downSampleMaxPart];
    uint32_t culmulative[downSampleMaxPart];

    memset(downsampleCount, 0, sizeof(downsampleCount));
    memset(culmulative, 0, sizeof(culmulative));

    auto cschema = dbSchema->columnTypeByName[downsampleReq.columnName];
    auto colIndex = cschema.first;
    auto ctype = cschema.second;

    // if (downsampleReq.aggregator == LindormContest::MAX) {
    //     dbSchema->queriedMAX[colIndex]++;
    // } else
    //     dbSchema->queriedAVG[colIndex]++;

    assert(ctype == LindormContest::COLUMN_TYPE_DOUBLE_FLOAT ||
           ctype == LindormContest::COLUMN_TYPE_INTEGER);

    uint32_t maxPart =
        (downsampleReq.timeUpperBound - downsampleReq.timeLowerBound) /
        downsampleReq.interval;
    uint32_t partMod =
        (downsampleReq.timeUpperBound - downsampleReq.timeLowerBound) %
        downsampleReq.interval;

    assert(maxPart < downSampleMaxPart);
    assert(partMod == 0 ||
           maxPart == 0);  // if not 0, need to modify viewId mapping.
    if (maxPart == 0)
        maxPart = 1;
    // if (M <= 10) {
    //     printf("%ld downSample interval: %ld %ld. part %d\n", M,
    //            downsampleReq.interval,
    //            downsampleReq.timeUpperBound - downsampleReq.timeLowerBound,
    //            maxPart);
    //     M++;
    // }

    std::vector<LindormContest::RNColumnIndex> indexs;
    uint32_t xrange =
        (downsampleReq.timeUpperBound - downsampleReq.timeLowerBound) / 1000;
    // indexs.reserve(std::max(xrange, 64U));
    indexs.reserve(xrange);

    auto vin = downsampleReq.vin;
    TSDBIndex* vinIndex = eMap->query(vin);
    // Must be transformed to be further compare (or will be error because
    // CTimestamp is 4B while Timestamp is 8B)
    CTimestamp tsL = downsampleReq.timeLowerBound;
    CTimestamp tsR = downsampleReq.timeUpperBound;

    if (vinIndex) {
        // if (M <= 10) {
        //     printf("downSample BK 01\n");
        // }

        for (int k = 0; k < vinIndex->serial.size(); ++k) {
            int c = vinIndex->serialReindex[k];
            auto& s = vinIndex->serial[c];
            if (tsL <= s.timestamp && s.timestamp < tsR) {
                int viewId = (s.timestamp - tsL) / downsampleReq.interval;
                downsampleCount[viewId]++;
                indexs.emplace_back(std::move(LindormContest::RNColumnIndex(
                    s.blkRnId, viewId, s.rnOffset)));
            }
        }

        // for (auto& s : vinIndex->serial) {
        //     if (tsL <= s.timestamp && s.timestamp < tsR) {
        //         int viewId = (s.timestamp - tsL) / downsampleReq.interval;
        //         downsampleCount[viewId]++;
        //         indexs.emplace_back(std::move(LindormContest::RNColumnIndex(
        //             s.blkRnId, viewId, s.rnOffset)));
        //     }
        // }

        // build template
        rowTemplate.vin = vin;
        if (ctype == LindormContest::COLUMN_TYPE_DOUBLE_FLOAT) {
            rowTemplate.columns.insert(std::move(std::make_pair(
                downsampleReq.columnName, LindormContest::ColumnValue(0.00))));
        } else if (ctype == LindormContest::COLUMN_TYPE_INTEGER) {
            rowTemplate.columns.insert(std::move(std::make_pair(
                downsampleReq.columnName, LindormContest::ColumnValue(0))));
        }

        // if (M <= 10) {
        //     printf("downSample BK 02\n");
        // }

        bool existEmpty = false;
        culmulative[0] = (downsampleCount[0] == 0);

        for (int i = 0; i < maxPart; ++i) {
            if (downsampleCount[i] > 0) {
                existEmpty = true;
                rowTemplate.timestamp =
                    downsampleReq.timeLowerBound + i * downsampleReq.interval;
                downsampleRes.emplace_back(rowTemplate);
            }
            if (i > 0) {
                culmulative[i] +=
                    culmulative[i - 1] + (downsampleCount[i] == 0);
            }
        }

        // if (M <= 10) {
        //     printf("downSample BK 03 %d\n", downsampleRes.size());
        // }

        // Remove empty interval view and Adjust other viewId
        // view id is corresponding to the index of non-null interval
        if (existEmpty) {
            for (auto& s : indexs) {
                s.viewId -= culmulative[s.viewId];
                assert(s.viewId < maxPart);
            }
        }

        // uint32_t pid = partByVin(vin);
        uint32_t pid = vinIndex->getAssignPart();

        auto filter = downsampleReq.columnFilter;

        // if (M <= 10) {
        //     printf("downSample BK 04\n");
        // }
        dbParts[pid]->downsample(downsampleRes, vin, downsampleReq.columnName,
                                 ctype, indexs, downsampleReq.aggregator,
                                 &filter);

        // if (M <= 10) {
        //     printf("downSample BK 05\n");
        // }
    }

    // } catch (exception& e) {
    //     cout << "executeDownsampleQuery exception: " << e.what() << endl;
    //     perror("executeDownsampleQuery");
    // }
    queryCount--;
    if (queryCount == 0) {
        if (totalDownsampleCount > 100 && !infoShown) {
            infoShown = true;
            printf(
                "=======******* print info with downsample %ld "
                "*******===========\n",
                totalDownsampleCount);
            printInfo();
        }
    }

    return 0;
}

uint64_t TSDBEngineImpl::getReqCols(
    const std::set<std::string>& requestedColumns) {
    uint64_t reqCols = 0;
    for (int cI = 0; cI < columnsNum; ++cI) {
        std::string& cName = columnsName[cI];
        // ColumnType cType = columnsType[cI];
        // ColumnValue* cVal;
        int n = requestedColumns.count(cName);
        if (n || requestedColumns.size() == 0) {
            reqCols |= (1ULL << cI);
        }
    }
    return reqCols;
}

void TSDBEngineImpl::printInfo() {
    uint64_t N = 0, size = 0;
    uint64_t maxN = 0, maxSize = 0, maxRowSize = 0;
    int uslot = 0;
    int sumCount = 0;
    // eMap iteration
    for (int i = 0; i < eMap->cap; ++i) {
        auto& t = eMap->hashSpace[i];
        if (t.count > 0) {
            uslot++;
            // sumCount += t.count;
            if (t.count >= 1) {
                TSDBIndex* index = t.first.hIndex;
                N += index->serial.size();
                size += index->dataSize;
                maxN = std::max((uint64_t)index->serial.size(), maxN);
                maxSize = std::max(maxSize, index->dataSize);
                sumCount++;
            }

            for (auto it = t.nodevec.begin(); it != t.nodevec.end(); ++it) {
                TSDBIndex* index = it->hIndex;
                N += index->serial.size();
                size += index->dataSize;
                maxN = std::max((uint64_t)index->serial.size(), maxN);
                maxSize = std::max(maxSize, index->dataSize);
                sumCount++;
            }
        }
    }

    printf("slot/cap/used/avgcount: %d %ld %f %f\n", uslot, eMap->cap,
           uslot * 1.0 / eMap->cap, sumCount * 1.0 / uslot);

    printf("(rowCount, rowBytes, avgRowBytes): %ld %fMB %f\n", N,
           size / 1024.0 / 1024, size * 1.0 / N);
    std::cout << "(Vin max rowCount, Vin max fileSize): " << maxN << " "
              << maxSize / 1024.0 / 1024 << "MB" << std::endl;

    std::cout << "latest query hit cache: " << latest_cache_hit << "/"
              << latest_query_count << " "
              << latest_cache_hit * 1.0 / latest_query_count << std::endl;

    printf("latest: %ld / %ld %f %ld\n", sumLatestQuery1, sumLatestQuery2,
           sumLatestQuery2 * 1.0 / sumLatestQuery1, latestQ1);

    printf("range: %ld / %ld %f %ld\n", sumRangeQuery1, sumRangeQuery2,
           sumRangeQuery2 * 1.0 / sumRangeQuery1, rangeQ1);

    printf("time stat: %fs %fs %fms %fms \n", time1, time2,
           time1 * 1000 / sumLatestQuery1, time2 * 1000 / sumLatestQuery2);

    printf("time stat: %fs %fs %fms %fms \n", time3, time4,
           time3 * 1000 / sumRangeQuery1, time4 * 1000 / sumRangeQuery2);

    int Gap = DatabasePartNum / 16;

    // print per part db stat:
    for (int i = 0; i < DatabasePartNum; ++i) {
        if (i % Gap != 0)
            continue;
        // dbParts[i]->
        printf(
            "pid %d: cf0 blkid:  %d, cf0 cur bufoffset: %ld; cf1 blkid: %d, "
            "cf1 cur bufoffset: %ld\n",
            dbParts[i]->dbPartId, dbParts[i]->cf[0]->curBlockId,
            dbParts[i]->cf[0]->bufOffset, dbParts[i]->cf[1]->curBlockId,
            dbParts[i]->cf[1]->bufOffset);
    }

    uint32_t emapSize = eMap->size;
    printf("emap size: %d\n", emapSize);

    for (int i = 0; i < DatabasePartNum; ++i) {
        if (i % Gap != 0)
            continue;
        for (int j = 0; j < ColumnFamilyNum; ++j) {
            auto it = dbParts[i]->cf[j];
            printf(
                "%s: hit:(%ld/%ld %f) fromBuf: %ld, size: %f MB, inBuf: %f "
                "MB\n",
                it->directory.c_str(), it->statF2, it->statF1,
                it->statF2 * 1.0 / it->statF1, it->statF3,
                it->offset1 * 1.0 / 1024 / 1024,
                it->bufOffset * 1.0 / 1024 / 1024);
        }
    }

    // print aggregation stat info
    printf("time Range: %f, aggregation Range: %f, downsample Range %f\n",
           dbSchema->queryRange * 1.0 / dbSchema->queryRangeWindow,
           dbSchema->queryRangeAgg * 1.0 / dbSchema->queryRangeWindow,
           dbSchema->queryRangeDownsample * 1.0 / dbSchema->queryRangeWindow);
    size_t sumAVG = 0, sumMAX = 0;
    for (int i = 0; i < dbSchema->columnsNum; ++i) {
        sumAVG += dbSchema->queriedAVG[i];
        sumMAX += dbSchema->queriedMAX[i];
    }

    printf("each column AVG query (Only AGG): \n");
    for (int i = 0; i < dbSchema->columnsNum; ++i) {
        printf("%d-T%d(%d, %.2f) ", i, dbSchema->columnsType[i],
               dbSchema->queriedAVG[i], dbSchema->queriedAVG[i] * 1.0 / sumAVG);
    }
    printf("\n");

    printf("each column MAX query (Only AGG): \n");
    for (int i = 0; i < dbSchema->columnsNum; ++i) {
        printf("%d-T%d (%d, %.2f) | ", i, dbSchema->columnsType[i],
               dbSchema->queriedMAX[i], dbSchema->queriedMAX[i] * 1.0 / sumMAX);
    }
    printf("\n");

    printf("concurrency queried vins: %d\n", maxConcurrencyVin);
    printf("max writen str size: %d\n", maxStrSize);
}

int TSDBEngineImpl::shutdown() {
    processMem_t m = GetProcessMemory();
    std::cout << "Before Shutdown Mem: " << m.virtualMem / 1024.0 << "MB"
              << std::endl;

    // Persist data index
    storeIndex();

    // Persist the schema.
    if (columnsNum > 0) {
        std::ofstream schemaFout;
        schemaFout.open(getDataPath() + "/schema", std::ios::out);
        schemaFout << columnsNum;
        schemaFout << " ";
        for (int i = 0; i < columnsNum; ++i) {
            schemaFout << columnsName[i] << " ";
            schemaFout << (int32_t)columnsType[i] << " ";
        }
        schemaFout.close();
    }

    // Persist vinID-map
    size_t sumCount = 0;
    auto dir = getDataPath();
    VinIDStorage vinIDStorage(dir);
    vinIDStorage.vopen();
    vinIDStorage.writeNum(eMap->size);
    // eMap iteration
    for (int i = 0; i < eMap->cap; ++i) {
        auto& t = eMap->hashSpace[i];
        if (t.count > 0) {
            // sumCount += t.count;
            if (t.count >= 1) {
                TSDBIndex* index = t.first.hIndex;

                vinIDStorage.recordVinIDMap(sumCount, index->getVinID(),
                                            index->getVin());
                sumCount++;
            }
            for (auto it = t.nodevec.begin(); it != t.nodevec.end(); ++it) {
                TSDBIndex* index = it->hIndex;
                vinIDStorage.recordVinIDMap(sumCount, index->getVinID(),
                                            index->getVin());
                sumCount++;
            }
        }
    }

    vinIDStorage.vclose();

    return 0;
}

TSDBEngineImpl::~TSDBEngineImpl() {
    printInfo();

    // release timer thread
    if (timerThread) {
        timerStopSig = 1;
        delete timerThread;
    }
    // release index
    if (eMap)
        delete eMap;
    if (dbSchema)
        delete dbSchema;

    for (int i = 0; i < DatabasePartNum; ++i) {
        if (dbParts[i])
            delete dbParts[i];
    }

    // if (firstLoad) {
    //     freeShm(shareId1, (char*)staticMem, false);rn
    //     freeShm(shareId2, (char*)staticOffset, false);
    // } else {
    //     freeShm(shareId1, (char*)staticMem, true);
    //     freeShm(shareId2, (char*)staticOffset, true);
    // }

    malloc_trim(0);
    processMem_t m1 = GetProcessMemory();
    std::cout << "After Shutdown Mem: " << m1.virtualMem / 1024.0 << "MB"
              << std::endl;
}

}  // namespace LindormContest
