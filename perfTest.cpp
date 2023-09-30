//
//
//
#include <stdint.h>
#include <algorithm>
#include <filesystem>
#include <future>
#include <iostream>
#include <random>
#include <thread>
#include "ColumnFamily.h"
#include "Easyuomap.h"
#include "RTCompressDB.h"
#include "TSDBEngineImpl.h"
#include "TSDBIndex.h"
#include "config.h"
#include "omp.h"

// int main(int argc, char **argv) {
//     std::mt19937_64 rng(2);    // 种子，可以选择时间作为seed
//     std::uniform_int_distribution<uint64_t> distribution(1, 100);    //
//     设置范围 std::cout << distribution(rng) << " -----  " <<
//     distribution(rng) << std::endl; std::cout << distribution(rng) << " -----
//     " << distribution(rng) << std::endl; std::cout << distribution(rng) << "
//     -----  " << distribution(rng) << std::endl; return 0;
// }

const int thread_num = 8;
const int thread_read_num = 16;
const int maxEntity = vinMaxSize;
const int64_t tBase = 10701000000;
const int64_t tMax = queryRangeSortSize;
int64_t timeL = tBase, timeR = tBase + tMax;

// const int rangeTest = 500;

char strbuf[1024];
std::mt19937_64 rng(42);
std::uniform_int_distribution<uint64_t> distTimestamp(0, 3600 * 1000LL - 1);
// std::uniform_int_distribution<uint64_t> distTimestamp(0, 10000);
std::uniform_int_distribution<uint64_t> distVin(0, maxEntity - 1);
std::uniform_int_distribution<uint32_t> dist4BInt(0, UINT32_MAX - 1);
std::uniform_real_distribution<> dist8BReal(0, 1);

std::vector<LindormContest::Vin> Vins;

void randomStr(void* buf, int size) {
    char* data = (char*)buf;
    const char charset[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    for (int i = 0; i < size; i++) {
        int index = rand() % (sizeof(charset) - 1);
        data[i] = charset[index];
    }
}

void initEntity() {
    for (int i = 0; i < maxEntity; ++i) {
        LindormContest::Vin v;
        randomStr(v.vin, 17);
        Vins.push_back(v);
    }
}

void readEmap(LindormContest::EasyUnorderedMap* emap) {
    for (auto& vin : Vins) {
        auto f = emap->query(vin);
    }
}

void readUmap(std::unordered_map<LindormContest::Vin,
                                 LindormContest::TSDBIndex*,
                                 LindormContest::VinHasher,
                                 LindormContest::VinHasher>* umap) {
    for (auto& vin : Vins) {
        auto f = umap->find(vin);
    }
}
void testMapWRVin() {
    std::unordered_map<LindormContest::Vin, LindormContest::TSDBIndex*,
                       LindormContest::VinHasher, LindormContest::VinHasher>
        umap;
    const int VinNum = 300000;

    LindormContest::EasyUnorderedMap* emap =
        new LindormContest::EasyUnorderedMap(VinNum * 6,
                                             "/tmp/db_tsdb_test/test");

    std::mutex lock;
    for (int i = 0; i < VinNum; ++i) {
        LindormContest::Vin v;
        randomStr(v.vin, 17);
        Vins.push_back(v);

        // lock.lock();
        umap.insert(std::move(std::make_pair(v, nullptr)));
        // lock.unlock();
        // emap->insertSpdTest(v);
    }

    const int mapTestThreadNum = 32;
    std::thread thTest[mapTestThreadNum];
    for (int i = 0; i < mapTestThreadNum; ++i) {
        // thTest[i] = std::thread(readEmap, emap);
        thTest[i] = std::thread(readUmap, &umap);
    }
    for (int i = 0; i < mapTestThreadNum; ++i) {
        thTest[i].join();
    }
}

int createTable(LindormContest::TSDBEngine* engine) {
    LindormContest::Schema schema1;
    schema1.columnTypeMap["t1c1"] = LindormContest::COLUMN_TYPE_INTEGER;
    schema1.columnTypeMap["t1c2"] = LindormContest::COLUMN_TYPE_DOUBLE_FLOAT;
    schema1.columnTypeMap["t1c3"] = LindormContest::COLUMN_TYPE_STRING;
    schema1.columnTypeMap["t1c4"] = LindormContest::COLUMN_TYPE_DOUBLE_FLOAT;
    schema1.columnTypeMap["t1c5"] = LindormContest::COLUMN_TYPE_STRING;
    schema1.columnTypeMap["t1c6"] = LindormContest::COLUMN_TYPE_STRING;

    int ret = engine->createTable("t1", schema1);
    if (ret != 0) {
        std::cerr << "Create table 1 failed" << std::endl;
        return -1;
    }
    return 0;
}

void genRow(LindormContest::Vin& vin, uint64_t ts, LindormContest::Row& row) {
    // int eid = distVin(rng);
    // row.vin = Vins[eid];
    // row.timestamp = timeL + distTimestamp(rng);
    row.vin = vin;
    row.timestamp = ts;
    // std::string str(row.vin.vin, 17);
    // std::cout << str << " " << eid << std::endl;

    // int v1 = dist4BInt(rng);
    // auto f1 = std::hash<int>();
    std::hash<std::string> f1;
    int v1 = f1(std::string(vin.vin, 17) + std::to_string(ts));
    // if (ts == tBase + tMax) {
    //     std::cout << std::string(vin.vin, 17) << " " << v1 << std::endl;
    // }
    v1 = ts - tBase;
    // v1 = 20000;
    v1 = -v1;

    row.columns.insert(std::make_pair("t1c1", v1));
    // double v2 = v1 * dist8BReal(rng);
    double v2 = v1 * 0.25;
    // v2 = 256;
    row.columns.insert(std::make_pair("t1c2", v2));

    std::string str = std::to_string(v1) + std::to_string(v2);
    int strlen = str.size();
    row.columns.insert(std::make_pair(
        "t1c3", LindormContest::ColumnValue(str.c_str(), strlen)));

    double v4 = v1 * 0.35;
    row.columns.insert(std::make_pair("t1c4", v4));

    str = std::to_string(v1) + std::to_string(v4);
    strlen = str.size();
    row.columns.insert(std::make_pair(
        "t1c5", LindormContest::ColumnValue(str.c_str(), strlen)));

    str = std::to_string(v2) + std::to_string(v4);
    strlen = str.size();
    row.columns.insert(std::make_pair(
        "t1c6", LindormContest::ColumnValue(str.c_str(), strlen)));
}

/*
  generate random data (write process)
*/
int writeProcess(int tnum, int tstart, LindormContest::TSDBEngine* engine) {
    // Execute upsert.
    LindormContest::WriteRequest wReq;
    // if (promiseObj)
    //     promiseObj->set_value(-1);

    wReq.tableName = "t1";
    //
    for (int k = 0; k < maxEntity; ++k) {
        wReq.rows.clear();
        auto vin = Vins[k];
        std::vector<int> random_seq(tnum);
        std::iota(random_seq.begin(), random_seq.end(), 1);
        std::shuffle(random_seq.begin(), random_seq.end(), std::mt19937());

        for (auto& n : random_seq) {
            int64_t ts = tBase + n + tstart;
            LindormContest::Row tmpRow;
            genRow(vin, ts, tmpRow);
            wReq.rows.push_back(tmpRow);
        }
        // std::cout << k << std::endl;
        int ret = engine->write(wReq);
        if (ret != 0) {
            std::cerr << "Upsert failed" << std::endl;
            return ret;
        }
    }
    // if (promiseObj)
    //     promiseObj->set_value(0);
    return 0;
}

/*
  read test only, not correctness verification..
*/
int readProcess(int testNum, LindormContest::TSDBEngine* engine) {
    LindormContest::LatestQueryRequest pReadReq;
    std::vector<LindormContest::Row> pReadRes;
    pReadReq.tableName = "t1";
    pReadReq.requestedColumns.insert("t1c1");
    pReadReq.requestedColumns.insert("t1c2");
    pReadReq.requestedColumns.insert("t1c3");
    pReadReq.requestedColumns.insert("t1c4");
    pReadReq.requestedColumns.insert("t1c5");
    pReadReq.requestedColumns.insert("t1c6");

    std::uniform_int_distribution<uint64_t> distReadVin(0, maxEntity - 1);
    std::uniform_int_distribution<uint64_t> rnd02(0, 1);
    std::mt19937_64 rngRead(40);

    LindormContest::TimeRangeQueryRequest trR;
    trR.tableName = "t1";
    std::vector<LindormContest::Row> trReadRes;
    std::uniform_int_distribution<uint64_t> dist1(timeL,
                                                  timeR - 1);  // 设置范围;
    std::uniform_int_distribution<uint64_t> dist2(5,
                                                  15);  // 设置范围;

    for (int i = 0; i < testNum; ++i) {
        // int f = rnd02(rngRead);
        int f = 1;
        // 随机读latest 或 range  query
        if (f != 0) {
            int id = distReadVin(rngRead);
            pReadReq.vins.push_back(Vins[id]);
            pReadRes.clear();
            int ret = engine->executeLatestQuery(pReadReq, pReadRes);
            // if (ret != 0) {
            //     std::cerr << "Cannot query" << std::endl;
            //     return -1;
            // }
        } else {
            int id = distReadVin(rngRead);

            trR.timeLowerBound = dist1(rngRead);
            trR.timeUpperBound =
                std::min(trR.timeLowerBound + (int64_t)dist2(rngRead), timeR);

            trR.vin = Vins[id];

            trReadRes.clear();
            int ret = engine->executeTimeRangeQuery(trR, trReadRes);
            // if (ret != 0) {
            //     std::cerr << "Query time range failed" << std::endl;
            //     return -1;
            // }
        }
    }

    return 0;
}
std::mt19937 gen(std::random_device{}());

/*
 *  correctness verification (simple process)
 */
static int verifyTableData(LindormContest::TSDBEngine* engine,
                           int latestRepeat = 1) {
    // Execute latest query for part.
    LindormContest::LatestQueryRequest pReadReq;
    std::vector<LindormContest::Row> pReadRes;
    pReadReq.tableName = "t1";
    pReadReq.requestedColumns.insert("t1c1");
    pReadReq.requestedColumns.insert("t1c2");
    pReadReq.requestedColumns.insert("t1c3");
    pReadReq.requestedColumns.insert("t1c4");
    pReadReq.requestedColumns.insert("t1c5");
    pReadReq.requestedColumns.insert("t1c6");
    std::vector<int> rank(maxEntity);
    std::iota(rank.begin(), rank.end(), 0);
    // std::shuffle(rank.begin(), rank.end(), gen);

    // 1, latest query (one by one)
    for (auto k : rank) {
        pReadReq.vins.clear();
        pReadReq.vins.push_back(Vins[k]);

        pReadRes.clear();
        int ret = engine->executeLatestQuery(pReadReq, pReadRes);
        if (ret != 0) {
            std::cerr << "Cannot query" << std::endl;
            return -1;
        }

        LindormContest::Row temp;
        genRow(Vins[k], tMax + tBase, temp);
        CTimestamp ff = tMax + tBase;
        // int* pt = (int*)pReadRes[0].columns["t1c1"].columnData;
        // int* pt2 = (int*)temp.columns["t1c1"].columnData;
        // std::cout << *pt << " " << *pt2 << std::endl;
        if (pReadRes.size() == 0 || temp.columns != pReadRes[0].columns) {
            std::cerr << "Query result wrong in latest query (one by one)"
                      << std::endl;
            return -1;
        }
    }

    // // 2, latest query (package)
    for (int aa = 0; aa < latestRepeat; ++aa) {
        pReadReq.vins.clear();
        for (auto k : rank) {
            pReadReq.vins.push_back(Vins[k]);
        }
        pReadRes.clear();
        int ret = engine->executeLatestQuery(pReadReq, pReadRes);
        if (ret != 0) {
            std::cerr << "Cannot query" << std::endl;
            abort();
            return -1;
        }

        for (auto k : rank) {
            LindormContest::Row temp;
            genRow(Vins[k], tMax + tBase, temp);
            if (pReadRes.size() == 0 || temp.columns != pReadRes[k].columns) {
                std::cerr << "Query result wrong in latest query (package)"
                          << std::endl;
                abort();
                return -1;
            }
        }
    }
    // =====

    // // 3. Execute time range query for full.

    LindormContest::TimeRangeQueryRequest trR;
    trR.tableName = "t1";
    std::vector<LindormContest::Row> trReadRes;
    std::uniform_int_distribution<uint64_t> dist1(timeL,
                                                  timeR - 1);  // 设置范围;
    std::uniform_int_distribution<uint64_t> dist2(20,
                                                  40);  // 设置范围;

    for (int a = 0; a < 10; ++a) {
        for (auto k : rank) {
            // int r = distVin(rng);
            int r = k;

            trR.timeLowerBound = dist1(rng) + 1;
            trR.timeUpperBound =
                std::min(trR.timeLowerBound + (int64_t)dist2(rng), timeR + 1);

            trR.vin = Vins[r];

            trReadRes.clear();
            int ret = engine->executeTimeRangeQuery(trR, trReadRes);
            if (ret != 0) {
                std::cerr << "Query time range failed" << std::endl;
                abort();
                return -1;
            }

            if (trReadRes.size() != trR.timeUpperBound - trR.timeLowerBound) {
                std::cerr << "Result number of Query time range dismatch"
                          << std::endl;
                abort();
                return -1;
            }

            // 验证结果正确性W
            for (auto& res : pReadRes) {
                LindormContest::Row temp;
                genRow(res.vin, res.timestamp, temp);
                if (temp.columns != res.columns) {
                    std::cerr << "Query result wrong in time range query"
                              << std::endl;
                    abort();
                    return -1;
                }
            }
        }
    }

    // executeAggregateQuery Test
    LindormContest::TimeRangeAggregationRequest agg1;
    std::vector<LindormContest::Row> aggRes1;
    agg1.tableName = "t1";
    agg1.aggregator = LindormContest::MAX;
    agg1.columnName = "t1c1";  // 124
    agg1.timeLowerBound = tBase;
    agg1.timeUpperBound = tBase + tMax;
    Timestamp interval = tMax * 0.3;

    for (auto k : rank) {
        Timestamp t1 = tBase;
        Timestamp t2 = t1 + interval;
        while (t2 < tBase + tMax) {
            agg1.timeLowerBound = t1;
            agg1.timeUpperBound = t2;
            agg1.vin = Vins[k];
            aggRes1.clear();
            engine->executeAggregateQuery(agg1, aggRes1);
            // std::cout << aggRes1.size() << std::endl;
            // double_t v;
            // aggRes1[0].columns[agg1.columnName].getDoubleFloatValue(v);
            int32_t v;
            if (aggRes1.size() > 0)
                aggRes1[0].columns[agg1.columnName].getIntegerValue(v);

            t1 += interval;
            t2 = t1 + interval;
        }
    }

    // executeDownsampleQuery Test
    LindormContest::TimeRangeDownsampleRequest agg2;
    std::vector<LindormContest::Row> aggRes2;
    agg2.tableName = "t1";
    agg2.aggregator = LindormContest::MAX;
    agg2.columnName = "t1c2";  // 124
    agg2.timeLowerBound = tBase;
    agg2.timeUpperBound = tBase + tMax;
    LindormContest::CompareExpression compare0;
    compare0.compareOp = LindormContest::GREATER;
    double_t setValue = 3000;
    compare0.value = LindormContest::ColumnValue(setValue);
    agg2.columnFilter = compare0;
    agg2.interval = 1000000;

    for (auto k : rank) {
        agg2.vin = Vins[k];

        setValue = 500 + k;
        compare0.value = LindormContest::ColumnValue(setValue);
        agg2.columnFilter = compare0;
        agg2.aggregator =
            k % 2 == 0 ? LindormContest::MAX : LindormContest::AVG;

        aggRes2.clear();
        engine->executeDownsampleQuery(agg2, aggRes2);
        // std::cout << aggRes2.size() << std::endl;
        int ci = 0;
        for (auto& s : aggRes2) {
            assert(s.timestamp == agg2.timeLowerBound + agg2.interval * ci);
            auto col = s.columns[agg2.columnName];
            if (col.getColumnType() == LindormContest::COLUMN_TYPE_INTEGER) {
                int32_t v;
                col.getIntegerValue(v);
                assert(v >= setValue || INT_NAN == v);
                // if (v != INT_NAN) {
                //     int kk = 0;
                // }
                // assert(v == INT_NAN);
            } else if (col.getColumnType() ==
                       LindormContest::COLUMN_TYPE_DOUBLE_FLOAT) {
                double_t v;
                col.getDoubleFloatValue(v);
                assert(v >= setValue || DOUBLE_NAN == v);
                // if (v != DOUBLE_NAN) {
                //     int kk = 0;
                // }
            }
            ci++;
        }
    }

    return 0;
}
void countnumber(unsigned int n) {
    // for (unsigned int i = 1; i <= n; i++)
    //     ;
    auto id = std::this_thread::get_id();
    std::cout << "Thread " << id << " finished!"
              << " " << n << std::endl;
}

int main() {
    std::cout << sizeof(LindormContest::UmapNode) << " "
              << sizeof(LindormContest::UmapSlot) << " "
              << sizeof(LindormContest::UmapSlot) * vinMaxSize << std::endl;

    std::cout << sizeof(LindormContest::RowIndex) << std::endl;
    std::cout << sizeof(CompressIndex) << std::endl;

    // double start = omp_get_wtime();

    // auto tt0 = std::chrono::steady_clock::now();

    // testMapWRVin();

    // auto tt1 = std::chrono::steady_clock::now();

    // std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(tt1 -
    //                                                                    tt0)
    //                      .count() *
    //                  1.0 / 1000
    //           << "s" << std::endl;

    // return 0;

    // std::vector<std::map<std::string, std::string>> vtest;
    // std::map<std::string, std::string> temp;
    // int colN = 10;
    // for (int i = 0; i < colN; ++i) {
    //     std::string s = std::to_string(i);
    //     temp[s] = s;
    // }

    // // for (int i = 0; i < 100000; ++i) {
    // //     std::map<std::string, std::string> r(temp);
    // //     for (int i = 0; i < colN; ++i) {
    // //         std::string s = std::to_string(i);
    // //         std::string s2 = std::to_string(i + 5);
    // //         r[s] = s2;
    // //     }
    // //     // vtest.emplace_back(std::move(r));
    // //     vtest.push_back(std::move(r));
    // // }

    // for (int i = 0; i < 100000; ++i) {
    //     std::map<std::string, std::string> r(temp);
    //     // vtest.emplace_back(std::move(r));
    //     vtest.push_back(std::move(r));
    // }

    // for (int i = 0; i < 100000; ++i) {
    //     auto& v = vtest[i];
    //     for (int i = 0; i < colN; ++i) {
    //         std::string s = std::to_string(i);
    //         std::string s2 = std::to_string(i + 5);
    //         // v.at(s) = s2;
    //         v[s] = s2;

    //         // temp.emplace(s, s2);
    //     }
    // }

    // auto tt2 = std::chrono::steady_clock::now();
    // // std::cout << vtest[12]["6"] << std::endl;

    // std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(tt2 -
    //                                                                    tt0)
    //                      .count() *
    //                  1.0 / 1000
    //           << "s" << std::endl;

    // return 0;

    std::filesystem::path dbPath = std::filesystem::path("/tmp/db_tsdb_test");
    std::filesystem::remove_all(dbPath);
    std::filesystem::create_directory(dbPath);

    // clock_t t0 = clock();
    auto t0 = std::chrono::steady_clock::now();
    // prepareStaticVariables();

    // std::thread th[thread_num];
    // for (int i = 0; i < thread_num; i++)
    //     th[i] = std::thread(countnumber, i);
    // for (int i = 0; i < thread_num; i++)
    //     th[i].join();
    // return 0;

    initEntity();

    LindormContest::TSDBEngine* engine =
        new LindormContest::TSDBEngineImpl(dbPath.string());

    int ret = engine->connect();
    if (ret != 0) {
        std::cerr << "Connect db failed" << std::endl;
        return -1;
    }
    ret = createTable(engine);
    if (ret != 0) {
        std::cerr << "Create table failed" << std::endl;
        return -1;
    }

    std::thread th[thread_num];
    // std::promise<int> promiseObj[thread_num];
    // std::future<int> futureObj[thread_num];

    for (int i = 0; i < thread_num; i++) {
        int size = tMax / thread_num;
        if (size * thread_num != tMax && i == thread_num - 1)
            size += tMax - size * thread_num;
        th[i] = std::thread(writeProcess, size, tMax / thread_num * i, engine);
    }
    // writeProcess(tMax, 0, engine);

    for (int i = 0; i < thread_num; i++) {
        th[i].join();
    }

    // th_read[0].join();
    // th_read[1].join();

    // ret = writeProcess(tMax, 0, engine);
    // if (ret != 0) {
    //     std::cerr << "Write data failed" << std::endl;
    //     return -1;
    // }
    std::cout << "multiple- thread done" << std::endl;
    // verifyTableData(engine);
    // ret = verifyTableData(engine);
    // LindormContest::LatestQueryRequest q;
    // std::vector<LindormContest::Row> r;
    // q.tableName = "t1";
    // q.requestedColumns.insert("t1c1");
    // LindormContest::Vin vin;
    // memset(vin.vin, '0', 17);
    // q.vins.emplace_back(vin);
    // engine->executeLatestQuery(q, r);
    // if (ret != 0) {
    //     std::cerr << "Verify data before we restart the db failed" <<
    //     std::endl; return -1;
    // }

    // if (r.size() != 0) {
    //     std::cout << "empty query fail " << std::endl;
    //     return -1;
    // }
    printf(
        "========= PASSED data verification before we restart the db  "
        "=========\n");

    // Restart db.
    engine->shutdown();
    delete engine;

    engine = new LindormContest::TSDBEngineImpl(dbPath.string());

    ret = 0;
    ret = engine->connect();
    if (ret != 0) {
        std::cerr << "Connect db failed" << std::endl;
        return -1;
    }

    auto t00 = std::chrono::steady_clock::now();
    // Verify data.
    std::thread th_read[thread_read_num];
    for (int i = 0; i < thread_read_num; ++i) {
        th_read[i] = std::thread(verifyTableData, engine, 1);
    }

    for (int i = 0; i < thread_read_num; i++) {
        th_read[i].join();
    }

    ret = verifyTableData(engine, 1);
    if (ret != 0) {
        std::cerr << "Verify data after we restart the db failed" << std::endl;
        return -1;
    }

    printf(
        "========= PASSED data verification [AFTER] we restart the db  "
        "=========\n");

    auto t01 = std::chrono::steady_clock::now();

    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(t01 -
                                                                       t00)
                         .count() *
                     1.0 / 1000
              << "s" << std::endl;

    engine->shutdown();
    delete engine;

    // clock_t t1 = clock();
    auto t1 = std::chrono::steady_clock::now();

    // std::cout << (t1 - t0)/ 1000.0 / 1000 << "s" << std::endl;
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0)
                         .count() *
                     1.0 / 1000
              << "s" << std::endl;

    // double end = omp_get_wtime();
    // std::cout << "cost time:" << (end - start) << "s\n"
    //           << std::endl;  // 每s的时钟周期数
    return 0;
}
