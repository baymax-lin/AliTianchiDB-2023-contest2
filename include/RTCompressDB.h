#pragma once
#include "ColumnFamily.h"
#include "Easyuomap.h"
#include "TSDBIndex.h"
#include "TSDBTypes.h"
#include "config.h"
#include "struct/Row.h"
#include "struct/Vin.h"
// #include "struct/Column.h"
extern volatile uint32_t maxStrSize;

typedef struct ColumnIndex {
    uint8_t ctype;
    uint16_t familyId;
    uint16_t offset;  // 如果是实数型, 则该值表示偏移; 若为str型,
                      // 该值表示str列族下的第几个str列.
} ColumnIndex;

class DbSchema {
   public:
    ColumnIndex* columnIndex;  // 表示第n列对应的列族和列族内索引
    int columnsNum;
    // The column's type for each column.
    LindormContest::ColumnType* columnsType;
    // The column's name for each column.
    std::string* columnsName;

    int cfType[ColumnFamilyNum];
    int cfTypeNum[ColumnFamilyNum];
    uint32_t rnCFIndex
        [ColumnMaxSize];  // 表示实数列-第i列
                          // 对应在该列族的偏移(实数列偏移固定,文本列不固定)
                          // 暂且分为实数列族(INT_OR_DOUBLE) 和 文本列族(STR)

    uint32_t rnSize;

    // vinID map to vin
    std::unordered_map<VinID, LindormContest::Vin> vinMap;
    // vinID map to vinIndex
    std::unordered_map<VinID, LindormContest::TSDBIndex*> vinIndexMap;

    std::unordered_map<std::string, uint32_t> columnInRnOffset;

    // pair(colIndex, type)
    std::unordered_map<std::string, std::pair<int, LindormContest::ColumnType>>
        columnTypeByName;

    // rn/str column u64 flag
    uint64_t rnColumnsU64 = 0;
    uint64_t strColumnsU64 = 0;

    // STAT all queried columns
    uint32_t queriedMAX[ColumnMaxSize];
    uint32_t queriedAVG[ColumnMaxSize];

    const int queryRangeWindow = 100;
    uint64_t queryRange = 0, queryCount = 0;
    uint64_t queryRangeAgg = 0, queryAggCount = 0;
    uint64_t queryRangeDownsample = 0, queryDownsampleCount = 0;

    Timestamp timeBase = 0;

    DbSchema(int _n) {
        assert(_n >= 1);
        columnsNum = _n;
        columnsType = new LindormContest::ColumnType[columnsNum];
        columnsName = new std::string[columnsNum];

        columnIndex = new ColumnIndex[columnsNum];

        cfType[COLUMNFAMILY_INT_OR_DOUBLE] = COLUMNFAMILY_INT_OR_DOUBLE;
        cfType[COLUMNFAMILY_STR] = COLUMNFAMILY_STR;

        cfTypeNum[0] = cfTypeNum[1] = 0;
        rnSize = 0;

        memset(queriedMAX, 0, sizeof(queriedMAX));
        memset(queriedAVG, 0, sizeof(queriedAVG));
    }
    ~DbSchema() {
        delete[] columnsType;
        delete[] columnsName;
        delete[] columnIndex;
    }
    void initColuimnFamilyIndex() {
        // 初始化不同列族中列的数量
        size_t offsetInRN = 0, countInRN = 0;

        rnColumnsU64 = strColumnsU64 = 0;
        for (int i = 0; i < columnsNum; ++i) {
            if (columnsType[i] == LindormContest::COLUMN_TYPE_DOUBLE_FLOAT) {
                columnTypeByName[columnsName[i]] =
                    std::make_pair(i, LindormContest::COLUMN_TYPE_DOUBLE_FLOAT);

                columnInRnOffset[columnsName[i]] = offsetInRN;
                rnCFIndex[countInRN++] = offsetInRN;
                cfTypeNum[COLUMNFAMILY_INT_OR_DOUBLE]++;
                offsetInRN += sizeof(double_t);
                rnSize += sizeof(double_t);

                rnColumnsU64 |= (1ULL << i);
            } else if (columnsType[i] == LindormContest::COLUMN_TYPE_INTEGER) {
                columnTypeByName[columnsName[i]] =
                    std::make_pair(i, LindormContest::COLUMN_TYPE_INTEGER);

                columnInRnOffset[columnsName[i]] = offsetInRN;
                rnCFIndex[countInRN++] = offsetInRN;
                cfTypeNum[COLUMNFAMILY_INT_OR_DOUBLE]++;
                offsetInRN += sizeof(int32_t);
                rnSize += sizeof(int32_t);

                rnColumnsU64 |= (1ULL << i);

            } else if (columnsType[i] == LindormContest::COLUMN_TYPE_STRING) {
                columnTypeByName[columnsName[i]] =
                    std::make_pair(i, LindormContest::COLUMN_TYPE_STRING);

                cfTypeNum[COLUMNFAMILY_STR]++;
                strColumnsU64 |= (1ULL << i);
            }
        }

        size_t offset1 = 0, strNum = 0;

        for (int i = 0; i < columnsNum; ++i) {
            if (columnsType[i] == LindormContest::COLUMN_TYPE_STRING) {
                columnIndex[i].familyId = COLUMNFAMILY_STR;
                columnIndex[i].offset = strNum;
                columnIndex[i].ctype = LindormContest::COLUMN_TYPE_STRING;
                strNum++;
            } else if (columnsType[i] == LindormContest::COLUMN_TYPE_INTEGER) {
                columnIndex[i].familyId = COLUMNFAMILY_INT_OR_DOUBLE;
                columnIndex[i].offset = offset1;
                columnIndex[i].ctype = LindormContest::COLUMN_TYPE_INTEGER;
                offset1 += sizeof(int32_t);
            } else if (columnsType[i] ==
                       LindormContest::COLUMN_TYPE_DOUBLE_FLOAT) {
                columnIndex[i].familyId = COLUMNFAMILY_INT_OR_DOUBLE;
                columnIndex[i].offset = offset1;
                columnIndex[i].ctype = LindormContest::COLUMN_TYPE_DOUBLE_FLOAT;
                offset1 += sizeof(double_t);
            }
        }
    }
};

class CompressDB {
   public:
    std::string directory;
    int dbPartId;
    ColumnFamily* cf[ColumnFamilyNum];
    std::mutex dblock;

    DbSchema* dbSchema;
    int cfNum = ColumnFamilyNum;

    CompressDB(const std::string& _directory, int _dbPart, DbSchema* _dbShema) {
        directory = _directory + "/" + std::to_string(_dbPart);
        cf[COLUMNFAMILY_INT_OR_DOUBLE] =
            new ColumnFamily(directory, "rn", COLUMNFAMILY_INT_OR_DOUBLE);
        cf[COLUMNFAMILY_STR] =
            new ColumnFamily(directory, "str", COLUMNFAMILY_STR);

        dbPartId = _dbPart;
        dbSchema = _dbShema;
    }
    ~CompressDB() {
        for (int i = 0; i < ColumnFamilyNum; ++i) {
            delete cf[i];
        }
    }

    ColumnFamily* getColumnFamilyEntity(int id) {
        // assert(id <= 1);
        return cf[id];
    }

    size_t write(const LindormContest::Row& row,
                 const VinID vid,
                 LindormContest::RowIndex& rIndex) {
        VinID vinID = vid;
        Timestamp ts = row.timestamp;

        dblock.lock();
        // write rowkey to db buffer
        for (int i = 0; i < ColumnFamilyNum; ++i) {
            if (!cf[i]->checkBuffer()) {
                cf[i]->flushBuffer();
            }
            cf[i]->add(&vinID, sizeof(VinID));
            cf[i]->add(&ts, sizeof(Timestamp));
        }
        // set index (offset including timestamp)
        rIndex.timestamp = ts;

        rIndex.blkRnId = cf[COLUMNFAMILY_INT_OR_DOUBLE]->curBlockId;
        rIndex.rnOffset = cf[COLUMNFAMILY_INT_OR_DOUBLE]->bufOffset;
        rIndex.blkStrId = cf[COLUMNFAMILY_STR]->curBlockId;
        rIndex.strOffset = cf[COLUMNFAMILY_STR]->bufOffset;

        // write row data to db buffer
        int colCount = 0;
        uint32_t strSize = 0;
        for (auto it = row.columns.begin(); it != row.columns.end(); ++it) {
            const LindormContest::ColumnValue& cVal = it->second;
            int32_t rawSize = cVal.getRawDataSize();
            int cfType = dbSchema->columnIndex[colCount].familyId;
            cf[cfType]->add(cVal.columnData, rawSize);
            if (cfType == COLUMNFAMILY_STR)
                strSize += rawSize;
            colCount++;
        }

        // set str cf size
        rIndex.strSize = strSize;
        dblock.unlock();
        // if (strSize > MinReserveBufferSize - 12) {
        //     // static int KKK = 0;
        //     // if (KKK < 30) {
        //     //     printf("TOO LONG STR:%d\n", strSize);
        //     //     KKK++;
        //     // }
        //     // maxStrSize = std::max(maxStrSize, strSize);
        //     if (maxStrSize < strSize) {
        //         maxStrSize = strSize;
        //     }
        // }
        return strSize + dbSchema->rnSize +
               (sizeof(Timestamp) + sizeof(VinID)) * 2;
    }

    int read(LindormContest::Row& row,
             const LindormContest::Vin& vin,
             LindormContest::RowIndex& rIndex,
             uint32_t* reqCols,
             bool needStrColumnFamily,
             char* cacheBuffer = nullptr,
             uint16_t* cacheColIndex = nullptr) {
        char rnBuf[MaxRowSize];
        char strBuf[MaxRowSize];

        row.vin = vin;
        assert(rIndex.strSize < 16384);

        cf[COLUMNFAMILY_INT_OR_DOUBLE]->get(
            rnBuf, rIndex.blkRnId, rIndex.rnOffset - sizeof(Timestamp),
            dbSchema->rnSize + sizeof(Timestamp));

        if (needStrColumnFamily) {
            cf[COLUMNFAMILY_STR]->get(strBuf, rIndex.blkStrId, rIndex.strOffset,
                                      rIndex.strSize);
        }

        // If need cache row data and index:
        if (cacheBuffer && cacheColIndex) {
            assert(needStrColumnFamily);
            // timestamp
            memcpy(cacheBuffer, rnBuf, sizeof(Timestamp));
            // rntype data
            memcpy(cacheBuffer + sizeof(Timestamp), rnBuf + sizeof(Timestamp),
                   dbSchema->rnSize);
            // strtype data
            memcpy(cacheBuffer + sizeof(Timestamp) + dbSchema->rnSize, strBuf,
                   rIndex.strSize);

            size_t rn0 = sizeof(Timestamp);
            size_t str0 = 0;

            for (int i = 0; i < dbSchema->columnsNum; ++i) {
                int ctype = dbSchema->columnsType[i];
                if (ctype == LindormContest::COLUMN_TYPE_DOUBLE_FLOAT) {
                    cacheColIndex[i] = rn0;
                    rn0 += sizeof(double_t);
                } else if (ctype == LindormContest::COLUMN_TYPE_INTEGER) {
                    cacheColIndex[i] = rn0;
                    rn0 += sizeof(int32_t);
                } else if (ctype == LindormContest::COLUMN_TYPE_STRING) {
                    cacheColIndex[i] =
                        sizeof(Timestamp) + dbSchema->rnSize + str0;
                    size_t curstrlen = *(uint32_t*)(strBuf + str0);
                    assert(curstrlen < 16384);
                    str0 += curstrlen + sizeof(uint32_t);
                }
            }
        }

        int cI = 0, strCount = 0, strOffset = 0;
        row.timestamp = *(Timestamp*)rnBuf;
        char* rn = rnBuf + sizeof(Timestamp);
        char* strb = strBuf;
        // strOffset = sizeof(Timestamp);

        for (auto it = row.columns.begin(); it != row.columns.end(); ++it) {
            uint32_t col = reqCols[cI];
            auto& cfInfo = dbSchema->columnIndex[col];

            if (cfInfo.familyId == COLUMNFAMILY_INT_OR_DOUBLE) {
                // 此时cfInfo.offseti表示在rn列族中的具体偏移
                if (cfInfo.ctype == LindormContest::COLUMN_TYPE_INTEGER) {
                    int32_t value = *(int32_t*)(rn + cfInfo.offset);
                    it->second = std::move(LindormContest::ColumnValue(value));
                } else {  // DOUBLE_FLOAT
                    double_t value = *(double_t*)(rn + cfInfo.offset);
                    it->second = std::move(LindormContest::ColumnValue(value));
                }
            } else if (cfInfo.familyId == COLUMNFAMILY_STR) {
                assert(needStrColumnFamily);
                // 此时cfInfo.offset表示str列族中第几列(因为每行数据不定长
                // 无法由schema统一标记具体偏移)
                while (strCount != cfInfo.offset &&
                       strCount < dbSchema->columnsNum) {
                    strCount++;
                    uint32_t strLen = *(uint32_t*)(strb + strOffset);
                    strOffset += strLen + sizeof(uint32_t);
                }
                if (strCount == cfInfo.offset) {
                    uint32_t strLen = *(uint32_t*)(strb + strOffset);
                    it->second = std::move(LindormContest::ColumnValue(
                        strb + strOffset + sizeof(uint32_t), strLen));
                    strCount++;
                    strOffset += strLen + sizeof(uint32_t);
                }
            }
            cI++;
        }

        return 0;
    }

    int aggregate(std::vector<LindormContest::Row>& resView,
                  LindormContest::Vin& vin,
                  std::string aggColname,
                  LindormContest::ColumnType ctype,
                  std::vector<LindormContest::RNColumnIndex>& rnColumnIndex,
                  LindormContest::Aggregator aggregator) {
        double_t doubleRes;
        int32_t resCount = 0;

        if (aggregator == LindormContest::MAX) {
            doubleRes = numeric_limits<double>::lowest();
        } else if (aggregator == LindormContest::AVG) {
            doubleRes = 0;
        }
        assert(resView.size() == 1);

        if (ctype == LindormContest::COLUMN_TYPE_STRING)
            return 0;

        // sort(rnColumnIndex.begin(), rnColumnIndex.end());

        size_t specRnOffset = dbSchema->columnInRnOffset[aggColname];

        int curBlockId = -1;
        char* data = new char[BlockBufferSize];
        for (int i = 0; i < rnColumnIndex.size(); ++i) {
            auto& rn = rnColumnIndex[i];
            if (rn.blkRnId != curBlockId) {
                cf[COLUMNFAMILY_INT_OR_DOUBLE]->get(data, rn.blkRnId, 0);
                curBlockId = rn.blkRnId;
            }
            uint32_t viewId = rn.viewId;

            char* f = data + rn.rnOffset + specRnOffset;
            if (ctype == LindormContest ::COLUMN_TYPE_INTEGER) {
                int32_t val = *(int32_t*)f;
                LindormContest::ColumnValue cval(val);

                resCount++;
                if (aggregator == LindormContest::AVG) {
                    doubleRes += val;
                } else if (aggregator == LindormContest::MAX) {
                    doubleRes = std::max(doubleRes, (double_t)val);
                }

            } else if (ctype == LindormContest::COLUMN_TYPE_DOUBLE_FLOAT) {
                double_t val = *(double_t*)f;
                LindormContest::ColumnValue cval(val);
                resCount++;
                if (aggregator == LindormContest::AVG) {
                    doubleRes += val;
                } else if (aggregator == LindormContest::MAX) {
                    doubleRes = std::max(doubleRes, (double_t)val);
                }
            }
        }

        auto& r = resView[0];
        if (aggregator == LindormContest::AVG) {
            double_t avg = resCount > 0 ? doubleRes / resCount : DOUBLE_NAN;
            r.columns[aggColname] = std::move(LindormContest::ColumnValue(avg));
        } else if (aggregator == LindormContest::MAX) {
            if (ctype == LindormContest ::COLUMN_TYPE_INTEGER) {
                int32_t maxn = resCount > 0 ? round(doubleRes) : INT_NAN;
                r.columns[aggColname] =
                    std::move(LindormContest::ColumnValue(maxn));

            } else if (ctype == LindormContest ::COLUMN_TYPE_DOUBLE_FLOAT) {
                double_t maxn = resCount > 0 ? doubleRes : DOUBLE_NAN;
                r.columns[aggColname] =
                    std::move(LindormContest::ColumnValue(maxn));
            }
        }

        delete[] data;

        return 0;
    }

    int downsample(std::vector<LindormContest::Row>& resView,
                   LindormContest::Vin& vin,
                   std::string aggColname,
                   LindormContest::ColumnType ctype,
                   std::vector<LindormContest::RNColumnIndex>& rnColumnIndex,
                   LindormContest::Aggregator aggregator,
                   LindormContest::CompareExpression* filter = nullptr) {
        double_t doubleRes[downSampleMaxPart];
        int32_t resCount[downSampleMaxPart];

        if (aggregator == LindormContest::MAX) {
            memset(doubleRes, 0xfe, sizeof(doubleRes));
        } else if (aggregator == LindormContest::AVG) {
            memset(doubleRes, 0x00, sizeof(doubleRes));
        }

        memset(resCount, 0, sizeof(resCount));

        assert(resView.size() < downSampleMaxPart);
        if (resView.size() == 0)
            return 0;
        if (ctype == LindormContest::COLUMN_TYPE_STRING)
            return 0;

        // sort(rnColumnIndex.begin(), rnColumnIndex.end());

        size_t specRnOffset = dbSchema->columnInRnOffset[aggColname];

        int curBlockId = -1;
        char* data = new char[BlockBufferSize];
        for (int i = 0; i < rnColumnIndex.size(); ++i) {
            auto& rn = rnColumnIndex[i];
            if (rn.blkRnId != curBlockId) {
                cf[COLUMNFAMILY_INT_OR_DOUBLE]->get(data, rn.blkRnId, 0);
                curBlockId = rn.blkRnId;
            }
            uint32_t viewId = rn.viewId;
            assert(viewId < downSampleMaxPart);

            char* f = data + rn.rnOffset + specRnOffset;
            if (ctype == LindormContest ::COLUMN_TYPE_INTEGER) {
                int32_t val = *(int32_t*)f;
                LindormContest::ColumnValue cval(val);
                // bool k = filter;
                // bool asd = k && filter->doCompare(cval);
                if (!filter || filter->doCompare(cval)) {
                    // if (val != 1024 && filter->doCompare(cval)) {
                    //     std::cout << " == " << std::endl;
                    //     bool cxd = filter->doCompare(cval);
                    // }
                    resCount[viewId]++;
                    if (aggregator == LindormContest::AVG) {
                        doubleRes[viewId] += val;
                    } else if (aggregator == LindormContest::MAX) {
                        doubleRes[viewId] =
                            std::max(doubleRes[viewId], (double_t)val);
                    }
                }

            } else if (ctype == LindormContest::COLUMN_TYPE_DOUBLE_FLOAT) {
                double_t val = *(double_t*)f;
                LindormContest::ColumnValue cval(val);
                if (!filter || filter->doCompare(cval)) {
                    resCount[viewId]++;
                    if (aggregator == LindormContest::AVG) {
                        doubleRes[viewId] += val;
                    } else if (aggregator == LindormContest::MAX) {
                        doubleRes[viewId] =
                            std::max(doubleRes[viewId], (double_t)val);
                    }
                }
            }
        }

        for (int i = 0; i < resView.size(); ++i) {
            auto& r = resView[i];
            // r.vin = vin;
            // r.timestamp =
            if (aggregator == LindormContest::AVG) {
                double_t avg =
                    resCount[i] > 0 ? doubleRes[i] / resCount[i] : DOUBLE_NAN;
                r.columns[aggColname] =
                    std::move(LindormContest::ColumnValue(avg));
            } else if (aggregator == LindormContest::MAX) {
                if (ctype == LindormContest ::COLUMN_TYPE_INTEGER) {
                    int32_t maxn =
                        resCount[i] > 0 ? round(doubleRes[i]) : INT_NAN;

                    r.columns[aggColname] =
                        std::move(LindormContest::ColumnValue(maxn));

                } else if (ctype == LindormContest ::COLUMN_TYPE_DOUBLE_FLOAT) {
                    double_t maxn = resCount[i] > 0 ? doubleRes[i] : DOUBLE_NAN;
                    r.columns[aggColname] =
                        std::move(LindormContest::ColumnValue(maxn));
                }
            }
        }

        delete[] data;

        return 0;
    }

    int compressAllFile() {
        for (int i = 0; i < ColumnFamilyNum; ++i) {
            cf[i]->compressFile();
        }
        return 0;
    }
    int decompressAllFile() {
        for (int i = 0; i < ColumnFamilyNum; ++i) {
            cf[i]->decompressFile();
        }
        return 0;
    }
};