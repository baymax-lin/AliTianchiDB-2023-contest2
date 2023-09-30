#pragma once
#define TEST_MODE 1
#define CONCURRENT_HASH

// CONSTANT
#define EN_LRU 0
#define CONCENTRATED_MODE 1

static int64_t DOUBLE_NAN_AS_LONG = 0xfff0000000000000L;
static double_t DOUBLE_NAN = *(double_t*)(&DOUBLE_NAN_AS_LONG);
static int32_t INT_NAN = 0x80000000ULL;

const int downSampleMaxPart = 256;  // 64;

const size_t MaxRowSize = 4ULL * 1024;
const int ColumnMaxSize = 64;
const int ColumnFamilyNum = 2;

// static

// 36000 5000
// 600 5000
const int queryRangeSortSize = 36000;
const size_t vinMaxSize = 50;

// const int queryRangeSortSize = 36000;
// const size_t vinMaxSize = 5000;

const size_t fileReserveSize = 2 * 1024 * 1024;

const size_t BlockBufferSize = 1 * 16ULL * 1024;
const size_t MinReserveBufferSize = 512;
const size_t MinRNReserveBufferSize = 60 * 8;

const size_t BlockCompressBufferUpperBound =
    // ZSTD_compressBound(BlockBufferSize);
    std::max(size_t(BlockBufferSize * 1.2), size_t(4096));
const int zstd_level = 3;

// const size_t RnColruCapcity = 80;
// const size_t StrColLruCapcity = 80;

const size_t RnColruCapcity = 32;
const size_t StrColLruCapcity = 32;

// const size_t RnColruCapcity = 150;
// const size_t StrColLruCapcity = 150;

const size_t IndexStorageBufferSize = BlockBufferSize;
const size_t IndexFileMinReserve = 16;

#if CONCENTRATED_MODE == 1
const int DatabasePartNum = vinMaxSize;
const size_t IndexFileIncSpace = 2ULL * 1024 * 1024;
#else
const int DatabasePartNum = 500;
const size_t IndexFileIncSpace = 4ULL * 1024 * 1024;
#endif

/*
坑点:
0 列式db后 latest换成下标索引，如果在排序或者扩容 取回的值会有问题
这时候如果读不加锁会有BUG... 同样 如果3600是未知的 就不能用vector...
否则扩容的时候会有问题 所以 如果不知道index中的serial最大值预先扩容
取latest的时候 以当前写法 一定要加锁。 否则就是要单独弄一个结构去存。
或许CAS速度还不如加锁每次更新?...

1 block中读没加锁 但正常要加 否则扩容的时候会有问题
2 Ctimestamp 是压缩后的 做过size不大的假设，如果数据太大 会coredump
要调回u64 3 用u64做了reqlCol的过滤 如果超过64列就会有问题 4 优化点 1
range查询排序优化 2 latest缓存优化&写入 3 mmap优化 5 queryRange Index文件下
vector如果无法确定是初始capcity 那么要锁定，因为有可能扩容 6
尤其要注意双重锁定问题 比如 A:检查-写 B:检查-写 A在检查完毕后释放锁切到B,
B也检查通过, 就造成A-B重复写的问题

7. 写入格式 在TSDBImpl 的upsert里.
现在新增了一个vinid.  那么在记录索引的时候注意跳过这个偏移
8. 读出格式 在TSDBImpl 的rowFromBytes里 (记录索引的跳过 二选一调整即可)
以及 rebuildIndex里解析时. rebuild里重建索引的偏移
9. 还要维护 schema文件, vin-id的映射文件.

10. block里 read是不加锁的

11. eMap->query(vin);改成了无锁(因为默认此时都已经插入)

ipcs -m | awk '$2 ~ /[0-9]+/ {print $2}' | while read s; do sudo ipcrm -m
$s; done

ipcs -m


1 没return 0
2 内存越界
3 内存申请超限

== LRU 缓存Block
== 异步写
== 统计不同col个数
*/
