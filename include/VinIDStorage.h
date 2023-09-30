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
#include "Root.h"
#include "TSDBEngine.hpp"
#include "TSDBTypes.h"
#include "struct/Row.h"
#include "struct/Vin.h"

namespace LindormContest {

class VinIDStorage {
   private:
    std::string directory;
    const std::string filename = "VinID";
    int fd;

   public:
    VinIDStorage(const std::string& dir) { directory = dir; }
    ~VinIDStorage() {
        if (fd >= 0)
            vclose();
    }

   public:
    int vopen() {
        std::string path = directory + "/" + filename;
        std::cout << path << std::endl;
        fd = open(path.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

        if (fd < 0) {
            bool ret = std::filesystem::create_directories(directory);
            if (ret) {
                fd = open(path.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
            }
            if (!ret || fd < 0) {
                perror("VinID storage open file fail");
            }
        }
        return 0;
    }

    int vclose() {
        close(fd);
        return 0;
    }

    int writeNum(uint32_t n) {
        assert(fd >= 0);
        int ret = pwrite(fd, &n, sizeof(uint32_t), 0);
        return ret;
    }
    int readNum() {
        assert(fd >= 0);
        uint32_t n;
        int ret = pread(fd, &n, sizeof(uint32_t), 0);
        return n;
    }
    int recordVinIDMap(uint32_t n, VinID vinID, LindormContest::Vin& vin) {
        assert(fd >= 0);
        size_t offset =
            sizeof(uint32_t) + n * (LindormContest::VIN_LENGTH + sizeof(vinID));
        int r = pwrite(fd, &vinID, sizeof(VinID), offset);
        assert(r != -1);
        offset += sizeof(VinID);
        r = pwrite(fd, &vin.vin, LindormContest::VIN_LENGTH, offset);
        assert(r != -1);
        offset += LindormContest::VIN_LENGTH;
        return 0;
    }

    int readVinIDMap(uint32_t n, VinID& vinID, Vin& vin) {
        assert(fd >= 0);
        size_t offset =
            sizeof(uint32_t) + n * (LindormContest::VIN_LENGTH + sizeof(vinID));
        int r = pread(fd, &vinID, sizeof(VinID), offset);
        assert(r != -1);
        offset += sizeof(VinID);
        r = pread(fd, &vin.vin, LindormContest::VIN_LENGTH, offset);
        assert(r != -1);
        offset += LindormContest::VIN_LENGTH;
        return 0;
    }
};

};  // namespace LindormContest