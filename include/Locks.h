#pragma once
#include <atomic>

class SpinLock {
   public:
    SpinLock() : flag_(false) {}

    void lock() {
        bool expect = false;
        while (!flag_.compare_exchange_weak(expect, true)) {
            // 这里一定要将expect复原，执行失败时expect结果是未定的

            expect = false;
        }
    }

    void unlock() { flag_.store(false); }

   private:
    std::atomic<bool> flag_;
};