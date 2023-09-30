#pragma once

#include <iostream>
#include <list>
#include <mutex>
#include <unordered_map>
#include "assert.h"
#include "config.h"

using namespace std;

struct ListNode {
    uint32_t key;
    char* value;
    ListNode* prev;
    ListNode* next;
    // bool dirty;
    uint32_t size;
    ListNode(int k = 0, uint32_t _size = 0, char* v = nullptr)
        : key(k), value(v), prev(nullptr), next(nullptr), size(_size) {}
};

template <typename T>
class LRUCache {
   private:
    int capacity;                  // cap size
    int nodeSize;                  // node size
    unordered_map<int, T*> cache;  //
    T* head = nullptr;             // dummy head
    T* tail = nullptr;             // dummy tail
    list<char*> freeCache;
    list<char*>* ptrFree = nullptr;

    char* pageCache = nullptr;

   public:
    std::mutex lock;

    int getSize() { return nodeSize; }
    LRUCache(int c) {
        capacity = c;
        nodeSize = 0;
        head = new T();
        tail = new T();
        head->next = tail;
        tail->prev = head;

        if (!EN_LRU)
            return;

        // alloc cache memory
        pageCache = new char[capacity * BlockBufferSize];
        assert(pageCache != nullptr);

        // initialize free page cache
        char* addr = pageCache;
        for (int i = 0; i < capacity; ++i) {
            freeCache.push_back(addr);
            addr += BlockBufferSize;
        }

        ptrFree = &freeCache;
    }

    ~LRUCache() {
        if (pageCache)
            delete[] pageCache;

        while (nodeSize) {
            auto e = removeTail();
            cache.erase(e->key);
            delete e;
            nodeSize--;
        }
        if (head)
            delete head;
        if (tail)
            delete tail;
    }

    void clear() {
        if (nodeSize >= capacity) {
            auto e = removeTail();
            cache.erase(e->key);
            ptrFree->push_back(e->value);
            delete e;
            nodeSize--;
        }
    }
    T* get(uint32_t key) {
        if (!EN_LRU)
            return nullptr;
        // lock.lock();
        if (cache.count(key) == 0) {
            // lock.unlock();
            return nullptr;
        } else {
            T* node = nullptr;
            node = cache[key];
            moveToHead(node);
            // lock.unlock();
            return node;
        }
        // return node->value + page_offset * PAGE_SIZE_BYTE;
    }

    void put(uint32_t key, char* buf, size_t cacheSize) {
        if (!EN_LRU)
            return;
        lock.lock();
        if (cache.count(key) == 0) {
            if (ptrFree->size() == 0) {
                clear();
            }
            assert(ptrFree->size() > 0);

            char* value = ptrFree->front();
            ptrFree->pop_front();
            memcpy(value, buf, cacheSize);

            T* node = new T(key, cacheSize, value);
            // node->dirty = true;
            cache[key] = node;
            addToHead(node);
            nodeSize++;
            // maintain();

        } else {
            T* node = cache[key];
            // node->dirty = true;
            memcpy(node->value, buf, cacheSize);
            moveToHead(node);
        }

        lock.unlock();
        return;
    }

   private:
    void addToHead(T* node) {
        node->prev = head;
        node->next = head->next;
        head->next->prev = node;
        head->next = node;
    }

    void removeNode(T* node) {
        node->prev->next = node->next;
        node->next->prev = node->prev;
    }

    void moveToHead(T* node) {
        removeNode(node);
        addToHead(node);
    }

    T* removeTail() {
        T* node = tail->prev;
        removeNode(node);
        return node;
    }
};
