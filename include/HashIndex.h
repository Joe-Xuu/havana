#pragma once
#include <vector>
#include <string>
#include <unordered_map>
#include <atomic> // 必须引入
#include <thread>

constexpr size_t INDEX_SHARDS = 1024;

class HashIndex {
private:
    struct Shard {
        // 替换 mutex 为 atomic_flag (轻量级自旋锁)
        std::atomic_flag lock = ATOMIC_FLAG_INIT;
        std::unordered_map<std::string, std::vector<size_t>> map;
    };

    std::vector<Shard> shards;

public:
    HashIndex() : shards(INDEX_SHARDS) {}

    void insert(const std::string& key, size_t row_id) {
        size_t hash_val = std::hash<std::string>{}(key);
        size_t shard_idx = hash_val % INDEX_SHARDS;

        Shard& shard = shards[shard_idx];
        
        // --- 自旋锁逻辑 Start ---
        // test_and_set 返回 true 表示锁被占用了，那就一直 while 循环等待
        while (shard.lock.test_and_set(std::memory_order_acquire)) {
            // 提示 CPU 我在忙等，稍微休息下避免发热，但别睡觉
            std::this_thread::yield(); 
        }
        // --- 临界区 ---

        shard.map[key].push_back(row_id);

        // --- 解锁 ---
        shard.lock.clear(std::memory_order_release);
    }

    std::vector<size_t> get(const std::string& key) {
        size_t hash_val = std::hash<std::string>{}(key);
        size_t shard_idx = hash_val % INDEX_SHARDS;

        Shard& shard = shards[shard_idx];
        
        // 读的时候也要加锁
        while (shard.lock.test_and_set(std::memory_order_acquire)) {
            std::this_thread::yield();
        }

        std::vector<size_t> result;
        auto it = shard.map.find(key);
        if (it != shard.map.end()) {
            result = it->second;
        }

        shard.lock.clear(std::memory_order_release);
        return result;
    }
};