#pragma once
#include <vector>
#include <cstdint>
#include <limits>

const uint64_t INF_TS = std::numeric_limits<uint64_t>::max();

class MvccMeta {
private:
    std::atomic<std::vector<uint64_t>*> chunks_created[MAX_CHUNKS];
    std::atomic<std::vector<uint64_t>*> chunks_invalidated[MAX_CHUNKS]; // 仅用于 AGG_LAST 模式
    std::mutex alloc_mutex;

public:
    MvccMeta() {
        for (auto& p : chunks_created) p.store(nullptr);
        for (auto& p : chunks_invalidated) p.store(nullptr);
    }
    // 析构函数略 (记得 delete)...

    void ensureChunk(size_t chunk_idx) {
        if (chunks_created[chunk_idx].load(std::memory_order_acquire)) return;
        
        std::lock_guard<std::mutex> lock(alloc_mutex);
        if (!chunks_created[chunk_idx].load(std::memory_order_relaxed)) {
            // 初始化为 INF_TS
            auto* c1 = new std::vector<uint64_t>(CHUNK_SIZE, INF_TS);
            auto* c2 = new std::vector<uint64_t>(CHUNK_SIZE, INF_TS);
            chunks_created[chunk_idx].store(c1, std::memory_order_release);
            chunks_invalidated[chunk_idx].store(c2, std::memory_order_release);
        }
    }

    void setCreated(size_t row_idx, uint64_t ts) {
        size_t c_idx = row_idx / CHUNK_SIZE;
        size_t offset = row_idx % CHUNK_SIZE;
        (*chunks_created[c_idx].load(std::memory_order_relaxed))[offset] = ts;
    }

    bool isVisible(size_t row_idx, uint64_t query_ts) const {
        size_t c_idx = row_idx / CHUNK_SIZE;
        size_t offset = row_idx % CHUNK_SIZE;
        
        auto* c_ptr = chunks_created[c_idx].load(std::memory_order_relaxed);
        if (!c_ptr) return false; // 还没分配，肯定不可见
        
        uint64_t born = (*c_ptr)[offset];
        if (born == INF_TS || born > query_ts) return false;
        
        // 既然是 Hybrid 架构，我们主要看出生时间。
        // 如果要看死亡时间，逻辑同理
        return true; 
    }
    uint64_t getCreated(size_t row_idx) const {
        size_t c_idx = row_idx / CHUNK_SIZE;
        size_t offset = row_idx % CHUNK_SIZE;
        
        auto* chunk = chunks_created[c_idx].load(std::memory_order_relaxed);
        
        // 如果块还没分配，返回 INF_TS (表示没生出来)
        if (!chunk) return INF_TS; 
        
        return (*chunk)[offset];
    }
};