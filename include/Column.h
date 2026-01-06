#pragma once
#include <vector>
#include <string>
#include <stdexcept>
#include <iostream>
#include <type_traits>
#include <atomic>
#include <mutex>

// 定义分块大小：每块 10 万行
constexpr size_t CHUNK_SIZE = 100000;
// 定义最大块数：4096 块 -> 总容量约 4 亿行 (足够了)
constexpr size_t MAX_CHUNKS = 4096;

class AbstractColumn {
public:
    virtual ~AbstractColumn() = default;
    virtual void printValue(size_t row_idx) const = 0;

    // --- 新接口：按需扩容 ---
    // 告诉列："我要写第 row_idx 行，你看看内存够不够，不够就申请"
    virtual void ensureChunk(size_t chunk_idx) = 0;

    // 随机写 (逻辑不变)
    virtual void set(size_t row_idx, int val) { throw std::runtime_error("Type Err"); }
    virtual void set(size_t row_idx, const std::string& val) { throw std::runtime_error("Type Err"); }
};

template <typename T>
class Column : public AbstractColumn {
private:
    // 二级指针数组：chunks[i] 指向第 i 个数据块
    // 使用 atomic 指针，方便无锁检查
    std::atomic<std::vector<T>*> chunks[MAX_CHUNKS];
    
    // 这是一个很小的锁，只在申请新块的那一瞬间（每10万行一次）使用
    // 相比每行都锁，这个开销可以忽略不计
    std::mutex alloc_mutex;

public:
    Column() {
        // 初始化所有指针为空
        for (auto& ptr : chunks) ptr.store(nullptr);
    }

    ~Column() {
        // 析构时释放所有申请的块
        for (auto& ptr : chunks) {
            auto* p = ptr.load();
            if (p) delete p;
        }
    }

    // --- 核心：按需分配 ---
    void ensureChunk(size_t chunk_idx) override {
        if (chunk_idx >= MAX_CHUNKS) throw std::out_of_range("Exceeded DB Max Capacity");

        // 1. 快速检查 (Double-Checked Locking 的第一步)
        if (chunks[chunk_idx].load(std::memory_order_acquire) != nullptr) return;

        // 2. 加锁分配
        std::lock_guard<std::mutex> lock(alloc_mutex);
        
        // 3. 再次检查 (防止别人刚才分配了)
        if (chunks[chunk_idx].load(std::memory_order_relaxed) == nullptr) {
            auto* new_chunk = new std::vector<T>(CHUNK_SIZE);
            // 这里可以做一些默认值初始化，比如 int=0, string=""
            // 存回去
            chunks[chunk_idx].store(new_chunk, std::memory_order_release);
            
            // Debug: 打印一下，看看是不是真的只分配了几次
            // std::cout << "Allocated Chunk " << chunk_idx << " for type " << typeid(T).name() << std::endl;
        }
    }

    void set(size_t row_idx, int val) override {
        if constexpr (std::is_same_v<T, int>) {
            // 计算位置
            size_t c_idx = row_idx / CHUNK_SIZE;
            size_t offset = row_idx % CHUNK_SIZE;
            
            // 获取块指针 (这里假设 ensureChunk 已经被 Table 调过了，或者在这里调也可以)
            // 为了性能，我们假设 Table 会负责先调 ensureChunk
            auto* chunk = chunks[c_idx].load(std::memory_order_relaxed);
            (*chunk)[offset] = val;
        } else {
            AbstractColumn::set(row_idx, val);
        }
    }

    void set(size_t row_idx, const std::string& val) override {
        if constexpr (std::is_same_v<T, std::string>) {
            size_t c_idx = row_idx / CHUNK_SIZE;
            size_t offset = row_idx % CHUNK_SIZE;
            auto* chunk = chunks[c_idx].load(std::memory_order_relaxed);
            (*chunk)[offset] = val;
        } else {
            AbstractColumn::set(row_idx, val);
        }
    }

    // 读取 (Getter)
    T get(size_t row_idx) const {
        size_t c_idx = row_idx / CHUNK_SIZE;
        size_t offset = row_idx % CHUNK_SIZE;
        auto* chunk = chunks[c_idx].load(std::memory_order_relaxed);
        // 如果读到了还没分配的块，说明逻辑错了或者越界
        if (!chunk) return T{}; 
        return (*chunk)[offset];
    }

    void printValue(size_t row_idx) const override {
        std::cout << get(row_idx);
    }
};