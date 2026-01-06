#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include <atomic>
#include <iomanip> // 用于格式化输出
#include "Table.h"

// --- 辅助工具：简单的计时器 ---
class Timer {
    std::chrono::high_resolution_clock::time_point start;
public:
    Timer() { reset(); }
    void reset() { start = std::chrono::high_resolution_clock::now(); }
    double elapsed_ms() {
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    }
};

// --- 工作线程：负责写入数据 ---
// 模式：写入 Key, Price(Last), Qty(Sum)
void worker(Table* table, int start_id, int count) {
    // 为了极致性能，避免在循环里申请内存，我们预分配 Row 对象
    // 这里的 Key 用 String (模拟真实场景), Price 和 Qty 用 Int
    std::vector<Table::Value> row;
    row.push_back(std::string("")); // Key
    row.push_back(0);               // Price (Last)
    row.push_back(0);               // Qty (Sum)

    for (int i = 0; i < count; ++i) {
        int id = start_id + i;
        
        // 构造数据:
        // Key: "Prod_<id>"
        // Price: id (不断变大，测试 AGG_LAST 是否取最大)
        // Qty: 1 (每次加1，测试 AGG_SUM 是否累加)
        
        // 注意：这里为了测 Engine 性能，尽量减少 string 拼接的开销
        // 但为了真实性，我们还是拼一下，如果想测极限 TPS 可以改成纯 INT Key
        row[0] = "Prod_" + std::to_string(id); 
        row[1] = id; 
        row[2] = 1; 

        table->insertRow(row);
    }
}

// --- 测试 1: 逻辑正确性验证 ---
void test_correctness() {
    std::cout << "\n[1. Logic Correctness Test] Checking Hybrid Schema..." << std::endl;
    
    Table t("VerifyTable");
    t.createColumn("Product", TYPE_STRING, AGG_LAST, true); // 主键
    t.createColumn("Price",   TYPE_INT,    AGG_LAST, true); // 覆盖
    t.createColumn("Stock",   TYPE_INT,    AGG_SUM, true);  // 累加

    // 1. 初始: 价格 100, 库存 10
    t.insertRow({std::string("Tires"), 100, 10});
    
    // 2. 涨价: 价格 150, 库存 +5
    // 注意：这里我们模拟并发，稍微 sleep 一下保证时间戳不同（其实不需要，代码里 ++global_ts 很快）
    t.insertRow({std::string("Tires"), 150, 5});

    // 3. 降价但扣库存: 价格 120, 库存 -2
    t.insertRow({std::string("Tires"), 120, -2});

    // 查一下
    auto res = t.querySnapshot("Product", "Tires");

    std::cout << "  Query Result for 'Tires':" << std::endl;
    std::cout << "  Price (Expected 120): " << res["Price"] << std::endl;
    std::cout << "  Stock (Expected 13):  " << res["Stock"] << std::endl; // 10 + 5 - 2 = 13

    if (res["Price"] == "120" && res["Stock"] == "13") {
        std::cout << "  >>> PASS: Logic is correct." << std::endl;
    } else {
        std::cout << "  >>> FAIL: Logic error!" << std::endl;
    }
}

// --- 测试 2: 综合性能测试 ---
void run_benchmark(const std::string& label, int total_rows, int thread_count) {
    Table t("BenchTable");
    // 定义混合 Schema
    t.createColumn("Key",   TYPE_STRING, AGG_LAST, true);
    t.createColumn("Price", TYPE_INT,    AGG_LAST, true);
    t.createColumn("Qty",   TYPE_INT,    AGG_SUM, true);

    std::cout << "\n[" << label << "] Rows: " << total_rows << ", Threads: " << thread_count << std::endl;
    
    Timer timer;
    std::vector<std::thread> threads;
    int rows_per_thread = total_rows / thread_count;

    // 1. 并发写入 (Write Phase)
    for (int i = 0; i < thread_count; ++i) {
        threads.emplace_back(worker, &t, i * rows_per_thread, rows_per_thread);
    }
    for (auto& th : threads) th.join();

    double write_ms = timer.elapsed_ms();
    long write_tps = (long)((double)total_rows / write_ms * 1000);

    std::cout << "  Write Time: " << write_ms << " ms | TPS: " << write_tps << std::endl;

    // 2. 简单读取测试 (Read Phase)
    // 随机查一个 key，看 querySnapshot 耗时
    // 注意：querySnapshot 是全表扫描，随着数据量增加，这里会显著变慢（这是未加索引的正常现象）
    timer.reset();
    std::string search_key = "Prod_" + std::to_string(total_rows / 2); // 找中间的一个
    auto res = t.querySnapshot("Key", search_key);
    double read_ms = timer.elapsed_ms();

    std::cout << "  Read Time (Full Scan): " << read_ms << " ms" << std::endl;
    if (total_rows > 100000) {
        std::cout << "  (Hashing is fast)" << std::endl;
    }
}

int main() {
    std::cout << "=== HavanaDB Comprehensive Benchmark ===" << std::endl;
    std::cout << "Arch: Column-Store + Insert-Only + Chunking + Hybrid Aggregation" << std::endl;

    // 1. 验证逻辑
    test_correctness();

    // 2. 性能阶梯测试
    // A. 小数据量 (Warm up)
    run_benchmark("2. Small (Warmup)", 100000, 4);

    // B. 中数据量 (Chunking Test) - 100万行
    // 这里应该触发约 10 次扩容
    run_benchmark("3. Medium (1M)", 1000000, 4);

    // C. 大数据量 (Stress Test) - 500万行 (为了节省时间不跑1000万，你可以自己改)
    // 这里应该触发约 50 次扩容
    // M1 可能会遇到字符串分配的瓶颈，观察 TPS 是否稳定
    run_benchmark("4. Large (5M)", 5000000, 4);

    return 0;
}