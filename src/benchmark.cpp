#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include <atomic>
#include <iomanip>
#include "Table.h"

// 简易计时器
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

// 工作线程：写入 Prod_X, id, 1
void worker(Table* table, int start_id, int count) {
    std::vector<Table::Value> row;
    row.push_back(std::string("")); 
    row.push_back(0); 
    row.push_back(0); 

    for (int i = 0; i < count; ++i) {
        int id = start_id + i;
        row[0] = "Prod_" + std::to_string(id); 
        row[1] = id; 
        row[2] = 1; 
        table->insertRow(row);
    }
}

// 1. 逻辑正确性验证 (MVCC + Delta)
void test_correctness() {
    std::cout << "\n[1. Logic Correctness Test] Checking Hybrid Schema..." << std::endl;
    
    // truncate=true 表示每次跑都清空日志
    Table t("VerifyTable", true);
    t.createColumn("Product", TYPE_STRING, AGG_LAST, true); // 索引
    t.createColumn("Price",   TYPE_INT,    AGG_LAST);
    t.createColumn("Stock",   TYPE_INT,    AGG_SUM);

    t.insertRow({std::string("Tires"), 100, 10});  // 初始
    t.insertRow({std::string("Tires"), 150, 5});   // 涨价，进货
    t.insertRow({std::string("Tires"), 120, -2});  // 降价，出货

    auto res = t.querySnapshot("Product", "Tires");

    std::cout << "  Query Result for 'Tires':" << std::endl;
    std::cout << "  Price (Expected 120): " << res["Price"] << std::endl;
    std::cout << "  Stock (Expected 13):  " << res["Stock"] << std::endl;

    if (res["Price"] == "120" && res["Stock"] == "13") {
        std::cout << "  >>> PASS: Logic is correct." << std::endl;
    } else {
        std::cout << "  >>> FAIL: Logic error!" << std::endl;
    }
}

// 2. 性能测试核心函数
void run_benchmark(const std::string& label, int total_rows, int thread_count) {
    // truncate=true 确保从零开始
    Table t("BenchTable", true);
    t.createColumn("Key",   TYPE_STRING, AGG_LAST, true); // 开启索引
    t.createColumn("Price", TYPE_INT,    AGG_LAST);
    t.createColumn("Qty",   TYPE_INT,    AGG_SUM);

    std::cout << "\n[" << label << "] Rows: " << total_rows << ", Threads: " << thread_count << std::endl;
    
    Timer timer;
    std::vector<std::thread> threads;
    int rows_per_thread = total_rows / thread_count;

    // 写阶段
    for (int i = 0; i < thread_count; ++i) {
        threads.emplace_back(worker, &t, i * rows_per_thread, rows_per_thread);
    }
    for (auto& th : threads) th.join();

    double write_ms = timer.elapsed_ms();
    long write_tps = (long)((double)total_rows / write_ms * 1000);
    std::cout << "  Write Time: " << write_ms << " ms | TPS: " << write_tps << std::endl;

    // 读阶段 (走索引)
    timer.reset();
    std::string search_key = "Prod_" + std::to_string(total_rows / 2);
    auto res = t.querySnapshot("Key", search_key);
    double read_ms = timer.elapsed_ms();

    std::cout << "  Read Time (Index Lookup): " << read_ms << " ms" << std::endl;
}

// 3. 崩溃恢复测试
void test_recovery() {
    std::cout << "\n[5. Recovery Test] Writing, Simulating Crash, Reloading..." << std::endl;
    
    std::string table_name = "RecoverDB";
    int rows_to_write = 50000;

    // --- Phase 1: 写入并销毁 ---
    {
        std::cout << "  Phase 1: Writing " << rows_to_write << " rows..." << std::endl;
        Table t(table_name, true); // 新建表
        t.createColumn("Key", TYPE_STRING, AGG_LAST, true);
        t.createColumn("Val", TYPE_INT, AGG_SUM);

        for(int i=0; i<rows_to_write; ++i) {
            t.insertRow({std::string("Key_") + std::to_string(i), 1});
        }
        std::cout << "  Phase 1 Done. Table destructed (Log Flushed)." << std::endl;
    } // t 在这里析构

    // --- Phase 2: 重启并恢复 ---
    {
        std::cout << "  Phase 2: Restarting..." << std::endl;
        Table t(table_name, false); // false = 保留旧日志用于恢复
        
        // 必须重新定义 Schema
        t.createColumn("Key", TYPE_STRING, AGG_LAST, true);
        t.createColumn("Val", TYPE_INT, AGG_SUM);

        Timer t_rec;
        t.recover(); // 执行恢复
        std::cout << "  Recovery took " << t_rec.elapsed_ms() << " ms." << std::endl;

        // 验证
        auto res = t.querySnapshot("Key", "Key_100");
        if (res["Val"] == "1") {
            std::cout << "  >>> PASS: Data recovered successfully!" << std::endl;
        } else {
            std::cout << "  >>> FAIL: Data lost! Got " << res["Val"] << std::endl;
        }
    }
}

int main() {
    std::cout << "=== HavanaDB Comprehensive Benchmark ===" << std::endl;
    std::cout << "Feature Set: [Column-Store] [Insert-Only] [Chunking] [Hash-Index] [Binary-WAL]" << std::endl;

    test_correctness();

    // 性能阶梯测试
    run_benchmark("2. Small (Warmup)", 100000, 4);
    run_benchmark("3. Medium (1M)", 1000000, 4);
    
    // 大数据测试 (建议用 500万，跑 1000万 可能日志文件会比较大)
    run_benchmark("4. Large (5M)", 5000000, 4);

    test_recovery();

    return 0;
}