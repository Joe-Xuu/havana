#include <iostream>
#include <string>
#include <chrono> // C++ 的时间库，用来计时
#include "Table.h"

int main() {
    Table myTable;
    const int INSERT_COUNT = 1000000; // 一百万行！

    std::cout << "--- Benchmark Start ---" << std::endl;

    // 1. 准备数据
    // 我们模拟 3 种产品循环插入
    std::string products[] = {"Tires", "Frames", "Brakes"};

    // -------------------------------------------------
    // 测试 1: 写入性能 (Insert)
    // -------------------------------------------------
    auto start_time = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < INSERT_COUNT; ++i) {
        // 轮流插入 Tires, Frames, Brakes
        myTable.insert(products[i % 3], i); 
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    std::cout << "Inserted " << INSERT_COUNT << " rows in " 
              << duration.count() << " ms." << std::endl;
    // 计算每秒写入 (TPS)
    double seconds = duration.count() / 1000.0;
    std::cout << "Write Throughput: " << (INSERT_COUNT / seconds) << " rows/sec" << std::endl;


    // -------------------------------------------------
    // 测试 2: 扫描查询性能 (Scan)
    // -------------------------------------------------
    std::cout << "\nScanning for 'Tires'..." << std::endl;
    
    start_time = std::chrono::high_resolution_clock::now();

    // 执行查询
    std::vector<size_t> results = myTable.select("Tires");

    end_time = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "Found " << results.size() << " matching rows in " 
              << duration.count() << " ms." << std::endl;
              
    // 验证一下结果对不对 (随便抽查一个)
    if (!results.empty()) {
        size_t first_row = results[0];
        std::cout << "Verify first result Amount: " << myTable.getAmount(first_row) << std::endl;
    }

    // -------------------------------------------------
    // 测试 3: 复杂查询 (Tires AND Amount > 500)
    // -------------------------------------------------
    std::cout << "\nScanning for 'Tires' AND Amount > 500000..." << std::endl;
    // 注意：之前 benchmark 插入 amount 用的就是 i (0 到 999999)
    // 所以我们查 > 500000 应该能过滤掉一半的 Tires
    
    start_time = std::chrono::high_resolution_clock::now();

    std::vector<size_t> complex_results = myTable.select_complex("Tires", 500000);

    end_time = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "Found " << complex_results.size() << " complex matches in " 
              << duration.count() << " ms." << std::endl;

    return 0;
}