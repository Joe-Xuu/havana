#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include "Table.h"

// 纯 INT 测试，避免 string 干扰
void worker(Table* table, int start_val, int count) {
    std::vector<Table::Value> row;
    row.push_back(0); // ID
    row.push_back(0); // Val1
    row.push_back(0); // Val2

    for (int i = 0; i < count; ++i) {
        row[0] = start_val + i;
        row[1] = i;
        row[2] = i * 2;
        table->insertRow(row);
    }
}

void run_test(const std::string& name, int total_rows) {
    Table table("TestTable");
    table.createColumn("ID", TYPE_INT, AGG_LAST);
    table.createColumn("Val1", TYPE_INT, AGG_SUM);
    table.createColumn("Val2", TYPE_INT, AGG_SUM);

    int thread_count = 4;
    int rows_per_thread = total_rows / thread_count;

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    for (int i = 0; i < thread_count; ++i) {
        threads.emplace_back(worker, &table, i * rows_per_thread, rows_per_thread);
    }

    for (auto& t : threads) t.join();

    auto end = std::chrono::high_resolution_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    std::cout << "[" << name << "] Rows: " << total_rows 
              << " | Time: " << ms << " ms"
              << " | TPS: " << (long)((double)total_rows / ms * 1000) << std::endl;
}

int main() {
    std::cout << "--- Chunking Architecture Benchmark ---" << std::endl;
    std::cout << "Chunk Size: " << CHUNK_SIZE << " rows" << std::endl;

    // 1. 小数据量：只分配 1 个块
    run_test("Small ", 50000); 

    // 2. 中数据量：跨越 10 个块
    run_test("Medium", 1000000); 

    // 3. 大数据量：跨越 100 个块
    // 如果 Chunking 逻辑有问题，这里可能会崩或者变得极慢
    run_test("Large ", 10000000); 

    return 0;
}