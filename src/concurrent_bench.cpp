#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include "Table.h"

// void worker(Table* table, int id, int count) {
//     for (int i = 0; i < count; ++i) {
//         // 构造数据
//         std::vector<Table::Value> row;
//         row.push_back(id);
//         row.push_back(std::string("Thread_") + std::to_string(id));
//         row.push_back(i);
        
//         // 疯狂写入
//         table->insertRow(row);
//     }
// }

void worker(Table* table, int id, int count) {
    // 预先构造好一个 row 对象，避免重复申请 vector 内存
    // 这里我们只测 INT 列，彻底排除 string干扰
    std::vector<Table::Value> row;
    row.push_back(id);
    row.push_back(id); // 把 Name 列也临时改成 INT 或者只插空字符串
    row.push_back(0);

    for (int i = 0; i < count; ++i) {
        // 只修改数值，不重新分配内存
        row[2] = i; 
        
        // 疯狂写入
        // 注意：如果你的 Name 列是 String 类型，这里传 int 会报错
        // 建议临时把 Schema 改成 3 个 INT，或者插入一个固定的 string 常量
        table->insertRow(row); 
    }
}

int main() {
    // 1. 初始化表，预分配 1000 万行空间 (避免扩容)
    // 这里的 10000000 会占用几百兆虚拟内存
    Table myTable("NoLockTest", 10000000); 

    myTable.createColumn("ThreadID", TYPE_INT);
    myTable.createColumn("Name", TYPE_INT);
    myTable.createColumn("Seq", TYPE_INT);

    // 2. 配置压测参数
    // M1 性能很强，我们用 4 线程，每线程 100 万行，总量 400 万行
    int thread_count = 4;
    int rows_per_thread = 1000000; 
    
    std::cout << "--- Lock-Free Benchmark Start ---" << std::endl;
    std::cout << "Threads: " << thread_count << ", Rows/Thread: " << rows_per_thread << std::endl;
    std::cout << "Pre-allocated Capacity: 10,000,000 rows" << std::endl;

    auto start = std::chrono::high_resolution_clock::now();

    // 3. 启动线程
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_count; ++i) {
        threads.emplace_back(worker, &myTable, i, rows_per_thread);
    }

    for (auto& t : threads) {
        t.join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "All threads finished in " << duration.count() << " ms." << std::endl;
    
    // 4. 计算 TPS
    double total_rows = (double)thread_count * rows_per_thread;
    double seconds = duration.count() / 1000.0;
    std::cout << "Total Throughput: " << (total_rows / seconds) << " rows/sec" << std::endl;

    return 0;
}