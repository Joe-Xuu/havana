#pragma once
#include <fstream>
#include <string>
#include <mutex>
#include <vector>

class Logger {
private:
    std::ofstream log_file;
    std::mutex log_mutex;

public:
    Logger(const std::string& filename) {
        // 打开文件，追加模式 (std::ios::app)，二进制模式
        log_file.open(filename, std::ios::out | std::ios::app | std::ios::binary);
    }

    ~Logger() {
        if (log_file.is_open()) log_file.close();
    }

    // 记录一条日志: <OpType> <Col1> <Col2> ...
    // 简单起见，我们直接存文本，或者简单的二进制格式
    void logInsert(const std::vector<std::string>& values) {
        // 加锁，保证日志顺序不乱 (瓶颈预警!)
        std::lock_guard<std::mutex> lock(log_mutex);
        
        // 格式: "INS val1,val2,val3\n"
        log_file << "INS";
        for (const auto& v : values) {
            log_file << " " << v;
        }
        log_file << "\n";
        
        // 关键点：如果不 flush，数据还在 OS 缓存里，断电会丢
        // 如果 flush，性能会掉。我们先不 flush，模拟异步日志。
        log_file.flush(); 
    }
};