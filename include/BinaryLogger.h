#pragma once
#include <vector>
#include <string>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <fstream>
#include <atomic>
#include <chrono>
#include <variant>
#include <cstring> // for memcpy
#include <iostream>

class BinaryLogger {
private:
    std::fstream log_file; // fstream 既能读也能写
    std::atomic<bool> running{true};
    std::thread background_thread;

    // 缓冲区：这次存的是原始字节 (char)
    std::vector<char> buffer; 
    std::mutex buffer_mutex;
    std::condition_variable cv;

    const int FLUSH_INTERVAL_MS = 10; 

public:
    // truncate = true 表示清空旧日志（新表）
    // truncate = false 表示保留旧日志（用于恢复）
    BinaryLogger(const std::string& filename, bool truncate = true) {
        auto mode = std::ios::out | std::ios::binary;
        if (truncate) {
            mode |= std::ios::trunc; // 清空文件
        } else {
            mode |= std::ios::app;   // 追加模式
        }
        
        log_file.open(filename, mode);
        
        buffer.reserve(65536); // 64KB Buffer
        background_thread = std::thread(&BinaryLogger::worker_loop, this);
    }

    ~BinaryLogger() {
        running = false;
        cv.notify_all();
        if (background_thread.joinable()) background_thread.join();
        if (log_file.is_open()) log_file.close();
    }

    // --- 极速写入 (Binary Append) ---
    // 这里的 row_data 包含 int 或 string
    void appendEntry(const std::vector<std::variant<int, std::string>>& row) {
        std::lock_guard<std::mutex> lock(buffer_mutex);
        
        // 简单协议：直接按列顺序写入数据
        for (const auto& val : row) {
            if (std::holds_alternative<int>(val)) {
                // 写入 Int (4 bytes)
                int v = std::get<int>(val);
                const char* ptr = reinterpret_cast<const char*>(&v);
                buffer.insert(buffer.end(), ptr, ptr + sizeof(int));
            } else {
                // 写入 String: [Length 4bytes] + [Body]
                const std::string& s = std::get<std::string>(val);
                int len = static_cast<int>(s.size());
                const char* len_ptr = reinterpret_cast<const char*>(&len);
                
                buffer.insert(buffer.end(), len_ptr, len_ptr + sizeof(int));
                buffer.insert(buffer.end(), s.begin(), s.end());
            }
        }
        // 不需要换行符，因为是二进制流
    }

    // --- 恢复功能：读取整个日志 ---
    // 需要传入文件名，因为 fstream 在构造函数里是以 write mode 打开的
    static std::vector<std::vector<std::variant<int, std::string>>> readLog(
        const std::string& filename, 
        const std::vector<int>& col_types // 需要 Schema 才知道怎么读 (0:INT, 1:STRING)
    ) {
        std::vector<std::vector<std::variant<int, std::string>>> rows;
        std::ifstream infile(filename, std::ios::binary);
        if (!infile.is_open()) return rows;

        while (infile.peek() != EOF) {
            std::vector<std::variant<int, std::string>> row;
            for (int type : col_types) {
                if (type == 0) { // TYPE_INT
                    int val;
                    if (!infile.read(reinterpret_cast<char*>(&val), sizeof(int))) break;
                    row.push_back(val);
                } else { // TYPE_STRING
                    int len;
                    if (!infile.read(reinterpret_cast<char*>(&len), sizeof(int))) break;
                    std::string s(len, ' '); // 预分配
                    if (!infile.read(&s[0], len)) break;
                    row.push_back(s);
                }
            }
            if (row.size() == col_types.size()) {
                rows.push_back(std::move(row));
            }
        }
        return rows;
    }

private:
    void worker_loop() {
        std::vector<char> swap_buffer;
        swap_buffer.reserve(65536);

        while (running) {
            {
                std::unique_lock<std::mutex> lock(buffer_mutex);
                cv.wait_for(lock, std::chrono::milliseconds(FLUSH_INTERVAL_MS), [this] { return !running; });
                buffer.swap(swap_buffer);
            }

            if (!swap_buffer.empty()) {
                log_file.write(swap_buffer.data(), swap_buffer.size());
                log_file.flush(); // 刷盘！
                swap_buffer.clear();
            }
        }
    }
};