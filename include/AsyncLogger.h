#pragma once
#include <vector>
#include <string>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <fstream>
#include <atomic>
#include <chrono>

class AsyncLogger {
private:
    std::ofstream log_file;
    std::atomic<bool> running{true};
    std::thread background_thread;

    // 缓冲区：存的是格式化好的字符串 (或者二进制流)
    // 使用 string 虽然不是极致性能，但实现最简单
    std::vector<std::string> buffer; 
    std::mutex buffer_mutex;
    std::condition_variable cv;

    // 参数：每隔多少毫秒刷一次盘
    const int FLUSH_INTERVAL_MS = 10; 

public:
    AsyncLogger(const std::string& filename) {
        log_file.open(filename, std::ios::out | std::ios::app | std::ios::binary);
        // 预分配 buffer 减少扩容开销
        buffer.reserve(10000); 
        
        // 启动后台刷盘线程
        background_thread = std::thread(&AsyncLogger::worker_loop, this);
    }

    ~AsyncLogger() {
        running = false;
        cv.notify_all(); // 唤醒后台线程让它下班
        if (background_thread.joinable()) background_thread.join();
        if (log_file.is_open()) log_file.close();
    }

    // 前台线程调用：极速写入
    void append(const std::vector<std::string>& row_strs) {
        // 简单的格式化：逗号分隔
        std::string line = "INS";
        for (const auto& s : row_strs) {
            line += "," + s;
        }
        line += "\n";

        {
            std::lock_guard<std::mutex> lock(buffer_mutex);
            buffer.push_back(std::move(line));
        }
        // 这里不 notify，除非 buffer 太大了。让后台线程自己定时醒来。
        // 这样可以避免频繁的上下文切换。
    }

private:
    // 后台线程的工作循环
    void worker_loop() {
        std::vector<std::string> swap_buffer;
        swap_buffer.reserve(10000);

        while (running) {
            {
                // 等待 10ms，或者被析构函数唤醒
                std::unique_lock<std::mutex> lock(buffer_mutex);
                cv.wait_for(lock, std::chrono::milliseconds(FLUSH_INTERVAL_MS), [this] { return !running; });
                
                // --- 关键时刻：交换缓冲区 (Swap) ---
                // 把 accumulated buffer 换给 swap_buffer，原来的清空给前台继续用
                // 持锁时间极短，只有几个指针交换操作
                buffer.swap(swap_buffer);
            }

            // --- 此时已经无锁了，慢慢写盘 ---
            if (!swap_buffer.empty()) {
                for (const auto& line : swap_buffer) {
                    log_file << line;
                }
                // Group Commit 的核心：一批数据只 Flush 一次！
                log_file.flush(); 
                swap_buffer.clear();
            }
        }
    }
};