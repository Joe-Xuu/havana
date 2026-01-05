#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>

class Dictionary {
private:
    // 两个映射：双向查找
    std::unordered_map<std::string, int> str_to_id;
    std::vector<std::string> id_to_str;
    
    // 既然是数据库，以后肯定有多线程，先预留个锁
    std::mutex mtx; 

public:
    // 核心逻辑：给字符串分配一个 ID。如果已存在，返回旧 ID。
    int getId(const std::string& val) {
        std::lock_guard<std::mutex> lock(mtx);
        
        if (str_to_id.find(val) == str_to_id.end()) {
            int new_id = id_to_str.size();
            id_to_str.push_back(val);
            str_to_id[val] = new_id;
            return new_id;
        }
        return str_to_id[val];
    }

    // 反查：把 ID 变回字符串 (用于查询结果显示)
    std::string getVal(int id) {
        std::lock_guard<std::mutex> lock(mtx);
        if (id < 0 || id >= id_to_str.size()) {
            return ""; // 或者抛异常
        }
        return id_to_str[id];
    }
};