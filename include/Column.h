#pragma once
#include <vector>
#include <stdexcept>

// 这是一个模板类，可以存 int, float, 或者我们字典编码后的 id (也是 int)
template <typename T>
class Column {
private:
    std::vector<T> data;

public:
    //追加数据 (HANA 的 Insert-Only 特性)
    void append(T val) {
        data.push_back(val);
    }

    // 读取数据 (通过行号)
    T get(size_t row_index) const {
        if (row_index >= data.size()) {
            throw std::out_of_range("Row index out of range");
        }
        return data[row_index];
    }

    // 获取当前有多少行
    size_t size() const {
        return data.size();
    }
};