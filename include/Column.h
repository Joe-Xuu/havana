#pragma once
#include <vector>
#include <string>
#include <stdexcept>
#include <iostream>

// 1. 定义一个抽象基类 (接口)
class AbstractColumn {
public:
    virtual ~AbstractColumn() = default;
    
    // 纯虚函数：子类必须实现
    virtual size_t size() const = 0;
    virtual void printValue(size_t row_idx) const = 0; // 用于 debug 打印
    // 注意：这里没法定义 get()，因为不知道返回 int 还是 string
};

// 2. 具体的列实现 (继承自 AbstractColumn)
template <typename T>
class Column : public AbstractColumn {
private:
    std::vector<T> data;

public:
    void append(T val) {
        data.push_back(val);
    }

    T get(size_t row_index) const {
        if (row_index >= data.size()) throw std::out_of_range("Out of range");
        return data[row_index];
    }

    size_t size() const override {
        return data.size();
    }

    // 实现父类的打印接口
    void printValue(size_t row_idx) const override {
        std::cout << data[row_idx];
    }
};