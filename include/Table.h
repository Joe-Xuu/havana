#pragma once
#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>
#include <memory> // 智能指针
#include <variant> // C++17 神器，用来处理不同类型的输入
#include "Column.h"
#include "MvccMeta.h"

// 定义我们支持的数据类型
enum ColumnType { TYPE_INT, TYPE_STRING };

class Table {
private:
    std::string table_name;
    
    // 核心变化：用 Map 存列名 -> 列对象的映射
    // 使用 unique_ptr 自动管理内存，防止内存泄漏
    std::unordered_map<std::string, std::unique_ptr<AbstractColumn>> columns;
    
    // 还需要记录列的顺序和类型，方便插入
    struct ColMeta {
        std::string name;
        ColumnType type;
    };
    std::vector<ColMeta> schema;

    MvccMeta meta;
    std::atomic<uint64_t> global_ts{0};

public:
    Table(std::string name) : table_name(name) {}

    // --- 1. DDL: 建表/加列 ---
    // 比如: createColumn("Age", TYPE_INT)
    void createColumn(const std::string& name, ColumnType type) {
        schema.push_back({name, type});
        
        if (type == TYPE_INT) {
            columns[name] = std::make_unique<Column<int>>();
        } else if (type == TYPE_STRING) {
            // 注意：为了简化，这里暂时不用 Dictionary，直接存 string
            // 之后我们要把 Dictionary 集成进 Column<string> 里
            columns[name] = std::make_unique<Column<std::string>>();
        }
    }

    // --- 2. DML: 插入数据 ---
    // 难点：用户传进来的数据可能是 int 也可能是 string。
    // C++17 提供了 std::variant，像一个能变的盒子。
    using Value = std::variant<int, std::string>;

    void insertRow(const std::vector<Value>& row_data) {
        if (row_data.size() != schema.size()) {
            throw std::runtime_error("Column count mismatch");
        }

        uint64_t tx_id = ++global_ts;

        for (size_t i = 0; i < schema.size(); ++i) {
            const auto& col_name = schema[i].name;
            const auto& col_type = schema[i].type;
            const auto& val = row_data[i];

            // 这是一个比较丑陋但必要的 switch/if，用来把 variant 拆包
            if (col_type == TYPE_INT) {
                // 拿到 Column<int> 指针
                auto* col = dynamic_cast<Column<int>*>(columns[col_name].get());
                // 从 variant 里拿出 int
                col->append(std::get<int>(val)); 
            } else {
                auto* col = dynamic_cast<Column<std::string>*>(columns[col_name].get());
                col->append(std::get<std::string>(val));
            }
        }
        
        // 别忘了 MVCC
        meta.append(tx_id);
    }

    // --- 3. 简单的全表打印 ---
    void printAll() {
        size_t total_rows = meta.t_created.size();
        uint64_t now = global_ts.load();

        std::cout << "=== Table: " << table_name << " ===" << std::endl;
        
        // 打印表头
        for (const auto& s : schema) std::cout << s.name << "\t";
        std::cout << std::endl;

        for (size_t i = 0; i < total_rows; ++i) {
            if (meta.isVisible(i, now)) {
                // 遍历每一列打印
                for (const auto& s : schema) {
                    columns[s.name]->printValue(i); // 多态调用
                    std::cout << "\t";
                }
                std::cout << std::endl;
            }
        }
    }
};