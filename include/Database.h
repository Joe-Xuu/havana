#pragma once
#include <unordered_map>
#include <string>
#include <memory>
#include <iostream>
#include <sstream>
#include <vector>
#include "Table.h"

class Database {
private:
    std::unordered_map<std::string, std::unique_ptr<Table>> tables;

public:
    // 获取表对象
    Table* getTable(const std::string& name) {
        if (tables.find(name) == tables.end()) return nullptr;
        return tables[name].get();
    }

    // --- SQL 解析与执行核心 ---
    // 这是一个非常简化的 Parser，靠空格分割字符串
    void executeSQL(const std::string& sql) {
        std::stringstream ss(sql);
        std::string cmd;
        ss >> cmd;

        if (cmd == "CREATE") {
            handleCreate(ss);
        } else if (cmd == "INSERT") {
            handleInsert(ss);
        } else if (cmd == "SELECT") {
            handleSelect(ss);
        } else {
            std::cout << "Error: Unknown command '" << cmd << "'" << std::endl;
        }
    }

private:
    // 处理: CREATE TABLE table_name (col1 INT, col2 STRING)
void handleCreate(std::stringstream& ss) {
        std::string table_keyword, table_name;
        ss >> table_keyword >> table_name;

        if (table_keyword != "TABLE") {
            std::cout << "Syntax Error: Expected CREATE TABLE" << std::endl;
            return;
        }

        tables[table_name] = std::make_unique<Table>(table_name);
        Table* t = tables[table_name].get();

        std::string token;
        // 循环读取 token
        while (ss >> token) {
            // --- 修复逻辑：跳过独立的括号和逗号 ---
            if (token == "(" || token == "," || token == ")") continue;
            
            // 如果不是符号，那它就是列名
            std::string col_name = token;
            
            // 紧接着读取下一个 token 作为类型
            std::string type_str;
            if (!(ss >> type_str)) break; // 防止读没了

            // 清理可能粘在单词上的标点 (例如 "INT," -> "INT")
            // 简单的清理逻辑
            if (col_name.front() == '(') col_name.erase(0, 1);
            if (type_str.back() == ',' || type_str.back() == ')') type_str.pop_back();

            ColumnType type = (type_str == "INT") ? TYPE_INT : TYPE_STRING;
            t->createColumn(col_name, type, AGG_LAST);
        }
        std::cout << "Table '" << table_name << "' created." << std::endl;
    }

    // 处理: INSERT INTO table_name VALUES (1, "Alice")
    void handleInsert(std::stringstream& ss) {
        std::string into_kw, table_name, values_kw;
        ss >> into_kw >> table_name >> values_kw;

        Table* t = getTable(table_name);
        if (!t) {
            std::cout << "Error: Table '" << table_name << "' not found." << std::endl;
            return;
        }

        std::string val_str;
        std::vector<Table::Value> row;
        
        while (ss >> val_str) {
            // --- 修复逻辑：跳过独立的括号和逗号 ---
            // 如果遇到 "(", ",", ")" 这种单独的符号，直接跳过
            if (val_str == "(" || val_str == "," || val_str == ")") continue;

            // 清理粘连的符号 (如 "1," -> "1")
            size_t start = val_str.find_first_not_of("(),");
            size_t end = val_str.find_last_not_of("(),");
            
            // 如果全是符号（比如 "),"），清理后是空的，跳过
            if (start == std::string::npos) continue;
            
            std::string clean = val_str.substr(start, end - start + 1);

            if (clean.front() == '"') {
                // 字符串
                if (clean.back() == '"') {
                    row.push_back(clean.substr(1, clean.size() - 2));
                } else {
                    row.push_back(clean.substr(1)); // 容错
                }
            } else {
                // 数字
                try {
                    row.push_back(std::stoi(clean));
                } catch (...) {
                    std::cout << "Error: Invalid number format '" << clean << "'" << std::endl;
                    return;
                }
            }
        }
        
        try {
            t->insertRow(row);
            std::cout << "1 row inserted." << std::endl;
        } catch (std::exception& e) {
            std::cout << "Insert Error: " << e.what() << std::endl;
        }
    }

    // 处理: SELECT * FROM table_name
    void handleSelect(std::stringstream& ss) {
        std::string cols, from_kw, table_name;
        ss >> cols >> from_kw >> table_name;

        Table* t = getTable(table_name);
        if (!t) {
            std::cout << "Error: Table '" << table_name << "' not found." << std::endl;
            return;
        }

        if (cols == "*") {
            t->printAll();
        } else {
            // 暂时只支持 *
            std::cout << "Feature not implemented: Column selection" << std::endl;
        }
    }
};