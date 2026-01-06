// #pragma once
// #include <iostream>
// #include <vector>
// #include <string>
// #include <unordered_map>
// #include <memory>
// #include <variant>
// #include <atomic>
// #include <shared_mutex>
// #include "Column.h"
// #include "MvccMeta.h"
// #include "HashIndex.h"
// #include "AsyncLogger.h" 

// // 在 Table.h 顶部加入

// enum AggType { 
//     AGG_LAST, // 普通属性
//     AGG_SUM   // 累积属性：select sum (如：库存变动、销售额)
// };

// enum ColumnType { TYPE_INT, TYPE_STRING };

// class Table {
// private:
//     // 列名 -> 索引对象
//     std::unordered_map<std::string, std::unique_ptr<HashIndex>> indexes;
//     std::string table_name;
//     std::unique_ptr<AsyncLogger> logger;
//     size_t max_capacity; // 最大容量


//     // Schema 定义
//     struct ColMeta {
//         std::string name;
//         ColumnType type;
//         AggType agg_type; // MVCC, Delta log hybrid entry
//     };
//     std::vector<ColMeta> schema;
//     std::unordered_map<std::string, std::unique_ptr<AbstractColumn>> columns;

//     // MVCC & 事务控制
//     MvccMeta meta;
//     std::atomic<uint64_t> global_ts{0};

//     // --- Lock-Free 核心 ---
//     std::atomic<size_t> tail_index{0}; // 原子游标：当前空闲位置

//     // 只用来保护 schema 变更，不保护 insert
//     mutable std::shared_mutex schema_lock;

// public:
//     using Value = std::variant<int, std::string>;

//     // DDL: 加列 (必须预分配)
//     Table(std::string name) : table_name(name) {
//         // 1. 自动生成文件名: "TableName.log"
//         std::string filename = name + ".log";
        
//         // 2. 初始化 Logger
//         // std::make_unique 会在堆上创建一个 Logger 对象，并返回指针
//         logger = std::make_unique<AsyncLogger>(filename);
//     }

//     // has_index
//     void createColumn(const std::string& name, ColumnType type, AggType agg_type = AGG_LAST, bool has_index = false) {
//         std::unique_lock lock(schema_lock);
//         schema.push_back({name, type, agg_type});

//         // 创建列内存
//         if (type == TYPE_INT) {
//             columns[name] = std::make_unique<Column<int>>();
//         } else {
//             columns[name] = std::make_unique<Column<std::string>>();
//         }

//         // --- 新增：如果需要索引，创建 Index 对象 ---
//         if (has_index) {
//             // 目前只支持 String 类型的索引 (主键通常是 String)
//             if (type == TYPE_STRING) {
//                 indexes[name] = std::make_unique<HashIndex>();
//             } else {
//                 std::cout << "Warning: Index on INT not supported yet in this version." << std::endl;
//             }
//         }
//     }

//     void insertRow(const std::vector<Value>& row_data) {
//         // 1. 领号
//         size_t my_idx = tail_index.fetch_add(1);

//         // 2. 计算块号
//         size_t chunk_idx = my_idx / CHUNK_SIZE;

//         // 3. 关键：确保这个块存在！
//         // 这一步虽然有锁，但只有每 10 万行的第一行才会触发，其他时候直接 return
//         // 我们需要通知所有列（以及 MVCC）去检查并分配内存
//         // 注意：多线程下，多个线程可能同时进入新块，ensureChunk 内部处理了并发
        
//         // 先搞定 MVCC 的内存
//         meta.ensureChunk(chunk_idx);

//         // 再搞定每一列的内存
//         for (auto& kv : columns) {
//             kv.second->ensureChunk(chunk_idx);
//         }

//         // 4. 拿到事务 ID
//         uint64_t tx_id = ++global_ts;

//         // 5. 写入数据
//         for (size_t i = 0; i < schema.size(); ++i) {
//             const auto& col_name = schema[i].name;
//             const auto& val = row_data[i];

//         // A. 写入数据列
//             if (std::holds_alternative<int>(val)) {
//                 columns[col_name]->set(my_idx, std::get<int>(val));
//             } else {
//                 const std::string& s_val = std::get<std::string>(val);
//                 columns[col_name]->set(my_idx, s_val);

//             // B. --- 新增：检查这一列有没有索引 ---
//                 if (indexes.find(col_name) != indexes.end()) {
//                     // 如果有，把 (值, 行号) 插入索引
//                     indexes[col_name]->insert(s_val, my_idx);
//                 }
//             }
//         }

//         // 6. 提交
//         meta.setCreated(my_idx, tx_id);

//     if (logger) {
//         // 这一部分会产生临时 string，稍微有点开销，但比写盘快多了
//         std::vector<std::string> log_entry;
//         log_entry.reserve(schema.size());
//         for (const auto& val : row_data) {
//             if (std::holds_alternative<int>(val)) {
//                 log_entry.push_back(std::to_string(std::get<int>(val)));
//             } else {
//                 log_entry.push_back(std::get<std::string>(val));
//             }
//         }
//         // 调用异步 append
//         logger->append(log_entry);
//     }
//     }

//     // 打印当前所有可见数据
//     void printAll() {
//         std::shared_lock lock(schema_lock);
        
//         // 遍历上限不再是 capacity，而是当前写到了哪里
//         size_t current_limit = tail_index.load();
//         // 既然 insert 可能会超 capacity，这里要做个 min
//         uint64_t now = global_ts.load();

//         std::cout << "=== Table: " << table_name << " (Rows: " << current_limit << "/" << max_capacity << ") ===" << std::endl;
//         for (const auto& s : schema) std::cout << s.name << "\t";
//         std::cout << std::endl;

//         for (size_t i = 0; i < current_limit; ++i) {
//             if (meta.isVisible(i, now)) {
//                 for (const auto& s : schema) {
//                     columns[s.name]->printValue(i);
//                     std::cout << "\t";
//                 }
//                 std::cout << std::endl;
//             }
//         }
//     }

//     // 模拟 SQL: SELECT * FROM Table WHERE Product = 'key_val'
//     // 返回的结果是一个“合成”出来的行// 模拟 SQL: SELECT * FROM Table WHERE Product = 'key_val'
//     std::unordered_map<std::string, std::string> querySnapshot(const std::string& key_col_name, const std::string& key_val) {
//         uint64_t query_ts = global_ts.load();
        
//         // 结果容器
//         std::unordered_map<std::string, std::string> result;
//         std::unordered_map<std::string, uint64_t> last_seen_ts;

//         // --- 步骤 1: 确定候选行 (Candidate Selection) ---
//         std::vector<size_t> candidate_rows;

//         // 检查这一列是否有索引
//         if (indexes.find(key_col_name) != indexes.end()) {
//             // [路径 A]: 走索引加速 (O(1) 查找)
//             // 直接获取相关的行号列表
//             candidate_rows = indexes[key_col_name]->get(key_val);
//         } else {
//             // [路径 B]: 没索引，只能全表扫描 (O(N))
//             size_t limit = tail_index.load();
//             candidate_rows.reserve(limit);
//             for (size_t i = 0; i < limit; ++i) {
//                 candidate_rows.push_back(i);
//             }
//         }

//         // 获取主键列指针 (用于双重检查)
//         auto* key_col = dynamic_cast<Column<std::string>*>(columns[key_col_name].get());

//         // --- 步骤 2: 遍历候选行 ---
//         for (size_t i : candidate_rows) {
//             // 1. MVCC 可见性 (只看 query_ts 之前出生的)
//             if (!meta.isVisible(i, query_ts)) continue;

//             // 2. Key 值校验
//             // 如果是走索引进来的，这一步通常是多余的(肯定相等)
//             // 但如果是全表扫描进来的，这一步是必须的
//             if (key_col->get(i) != key_val) continue;

//             // 获取这行数据的出生时间
//             uint64_t row_ts = meta.getCreated(i);

//             // 3. 混合聚合逻辑 (Hybrid Logic: MVCC + Delta)
//             for (const auto& s : schema) {
//                 // 跳过主键列本身
//                 if (s.name == key_col_name) continue;

//                 if (s.agg_type == AGG_SUM) {
//                     // --- [累积模式] (Delta) ---
//                     // 逻辑：把所有历史记录加起来
//                     int val = 0; 
//                     auto* col = dynamic_cast<Column<int>*>(columns[s.name].get());
//                     val = col->get(i);
                    
//                     // 读取旧累积值，加上新值
//                     int old_sum = 0;
//                     if (result.find(s.name) != result.end()) {
//                         old_sum = std::stoi(result[s.name]);
//                     }
//                     result[s.name] = std::to_string(old_sum + val);
//                 } 
//                 else if (s.agg_type == AGG_LAST) {
//                     // --- [覆盖模式] (MVCC) ---
//                     // 逻辑：只有当这行的时间比我之前看到的更新，我才替换
//                     if (row_ts > last_seen_ts[s.name]) {
//                         if (s.type == TYPE_INT) {
//                             auto* col = dynamic_cast<Column<int>*>(columns[s.name].get());
//                             result[s.name] = std::to_string(col->get(i));
//                         } else {
//                             auto* col = dynamic_cast<Column<std::string>*>(columns[s.name].get());
//                             result[s.name] = col->get(i);
//                         }
//                         last_seen_ts[s.name] = row_ts; // 更新“最新时间”
//                     }
//                 }
//             }
//         }
        
//         // 最后把主键也放进去
//         result[key_col_name] = key_val;
//         return result;
//     }
// };



#pragma once
#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>
#include <memory>
#include <variant>
#include <atomic>
#include <shared_mutex>
#include "Column.h"
#include "MvccMeta.h"
#include "HashIndex.h"
#include "BinaryLogger.h"

// 聚合类型定义
enum AggType { 
    AGG_LAST, // 普通属性 (MVCC)
    AGG_SUM   // 累积属性 (Delta)
};

enum ColumnType { TYPE_INT, TYPE_STRING };

class Table {
private:
    std::string table_name;

    // Schema 定义
    struct ColMeta {
        std::string name;
        ColumnType type;
        AggType agg_type;
    };
    std::vector<ColMeta> schema;

    // 存储引擎核心组件
    std::unordered_map<std::string, std::unique_ptr<AbstractColumn>> columns;
    std::unordered_map<std::string, std::unique_ptr<HashIndex>> indexes;
    
    // MVCC & 事务
    MvccMeta meta;
    std::atomic<uint64_t> global_ts{0};

    // 无锁写入游标
    std::atomic<size_t> tail_index{0};

    // 日志管理器
    std::unique_ptr<BinaryLogger> logger;

    // 锁 (仅保护 Schema 变更)
    mutable std::shared_mutex schema_lock;

public:
    using Value = std::variant<int, std::string>;

    // 构造函数
    // truncate_log: true = 清空旧日志(新建表); false = 保留旧日志(用于恢复)
    Table(std::string name, bool truncate_log = true) : table_name(name) {
        std::string filename = name + ".log";
        // 初始化二进制日志
        logger = std::make_unique<BinaryLogger>(filename, truncate_log);
    }

    // DDL: 创建列
    void createColumn(const std::string& name, ColumnType type, AggType agg_type = AGG_LAST, bool has_index = false) {
        std::unique_lock lock(schema_lock);
        schema.push_back({name, type, agg_type});

        // 1. 创建列数据存储
        if (type == TYPE_INT) {
            columns[name] = std::make_unique<Column<int>>();
        } else {
            columns[name] = std::make_unique<Column<std::string>>();
        }

        // 2. 创建索引 (目前仅支持 String 索引)
        if (has_index && type == TYPE_STRING) {
            indexes[name] = std::make_unique<HashIndex>();
        }
    }

    // DML: 插入数据 (支持日志开关)
    // enable_logging: 正常写入为 true，恢复(Recover)时为 false
    void insertRow(const std::vector<Value>& row_data, bool enable_logging = true) {
        // 1. 领号 (Atomic)
        size_t my_idx = tail_index.fetch_add(1);

        // 2. 自动扩容 (Chunking)
        size_t chunk_idx = my_idx / CHUNK_SIZE;
        meta.ensureChunk(chunk_idx);
        for (auto& kv : columns) kv.second->ensureChunk(chunk_idx);

        uint64_t tx_id = ++global_ts;

        // 3. 写入内存 & 更新索引
        for (size_t i = 0; i < schema.size(); ++i) {
            const auto& col_name = schema[i].name;
            const auto& val = row_data[i];

            if (std::holds_alternative<int>(val)) {
                columns[col_name]->set(my_idx, std::get<int>(val));
            } else {
                const std::string& s_val = std::get<std::string>(val);
                columns[col_name]->set(my_idx, s_val);
                
                // 更新索引
                if (indexes.find(col_name) != indexes.end()) {
                    indexes[col_name]->insert(s_val, my_idx);
                }
            }
        }

        // 4. 提交内存 (MVCC 生效)
        meta.setCreated(my_idx, tx_id);

        // 5. 写二进制日志 (WAL)
        if (enable_logging && logger) {
            logger->appendEntry(row_data);
        }
    }

    // 崩溃恢复
    void recover() {
        std::string filename = table_name + ".log";
        std::cout << "[System] Recovering table '" << table_name << "' from " << filename << "..." << std::endl;

        // 准备 Schema 类型映射 (0:INT, 1:STRING)
        std::vector<int> col_types;
        for (const auto& col : schema) {
            col_types.push_back((col.type == TYPE_INT) ? 0 : 1);
        }

        // 读取并重放
        auto rows = BinaryLogger::readLog(filename, col_types);
        int count = 0;
        for (const auto& row : rows) {
            insertRow(row, false); // false = 不再写日志
            count++;
        }
        std::cout << "[System] Recovery complete. Replayed " << count << " rows." << std::endl;
    }

    // 快照查询 (带索引加速 + 混合聚合)
    std::unordered_map<std::string, std::string> querySnapshot(const std::string& key_col_name, const std::string& key_val) {
        uint64_t query_ts = global_ts.load();
        
        std::unordered_map<std::string, std::string> result;
        std::unordered_map<std::string, uint64_t> last_seen_ts;
        std::vector<size_t> candidate_rows;

        // A. 索引加速
        if (indexes.find(key_col_name) != indexes.end()) {
            candidate_rows = indexes[key_col_name]->get(key_val);
        } else {
            // B. 全表扫描
            size_t limit = tail_index.load();
            candidate_rows.reserve(limit);
            for(size_t i=0; i<limit; ++i) candidate_rows.push_back(i);
        }

        auto* key_col = dynamic_cast<Column<std::string>*>(columns[key_col_name].get());

        for (size_t i : candidate_rows) {
            // MVCC & Key 检查
            if (!meta.isVisible(i, query_ts)) continue;
            if (key_col->get(i) != key_val) continue;

            uint64_t row_ts = meta.getCreated(i);

            // 混合聚合逻辑
            for (const auto& s : schema) {
                if (s.name == key_col_name) continue;

                if (s.agg_type == AGG_SUM) {
                    // Delta Accumulation
                    auto* col = dynamic_cast<Column<int>*>(columns[s.name].get());
                    int val = col->get(i);
                    int old_sum = 0;
                    if (result.find(s.name) != result.end()) old_sum = std::stoi(result[s.name]);
                    result[s.name] = std::to_string(old_sum + val);
                } 
                else if (s.agg_type == AGG_LAST) {
                    // MVCC Overwrite
                    if (row_ts > last_seen_ts[s.name]) {
                        if (s.type == TYPE_INT) {
                            auto* col = dynamic_cast<Column<int>*>(columns[s.name].get());
                            result[s.name] = std::to_string(col->get(i));
                        } else {
                            auto* col = dynamic_cast<Column<std::string>*>(columns[s.name].get());
                            result[s.name] = col->get(i);
                        }
                        last_seen_ts[s.name] = row_ts;
                    }
                }
            }
        }
        result[key_col_name] = key_val;
        return result;
    }
};