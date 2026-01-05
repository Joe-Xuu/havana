#pragma once
#include <atomic>
#include <iostream>
#include "Column.h"
#include "Dictionary.h"
#include "MvccMeta.h"

class Table {
private:
    // 1. 数据存储区
    Dictionary dict_product;
    Column<int> col_product_ids; // 存的是 ID
    Column<int> col_amount;      // 存的是数值
    
    // 2. 元数据区
    MvccMeta meta; //一个大数组

    // 3. 全局逻辑时钟 (Atomic 保证线程安全)
    // 每次写操作，时间 +1
    std::atomic<uint64_t> global_ts{0};

public:
    // --- 写入操作 ---

    // 插入 (Insert)
    void insert(const std::string& product, int amount) {
        uint64_t tx_id = ++global_ts; // 获取当前事务时间

        int prod_id = dict_product.getId(product); // 字典编码
        
        // 物理追加
        col_product_ids.append(prod_id);
        col_amount.append(amount);
        meta.append(tx_id); // 记录出生时间
    }

    // 更新 (Update) = "逻辑更新，物理插入" [cite: 308]
    // 为了简化，我们需要用户告诉我们要更新哪一行 (row_index)
    void update(size_t row_index, int new_amount) {
        uint64_t tx_id = ++global_ts; // tx_id 写操作时间加一

        // 1. 判读该行是否有效 (防止更新了已经是旧版本的数据)
        // 简单起见，这里假设用户传进来的是最新行的 index

        // 2. 将旧版本“杀死” (设置失效时间) [cite: 307]
        if (row_index < meta.t_invalidated.size()) {
            meta.t_invalidated[row_index] = tx_id;
        }

        // 3. 复制旧数据的未修改列 (Product 不变)
        int old_prod_id = col_product_ids.get(row_index);

        // 4. 插入新版本 (Product 用旧的，Amount 用新的)
        col_product_ids.append(old_prod_id);
        col_amount.append(new_amount);
        meta.append(tx_id); // 新行出生
    }

    // --- 读取操作 ---

    // 打印当前时间点可见的所有数据
    // 如果 query_ts 传 0，就用当前最新时间
    void printTable(uint64_t query_ts = 0) {
        if (query_ts == 0) query_ts = global_ts.load();

        std::cout << "--- Query at Time " << query_ts << " ---" << std::endl;
        std::cout << "Row | Product | Amount | Created | Invalidated" << std::endl;

        for (size_t i = 0; i < col_product_ids.size(); ++i) {
            // 核心：MVCC 可见性判断
            if (meta.isVisible(i, query_ts)) {
                int p_id = col_product_ids.get(i);
                std::string p_name = dict_product.getVal(p_id);
                int amt = col_amount.get(i);
                
                std::cout << i << "   | " << p_name << "   | " << amt 
                          << "     | " << meta.t_created[i] 
                          << "       | " << (meta.t_invalidated[i] == INF_TS ? "INF" : std::to_string(meta.t_invalidated[i]))
                          << std::endl;
            }
        }
        std::cout << "------------------------" << std::endl;
    }
    
    // 调试用：打印底层物理表的所有行（包括死掉的）
    void debugDump() {
         std::cout << "[DEBUG] Full Physical Table Dump:" << std::endl;
         for (size_t i = 0; i < col_product_ids.size(); ++i) {
             int p_id = col_product_ids.get(i);
             std::string p_name = dict_product.getVal(p_id);
             std::cout << "PhyRow " << i << ": " << p_name << ", " << col_amount.get(i) 
                       << " (Born: " << meta.t_created[i] << ", Died: " << meta.t_invalidated[i] << ")" << std::endl;
         }
    }

    // 查询引擎
    // SQL 语义: SELECT row_id FROM Table WHERE product = search_val
    std::vector<size_t> select(const std::string& search_val, uint64_t query_ts = 0) {
        if (query_ts == 0) query_ts = global_ts.load();
        
        std::vector<size_t> result_rows;
        
        // 1. 字典编码转换：先把字符串转成 int
        // 如果字典里压根没这个词，说明肯定查不到，直接返回空
        int search_id = dict_product.getId(search_val); 
        // 注意：这里有个小瑕疵，如果 "Tires" 从没插入过，getId 会自动创建它。
        // 严谨的数据库应该用 checkId (只查不建)

        // 2. 全表扫描 (Full Table Scan)
        // 这是列式数据库性能最关键的地方！
        // 因为 col_product_ids.data 在内存里是连续的，CPU 缓存命中率极高。
        size_t total_rows = col_product_ids.size();
        for (size_t i = 0; i < total_rows; ++i) {
            
            // 先比对数值 (极快，纯整数比较)
            if (col_product_ids.get(i) == search_id) {
                // 再检查 MVCC (这一步稍微慢点，因为要读另外两个数组)
                if (meta.isVisible(i, query_ts)) {
                    result_rows.push_back(i);
                }
            }
        }
        
        return result_rows;
    }

    // 模拟 SQL: SELECT row_id FROM Table WHERE product = 'target_prod' AND amount > min_amount
    std::vector<size_t> select_complex(const std::string& target_prod, int min_amount) {
        uint64_t query_ts = global_ts.load();
        std::vector<size_t> result_rows;

        int prod_id = dict_product.getId(target_prod);
        size_t total_rows = col_product_ids.size();

        for (size_t i = 0; i < total_rows; ++i) {
            // --- 优化核心：短路求值 (Short-circuit) ---
            
            // 1. 第一关：先查 Product 列
            // 如果这一行连产品名都不对，后面看都不要看，直接 continue
            if (col_product_ids.get(i) != prod_id) {
                continue;
            }

            // 2. 第二关：再查 Amount 列
            // 只有过了第一关的“幸存者”，我们才去读取 col_amount 的内存
            // 这能极大减少内存访问 (Cache Miss)
            if (col_amount.get(i) <= min_amount) {
                continue;
            }

            // 3. 第三关：最后查 MVCC
            // 为什么最后查？因为 MVCC 还要算时间，比较费 CPU
            if (meta.isVisible(i, query_ts)) {
                result_rows.push_back(i);
            }
        }
        return result_rows;
    }
    
    // 辅助函数：根据行号读取数据 (为了验证结果)
    int getAmount(size_t row_index) {
        return col_amount.get(row_index);
    }
};