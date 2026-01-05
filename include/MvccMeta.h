#pragma once
#include <vector>
#include <cstdint>
#include <limits>

// alive 无穷大
const uint64_t INF_TS = std::numeric_limits<uint64_t>::max();

class MvccMeta {
public:
    std::vector<uint64_t> t_created;     // 创建 u64的vector数组
    std::vector<uint64_t> t_invalidated; // 失效

    // 追加一行新的元数据
    void append(uint64_t current_ts) {
        t_created.push_back(current_ts); //数组更新
        t_invalidated.push_back(INF_TS); // 刚生下来时，还没死，所以是无穷大
    }

    // ！！这一行数据对于某个查询时间点 (query_ts) 是否可见
    // 论文逻辑：(出生 <= 查询时间) AND (死亡 > 查询时间) [cite: 231]
    bool isVisible(size_t row_index, uint64_t query_ts) const {
        if (row_index >= t_created.size()) return false;
        
        bool born_before_query = t_created[row_index] <= query_ts;
        bool died_after_query = t_invalidated[row_index] > query_ts;
        
        return born_before_query && died_after_query;
    }
};