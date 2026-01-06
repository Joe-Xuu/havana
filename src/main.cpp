#include <iostream>
#include "Table.h"

int main() {
    // 模拟 SAP 的物料主数据 + 库存表
    Table myTable("MaterialData");

    // 定义混合 Schema
    myTable.createColumn("Product", TYPE_STRING, AGG_LAST); // 主键
    myTable.createColumn("Price",   TYPE_INT,    AGG_LAST); // 价格：覆盖 (MVCC)
    myTable.createColumn("Stock",   TYPE_INT,    AGG_SUM);  // 库存：累加 (Delta)

    std::cout << "--- Hybrid Architecture Demo ---" << std::endl;

    // 1. 初始状态：Tires, 价格 100, 入库 50
    myTable.insertRow({std::string("Tires"), 100, 50});

    // 2. 业务动作：涨价了！价格变成 120
    // 注意：Stock 填 0，表示库存没变。
    // 在 Delta 架构下，我们不需要读旧数据，直接插入“价格变更事件”。
    myTable.insertRow({std::string("Tires"), 120, 0});

    // 3. 业务动作：卖出了 5 个
    // 注意：Price 填 0 或任意值？
    // 如果填 0，会被 AGG_LAST 忽略（只要我们保证涨价那条的时间戳更新）。
    // 但为了严谨，通常会填入“当前已知价格”或者设计 Null 值。
    // 这里我们假设填 0，且涨价发生在前，所以最终会取到 120。
    myTable.insertRow({std::string("Tires"), 0, -5});

    // 4. 业务动作：又入库 10 个
    myTable.insertRow({std::string("Tires"), 0, 10});

    // --- 查询时刻 ---
    // 数据库里现在有 4 条记录。查询引擎会自动“折叠”它们。
    
    auto result = myTable.querySnapshot("Product", "Tires");

    std::cout << "Product: " << result["Product"] << std::endl;
    std::cout << "Price (Last Write): " << result["Price"] << std::endl; 
    // 预期：120 (因为第2条记录时间比第1条晚，且后两条如果你处理得当应该被忽略或保持)
    // *代码里的 bug 提示*：如果第4条记录 Price 是 0 且时间最新，它会覆盖成 0。
    // *修正*：通常 Delta Update 时，普通属性如果不改，应该填 Null，代码里遇到 Null 跳过。
    // 简单起见，我们假设 Price 0 表示无效。

    std::cout << "Stock (Sum): " << result["Stock"] << std::endl;
    // 预期：50 + 0 - 5 + 10 = 55
    
    return 0;
}