#include <iostream>
#include "Column.h"
#include "Dictionary.h"

int main() {
    std::cout << "--- Havana DB Initializing ---" << std::endl;

    // 1. 模拟一张表：Product (String), Amount (Int)
    // 字符串列需要字典
    Dictionary dict_product;
    
    // 物理存储：产品列存的是 ID (int)，数量列存的是值 (int)
    Column<int> col_product_ids; 
    Column<int> col_amount;

    // 2. 模拟插入数据 (Insert-Only)
    // 插入 ("Tires", 10)
    int tire_id = dict_product.getId("Tires");
    col_product_ids.append(tire_id);
    col_amount.append(10);

    // 插入 ("Frames", 20)
    int frame_id = dict_product.getId("Frames");
    col_product_ids.append(frame_id);
    col_amount.append(20);

    // 插入 ("Tires", 5) - 第二次出现 Tires，ID 应该是一样的
    int tire_id_2 = dict_product.getId("Tires");
    col_product_ids.append(tire_id_2);
    col_amount.append(5);

    // 3. 模拟查询 (Select * from Table)
    std::cout << "Row | Product | Amount" << std::endl;
    for (size_t i = 0; i < col_product_ids.size(); ++i) {
        // 解码：从 ID 列拿数字 -> 去字典查字符串
        int p_id = col_product_ids.get(i);
        std::string p_name = dict_product.getVal(p_id);
        
        int amt = col_amount.get(i);

        std::cout << i << "   | " << p_name << "   | " << amt << std::endl;
    }

    return 0;
}