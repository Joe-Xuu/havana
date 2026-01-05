#include <iostream>
#include "Table.h"

int main() {
    Table myTable;

    std::cout << "=== 1. Initial Insert ===" << std::endl;
    myTable.insert("Tires", 10); // Transaction 1
    myTable.insert("Frames", 20); // Transaction 2
    
    myTable.printTable(); // 应该看到两行

    std::cout << "\n=== 2. Update Tires (10 -> 15) ===" << std::endl;
    // 我们知道 Tires 在第 0 行。
    // 在真实数据库里，需要先 Select 找到 row_id，这里手动指定
    myTable.update(0, 15); // Transaction 3
    
    // 此时物理上有3行数据，但在最新时间点，应该只看到2行（第0行Tires已死，第2行Tires新生）
    myTable.printTable(); 

    std::cout << "\n=== 3. Time Travel Query (Back to Time 2) ===" << std::endl;
    // 回到更新之前的时间点
    // 那时候 Tires 还是 10，Frames 还是 20
    myTable.printTable(2); 

    std::cout << "\n=== 4. Debug: Physical Storage ===" << std::endl;
    // 看看底层到底存了什么
    myTable.debugDump();

    return 0;
}