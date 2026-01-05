#include <iostream>
#include "Table.h"

int main() {
    // 1. 创建一个 "User" 表
    Table userTable("Users");

    // 2. 定义 Schema (这就像 SQL 的 CREATE TABLE)
    userTable.createColumn("ID", TYPE_INT);
    userTable.createColumn("Name", TYPE_STRING);
    userTable.createColumn("Age", TYPE_INT);

    std::cout << "Table Created." << std::endl;

    // 3. 插入数据
    // 注意：这里用 {} 构造 vector<variant>
    userTable.insertRow({1, std::string("Joe"), 23});
    userTable.insertRow({2, std::string("Alice"), 20});
    userTable.insertRow({3, std::string("Bob"), 35});

    // 4. 打印看看
    userTable.printAll();

    return 0;
}