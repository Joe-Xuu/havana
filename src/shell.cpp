#include <iostream>
#include <string>
#include "Database.h"

int main() {
    Database db;
    std::string input;

    std::cout << "=== HavanaDB SQL Shell ===" << std::endl;
    std::cout << "Type 'exit' to quit." << std::endl;

    while (true) {
        std::cout << "havana> ";
        // 读取整行
        if (!std::getline(std::cin, input)) break;
        if (input == "exit") break;
        if (input.empty()) continue;

        db.executeSQL(input);
    }

    return 0;
}