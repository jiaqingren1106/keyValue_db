
#include "../include/database.h"

#include <cstdio>
int main (int argc, char * argv[])
{
    std::cout << "First" << "\n";
    Database db = Database();
    std::cout << "Constructor is ok" << "\n";
    db.Open("scan_test_db");
    std::cout << "Open" << "\n";

    for (int i = 0; i < 1024; i++) {
        db.Put(i, i);
    }

    auto result = db.Scan(1000, 100000);

    std::cout << "[";
    for (auto item : result) {
        std::cout << "(" << item.key << "," << item.value << ")" << ";";
    }
    std::cout << "]\n";

    db.Close();

}
