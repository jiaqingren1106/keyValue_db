// A simple impl of memtable

#include "../include/simpleMem.h"

int main (int argc, char * argv[])
{
    SimpleMemtable mem = SimpleMemtable();
    mem.Put(0, 0);
    mem.Put(1, 1);
    mem.Put(2, 2);
    mem.Put(3, 3);
    mem.Put(4, 4);

    printf("%d\n", mem.Get(0));
    kv_pair* results = mem.Scan(2, 4);
    for (int i = 0; i < 5; i++) {
        printf("(%d, %d);", results[i].key, results[i].value);
    }
    return 0;
}
