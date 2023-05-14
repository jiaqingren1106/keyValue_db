#include "../include/simpleLRU.h"
#include <iostream>
#include <assert.h>

int main(int argc, char * argv[]) {
    SimpleLRU cache(3);
    assert(-1 == cache.accessAndUpdate(1));
    assert(-1 == cache.accessAndUpdate(2));
    assert(-1 == cache.accessAndUpdate(3));
    assert(-1 == cache.accessAndUpdate(1));
    cache.evict(2);
    assert(-1 == cache.accessAndUpdate(4));
    assert(3 == cache.accessAndUpdate(5));
    cache.display();
    return 0;
}
