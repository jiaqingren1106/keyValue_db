#include "../include/simpleBuf.h"
#include <iostream>

int main (int argc, char * argv[])
{
    SimpleBufferPool* buf = new SimpleBufferPool();
    buf->Put(0);
    buf->Put(0);
    buf->Put((kv_pair*)0x1);
    buf->Put((kv_pair*)0x2);
    buf->Put((kv_pair*)0x3);
    buf->Put((kv_pair*)0x4);
    buf->Delete((kv_pair*)0x10);
    for (kv_pair* i : buf->buffer) {
        std::cout << i << ' ';
    }
    return 0;
}
