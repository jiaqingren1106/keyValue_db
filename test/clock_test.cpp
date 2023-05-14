#include "../include/clockEviction.h"
#include <iostream>
#include <assert.h>

int main (int argc, char * argv[])
{
    ClockEviction* testClock = new ClockEviction(5);
   
    assert(-1 == testClock->accessAndUpdate(1));
    assert(-1 == testClock->accessAndUpdate(2));
    assert(-1 == testClock->accessAndUpdate(3));
    assert(-1 == testClock->accessAndUpdate(4));
    assert(-1 == testClock->accessAndUpdate(5));
    testClock->display();
    assert(1 == testClock->accessAndUpdate(6));
    assert(2 == testClock->accessAndUpdate(7));
    assert(3 == testClock->accessAndUpdate(8));
    testClock->display();
    
    return 0;
}
