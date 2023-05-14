CC = g++
FLAGS = -Wall -g

SRCS = src/database.cpp \
		src/memtable.cpp \
		src/sstable.cpp \
		src/naiveStorage.cpp \
		src/lsm_tree.cpp \
		src/extendibleHash.cpp \
		src/clockEviction.cpp \
		src/simpleLRU.cpp \
		src/BPTree.cpp
		
scan_test: test/scan_test.cpp ${SRCS}
	${CC} ${FLAGS} -o $@ $^
	
main: main.cpp ${SRCS}
	${CC} ${FLAGS} -o $@ $^

memtable_test: test/memtable_test.cpp src/simpleMem.cpp
	${CC} ${FLAGS} -o $@ $^

  
bufferpool_test: test/bufferpool_test.cpp src/simpleBuf.cpp
	${CC} ${FLAGS} -o $@ $^

lru_test: test/lru_test.cpp src/simpleLRU.cpp
	${CC} ${FLAGS} -o $@ $^

clock_test: test/clock_test.cpp src/clockEviction.cpp src/extendibleHash.cpp
	${CC} ${FLAGS} -o $@ $^
# ${CC} ${FLAGS} -o clock_test clock_test.cpp src/clockEviction.cpp
hashtable_test: test/hashtable_test.cpp src/extendibleHash.cpp
	${CC} ${FLAGS} -o $@ $^
