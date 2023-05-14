CC = g++
FLAGS = -Wall -g -w

SRCS = src/database.cpp \
		src/memtable.cpp \
		src/simpleMem.cpp \
		src/sstable.cpp \
		src/naiveStorage.cpp \
		src/lsm_tree.cpp \
		src/extendibleHash.cpp \
		src/clockEviction.cpp \
		src/simpleLRU.cpp \
		src/BPTree.cpp

EXPS = experiment_step_1 experiment_step_2_sst experiment_step_2_eviction experiment_step_3_lsm experiment_step_3_bloom

all: ${EXPS}

clean:
	rm -f ${EXPS}
	rm -rf db_experiment_*
	rm -f *.png
	rm -f *.txt

# $@ is target, $^ is all prerequisites

experiment_step_1: experiment_step_1.cpp ${SRCS}
	${CC} ${FLAGS} -o $@ $^

experiment_step_2_sst: experiment_step_2_sst.cpp ${SRCS}
	${CC} ${FLAGS} -o $@ $^

experiment_step_2_eviction: experiment_step_2_eviction.cpp ${SRCS}
	${CC} ${FLAGS} -o $@ $^

experiment_step_3_bloom: experiment_step_3_bloom.cpp ${SRCS}
	${CC} ${FLAGS} -o $@ $^

experiment_step_3_lsm: experiment_step_3_lsm.cpp ${SRCS}
	${CC} ${FLAGS} -o $@ $^

unittest: test.cpp ${SRCS}
	${CC} ${FLAGS} -o $@ $^

