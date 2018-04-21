ifndef MABAIN_INSTALL_DIR
MABAIN_INSTALL_DIR=/usr/local
endif

CPP=g++

all: mb_insert_test mb_lookup_test mb_longest_prefix_test \
	mb_remove_test mb_iterator_test mb_multi_proc_test \
	mb_rc_test mb_multi_thread_insert_test mb_memory_only_test

CFLAGS  = -I. -I$(MABAIN_INSTALL_DIR)/include -Wall -Werror -g -O0 -c -std=c++11
LDFLAGS = -lpthread -lcrypto -L$(MABAIN_INSTALL_DIR)/lib -lmabain

mb_insert_test: mb_insert_test.cpp
	$(CPP) $(CFLAGS) mb_insert_test.cpp
	$(CPP) mb_insert_test.o -o mb_insert_test $(LDFLAGS)
mb_lookup_test: mb_lookup_test.cpp
	$(CPP) $(CFLAGS) mb_lookup_test.cpp
	$(CPP) mb_lookup_test.o -o mb_lookup_test $(LDFLAGS)
mb_longest_prefix_test: mb_longest_prefix_test.cpp
	$(CPP) $(CFLAGS) mb_longest_prefix_test.cpp
	$(CPP) mb_longest_prefix_test.o -o mb_longest_prefix_test $(LDFLAGS)
mb_remove_test: mb_remove_test.cpp
	$(CPP) $(CFLAGS) mb_remove_test.cpp
	$(CPP) mb_remove_test.o -o mb_remove_test $(LDFLAGS)
mb_iterator_test: mb_iterator_test.cpp
	$(CPP) $(CFLAGS) mb_iterator_test.cpp
	$(CPP) mb_iterator_test.o -o mb_iterator_test $(LDFLAGS)
mb_multi_proc_test: mb_multi_proc_test.cpp
	$(CPP) $(CFLAGS) mb_multi_proc_test.cpp
	$(CPP) mb_multi_proc_test.o -o mb_multi_proc_test $(LDFLAGS)
mb_rc_test: mb_rc_test.cpp
	$(CPP) $(CFLAGS) mb_rc_test.cpp
	$(CPP) mb_rc_test.o -o mb_rc_test $(LDFLAGS)
mb_multi_thread_insert_test: mb_multi_thread_insert_test.cpp
	$(CPP) $(CFLAGS) mb_multi_thread_insert_test.cpp
	$(CPP) mb_multi_thread_insert_test.o -o mb_multi_thread_insert_test $(LDFLAGS)
mb_memory_only_test: mb_memory_only_test.cpp
	$(CPP) $(CFLAGS) mb_memory_only_test.cpp
	$(CPP) mb_memory_only_test.o -o mb_memory_only_test $(LDFLAGS)

build: all
clean:
	-rm -f ./*.o ./mb_*_test
	-rm -rf ./tmp_dir
