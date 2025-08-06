CPP=g++

CPPFLAGS = -I. -I.. -I../util -Wall -Werror -g -O2 -c -std=c++17
LDFLAGS  = -lpthread -lcrypto -ljemalloc

TESTSOURCES=$(wildcard *.cpp)

all: mb_test mb_test1 mb_test2 mb_test_mp multi_writer_bug_test mb_bound_test mb_header_test \
	mb_mm_test mb_mm_prune_test sigbus_disk_pressure_test jemalloc_test

mb_mm_prune_test: mb_mm_prune_test.cpp
	$(CPP) $(CPPFLAGS) mb_mm_prune_test.cpp
	$(CPP) mb_mm_prune_test.o -o mb_mm_prune_test -lmabain $(LDFLAGS)

mb_mm_test: mb_mm_test.cpp
	$(CPP) $(CPPFLAGS) mb_mm_test.cpp
	$(CPP) mb_mm_test.o -o mb_mm_test -lmabain $(LDFLAGS)

mb_test: mabain_test.cpp
	$(CPP) $(CPPFLAGS) mabain_test.cpp
	$(CPP) mabain_test.o -o mb_test -lmabain $(LDFLAGS)

jemalloc_test: jemalloc_test.cpp
	$(CPP) $(CPPFLAGS) jemalloc_test.cpp
	$(CPP) jemalloc_test.o -o jemalloc_test -lmabain $(LDFLAGS)

mb_test1: mbtest1.cpp
	$(CPP) $(CPPFLAGS) mbtest1.cpp 
	$(CPP) mbtest1.o -o mb_test1 -lmabain $(LDFLAGS)

mb_test2: mbtest2.cpp
	$(CPP) $(CPPFLAGS) mbtest2.cpp 
	$(CPP) mbtest2.o -o mb_test2 -lmabain $(LDFLAGS)

mb_test_mp: mbtest_mp.cpp
	$(CPP) $(CPPFLAGS) mbtest_mp.cpp
	$(CPP) mbtest_mp.o -o mb_test_mp -lmabain $(LDFLAGS)

multi_writer_bug_test: multi_writer_bug.cpp
	$(CPP) $(CPPFLAGS) multi_writer_bug.cpp
	$(CPP) multi_writer_bug.o -o multi_writer_bug_test -lmabain $(LDFLAGS)

mb_bound_test: mb_bound_test.cpp
	$(CPP) $(CPPFLAGS) mb_bound_test.cpp
	$(CPP) mb_bound_test.o -o mb_bound_test -lmabain $(LDFLAGS)

mb_header_test: mb_header_test.cpp
	$(CPP) $(CPPFLAGS) mb_header_test.cpp
	$(CPP) mb_header_test.o -o mb_header_test -lmabain $(LDFLAGS)

sigbus_disk_pressure_test: sigbus_disk_pressure_test.cpp ../libmabain.so
	$(CPP) $(CPPFLAGS) sigbus_disk_pressure_test.cpp
	$(CPP) sigbus_disk_pressure_test.o -o sigbus_disk_pressure_test -L../ -lmabain $(LDFLAGS)


clean:
	-rm -rf *.o mb_test* multi_writer_bug_test mb_bound_test mb_header_test mb_mm_test mb_mm_prune_test sigbus_disk_pressure_test jemalloc_test
