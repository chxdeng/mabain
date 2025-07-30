CPP=g++

CPPFLAGS = -I. -I.. -I../util -Wall -Werror -g -O2 -c -std=c++17
LDFLAGS  = -lpthread -lcrypto

TESTSOURCES=$(wildcard *.cpp)

all: mb_test mb_test1 mb_test2 mb_test_mp multi_writer_bug_test mb_bound_test mb_header_test sigbus_disk_pressure_test

mb_test: mabain_test.cpp ../libmabain.so
	$(CPP) $(CPPFLAGS) mabain_test.cpp
	$(CPP) mabain_test.o -o mb_test -L../ -lmabain $(LDFLAGS)

mb_test1: mbtest1.cpp ../libmabain.so
	$(CPP) $(CPPFLAGS) mbtest1.cpp 
	$(CPP) mbtest1.o -o mb_test1 -L../ -lmabain $(LDFLAGS)

mb_test2: mbtest2.cpp ../libmabain.so
	$(CPP) $(CPPFLAGS) mbtest2.cpp 
	$(CPP) mbtest2.o -o mb_test2 -L../ -lmabain $(LDFLAGS)

mb_test_mp: mbtest_mp.cpp ../libmabain.so
	$(CPP) $(CPPFLAGS) mbtest_mp.cpp
	$(CPP) mbtest_mp.o -o mb_test_mp -L../ -lmabain $(LDFLAGS)

multi_writer_bug_test: multi_writer_bug.cpp ../libmabain.so
	$(CPP) $(CPPFLAGS) multi_writer_bug.cpp
	$(CPP) multi_writer_bug.o -o multi_writer_bug_test -L../ -lmabain $(LDFLAGS)

mb_bound_test: mb_bound_test.cpp ../libmabain.so
	$(CPP) $(CPPFLAGS) mb_bound_test.cpp
	$(CPP) mb_bound_test.o -o mb_bound_test -L../ -lmabain $(LDFLAGS)

mb_header_test: mb_header_test.cpp ../libmabain.so
	$(CPP) $(CPPFLAGS) mb_header_test.cpp
	$(CPP) mb_header_test.o -o mb_header_test -L../ -lmabain $(LDFLAGS)

sigbus_disk_pressure_test: sigbus_disk_pressure_test.cpp ../libmabain.so
	$(CPP) $(CPPFLAGS) sigbus_disk_pressure_test.cpp
	$(CPP) sigbus_disk_pressure_test.o -o sigbus_disk_pressure_test -L../ -lmabain $(LDFLAGS)


clean:
	-rm -rf *.o mb_test* multi_writer_bug_test mb_bound_test mb_header_test sigbus_disk_pressure_test
