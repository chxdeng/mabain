TEST_BIN_NAME=mb_test

CPP=g++

CPPFLAGS = -I. -I.. -I../util -Wall -Werror -g -O2 -c -std=c++11
LDFLAGS  = -lpthread

TESTSOURCES=$(wildcard *.cpp)

all: mb_test

mb_test: mabain_test.cpp ../libmabain.so
	$(CPP) $(CPPFLAGS) mabain_test.cpp
	$(CPP) mabain_test.o -o $(TEST_BIN_NAME) -L../ -lmabain $(LDFLAGS)

clean:
	-rm -rf *.o $(TEST_BIN_NAME)
