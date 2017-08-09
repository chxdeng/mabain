TEST_BIN_NAME=test_mabain

CPP=g++

CPPFLAGS = -I. -I.. -I../util -I/usr/include -Wall -Werror -g -O2 -std=c++11 -D__UNIT_TEST__
LDFLAGS  = -lpthread -L/usr/lib64 -lgtest -lcrypto

TESTSOURCES=$(wildcard *.cpp)

all:unit-test

unit-test: $(TESTSOURCES) ../libmabain.so
	$(CPP) $(CPPFLAGS) -o $(TEST_BIN_NAME) $(TESTSOURCES) -L.. -lmabain $(LDFLAGS)

clean:
	-rm -rf $(TEST_BIN_NAME)
