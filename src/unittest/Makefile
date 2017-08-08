CPP=g++

all:unit-test

CFLAGS  = -I. -I.. -I../util -I/usr/include -Wall -Werror -g -O2 -std=c++11 -D__UNIT_TEST__
LDFLAGS = -lpthread -L/usr/lib64 -lgtest -lcrypto

TESTSOURCES=$(wildcard *.cpp)
unit-test: $(TESTSOURCES) ../libmabain.so
	$(CPP) $(CFLAGS) -o unit-test $(TESTSOURCES) -L.. -lmabain $(LDFLAGS)

clean:
	rm unit-test
