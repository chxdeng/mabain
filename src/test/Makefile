CPP=g++

all: mb_test

CFLAGS  = -I. -I.. -I../util -Wall -Werror -g -O2 -c -std=c++11 -D__UNIT_TEST__
LDFLAGS = -lpthread

mb_test: mabain_test.cpp ../libmabain.so
	$(CPP) $(CFLAGS) mabain_test.cpp
	$(CPP) mabain_test.o -L../ -lmabain -o mb_test $(LDFLAGS)

clean:
	rm *.o mb_test
