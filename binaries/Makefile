CPP=g++

all: mbc

CFLAGS  = -I. -I../src -Wall -Werror -g -O2 -c -std=c++11
LDFLAGS = -lpthread -lreadline -lncurses -L../src -lmabain

mbc: mbc.cpp
	$(CPP) $(CFLAGS) mbc.cpp
	$(CPP) mbc.o -o mbc $(LDFLAGS)

clean:
	-rm -f *.o mbc
