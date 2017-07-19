CPP=g++

all: mbc

CFLAGS  = -I. -I.. -Wall -Werror -g -O2 -c -std=c++11
LDFLAGS = -lpthread -lreadline -lncurses -L.. -lmabain

mbc: mbc.cpp
	$(CPP) $(CFLAGS) mbc.cpp
	$(CPP) mbc.o -o mbc $(LDFLAGS)

clean:
	rm *.o mbc
