CPP=g++

all: mbc

CFLAGS  = -I. -I../src -I../src/util -Wall -Werror -g -O2 -c -std=c++11
LDFLAGS = -lpthread -lreadline -lncurses -L../src -lmabain

mbc: mbc.cpp expr_parser.h expr_parser.cpp hexbin.h hexbin.cpp
	$(CPP) $(CFLAGS) mbc.cpp
	$(CPP) $(CFLAGS) expr_parser.cpp
	$(CPP) $(CFLAGS) hexbin.cpp
	$(CPP) mbc.o expr_parser.o hexbin.o -o mbc $(LDFLAGS)

build: mbc

clean:
	-rm -f *.o mbc
