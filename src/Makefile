CPP=g++
TARGET=libmabain.so

all: $(TARGET)

CFLAGS  = -I. -I.. -Iutil -Wall -Werror -c -Wwrite-strings -Wsign-compare -Wcast-align -Wformat-security -fdiagnostics-show-option
CFLAGS += -g -ggdb -fPIC -O2 -std=c++17
CFLAGS += -D__LOCK_FREE__
LDFLAGS = -lpthread

SOURCES = $(wildcard *.cpp) $(wildcard util/*.cpp)
HEADERS = $(wildcard *.h) $(wildcard util/*.h)
OBJECTS = $(SOURCES:.cpp=.o)

$(TARGET):$(OBJECTS) $(HEADERS)
	$(CPP) -shared -o $(TARGET) $(OBJECTS) $(LDFLAGS)

.cpp.o: $(HEADERS) $(SOURCES)
	$(CPP) $(CFLAGS) $< -o $@

build: all

uninstall:
	-rm -rf $(INSTALLDIR)/include/mabain
	-rm -f $(INSTALLDIR)/lib/$(TARGET)
	-rm -f $(INSTALLDIR)/bin/mbc

clean:
	-rm -f *.o util/*.o $(TARGET)
	-make -C unittest clean

unit-test: all
	make -C unittest
