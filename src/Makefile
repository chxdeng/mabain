CPP=g++
TARGET=libmabain.so
all=$(TARGET)

CFLAGS  = -I. -I.. -Iutil -Wall -Werror -c -Wwrite-strings -Wsign-compare -Wcast-align -Wformat-security -fdiagnostics-show-option
CFLAGS += -g -ggdb -fPIC -O2 -std=c++11
CFLAGS += -D__SHM_LOCK__ -D__LOCK_FREE__
LDFLAGS = -lpthread

SOURCES = $(wildcard *.cpp) $(wildcard util/*.cpp)
HEADERS = $(wildcard *.h) $(wildcard util/*.h)
OBJECTS = $(SOURCES:.cpp=.o)

$(TARGET):$(OBJECTS) $(HEADERS)
	$(CPP) -shared -o $(TARGET) $(OBJECTS) $(LDFLAGS)

.cpp.o: $(HEADERS) $(SOURCES)
	$(CPP) $(CFLAGS) $< -o $@

uninstall:
	rm -rf $(INSTALLDIR)/include/mabain
	rm $(INSTALLDIR)/lib/$(TARGET)
	rm $(INSTALLDIR)/bin/mbc

clean:
	rm *.o util/*.o $(TARGET)
