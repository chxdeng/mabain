CPP=g++
TARGET=libmabain.so

INSTALLDIR=/usr/local

CFLAGS  = -I. -I.. -Iutil -Wall -Werror -c -Wwrite-strings -Wsign-compare -Wcast-align -Wformat-security -fdiagnostics-show-option
CFLAGS += -g -ggdb -fPIC -O2 -std=c++11
CFLAGS += -D__SHM_LOCK__ -D__LOCK_FREE__
LDFLAGS = -lpthread

SOURCES = $(wildcard *.cpp) $(wildcard util/*.cpp)
HEADERS = $(wildcard *.h) $(wildcard util/*.h)
OBJECTS = $(SOURCES:.cpp=.o)

all: $(TARGET)

$(TARGET):$(OBJECTS) $(HEADERS)
	$(CPP) -shared -o $(TARGET) $(OBJECTS) $(LDFLAGS)

.cpp.o: $(HEADERS) $(SOURCES)
	$(CPP) $(CFLAGS) $< -o $@

build:
	make
	cd binaries; make; cd ..

install:
	install -d $(INSTALLDIR)/include/mabain
	cp db.h $(INSTALLDIR)/include/mabain
	cp mb_data.h $(INSTALLDIR)/include/mabain
	cp mabain_consts.h $(INSTALLDIR)/include/mabain
	cp lock.h $(INSTALLDIR)/include/mabain
	cp error.h $(INSTALLDIR)/include/mabain
	cp $(TARGET) $(INSTALLDIR)/lib
	cp binaries/mbc $(INSTALLDIR)/bin

uninstall:
	rm -rf $(INSTALLDIR)/include/mabain
	rm $(INSTALLDIR)/lib/$(TARGET)
	rm $(INSTALLDIR)/bin/mbc

clean:
	rm *.o util/*.o $(TARGET)
	cd binaries; make clean; cd ..
