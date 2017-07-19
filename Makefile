CPP=g++
TARGET=libmabain.a

CFLAGS  = -I. -I.. -Iutil -Wall -Werror -c -Wwrite-strings -Wsign-compare -Wcast-align -Wformat-security -fdiagnostics-show-option
CFLAGS += -g -ggdb -O2 -std=c++11
CFLAGS += -D__SHM_LOCK__ -D__LOCK_FREE__

SOURCES = $(wildcard *.cpp) $(wildcard util/*.cpp)
HEADERS = $(wildcard *.h) $(wildcard util/*.h)
OBJECTS = $(SOURCES:.cpp=.o)

all: $(TARGET)

$(TARGET):$(OBJECTS) $(HEADERS)
	ar rcs libmabain.a $(OBJECTS)

.cpp.o: $(HEADERS) $(SOURCES)
	$(CPP) $(CFLAGS) $< -o $@

libmabain.a: $(OBJECTS) $(HEADERS)

build:
	make; \
	cd unittest; \
	make; \
	cd ../examples; \
	make; \
	cd ../binaries; \
	make; \
	cd ..

clean:
	rm *.o *.a util/*.o; \
	cd unittest; make clean; \
	cd ../binaries; make clean; \
	cd ..
