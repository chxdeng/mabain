TEST_BIN_NAME=test_mabain
TEST_COVRAGE_DIR=coverage
BUILD_DIR ?= ../../build

CPP=g++

CPPFLAGS = -I. -I.. -I../util -I/usr/include -Wall -Werror -g -O2 -std=c++17
CPPFLAGS += -fprofile-arcs -ftest-coverage -fPIC -D__SHM_QUEUE__
LDFLAGS  = -L/usr/lib64 -ljemalloc -lgtest -lcrypto -lpthread

TESTSOURCES=$(wildcard *.cpp)

all: unit-test

build: $(TESTSOURCES) $(BUILD_DIR)/lib/libmabain.so
	$(CPP) $(CPPFLAGS) -o $(TEST_BIN_NAME) $(TESTSOURCES) -L$(BUILD_DIR)/lib -lmabain $(LDFLAGS)

unit-test: clean build
	mkdir -p $(TEST_COVRAGE_DIR)
	LD_LIBRARY_PATH=$$LD_LIBRARY_PATH:$(BUILD_DIR)/lib ./$(TEST_BIN_NAME)
	- gcovr -r . --xml > $(TEST_COVRAGE_DIR)/coverage.xml
	- gcovr -r . --html --html-details -o $(TEST_COVRAGE_DIR)/coverage.html

clean:
	-rm -rf $(TEST_BIN_NAME)
	-rm -rf *.gcda *.gcno
	-rm -rf $(TEST_COVRAGE_DIR)
