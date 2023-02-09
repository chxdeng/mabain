TEST_BIN_NAME=test_mabain
TEST_COVRAGE_DIR=coverage

CPP=g++

CPPFLAGS = -I. -I.. -I../util -I/usr/include -Wall -Werror -g -O2 -std=c++17
CPPFLAGS += -fprofile-arcs -ftest-coverage -fPIC -D__SHM_QUEUE__
LDFLAGS  = -L/usr/lib64 -lgtest -lcrypto -lpthread

TESTSOURCES=$(wildcard *.cpp)

all: unit-test

build: $(TESTSOURCES) ../libmabain.so
	$(CPP) $(CPPFLAGS) -o $(TEST_BIN_NAME) $(TESTSOURCES) -L.. -lmabain $(LDFLAGS)

unit-test: clean build
	mkdir -p $(TEST_COVRAGE_DIR)
	- LD_LIBRARY_PATH=$$LD_LIBRARY_PATH:../ ./$(TEST_BIN_NAME)
	- gcovr -r . --xml > $(TEST_COVRAGE_DIR)/coverage.xml
	- gcovr -r . --html --html-details -o $(TEST_COVRAGE_DIR)/coverage.html

clean:
	-rm -rf $(TEST_BIN_NAME)
	-rm -rf *.gcda *.gcno
	-rm -rf $(TEST_COVRAGE_DIR)
