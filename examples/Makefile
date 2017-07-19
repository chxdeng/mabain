CPP=g++

all: mb_insert_test mb_lookup_test mb_longest_prefix_test \
	mb_remove_test mb_iterator_test

MABAIN_INSTALLDIR=/usr/local

CFLAGS  = -I. -I$(MABAIN_INSTALLDIR)/include -I../io -Wall -Werror -g -O2 -c -std=c++11
LDFLAGS = -lpthread -lcrypto -L$(MABAIN_INSTALLDIR)/lib -lmabain


mb_insert_test: mb_insert_test.cpp
	$(CPP) $(CFLAGS) mb_insert_test.cpp
	$(CPP) mb_insert_test.o -o mb_insert_test $(LDFLAGS)
mb_lookup_test: mb_lookup_test.cpp
	$(CPP) $(CFLAGS) mb_lookup_test.cpp
	$(CPP) mb_lookup_test.o -o mb_lookup_test $(LDFLAGS)
mb_longest_prefix_test: mb_longest_prefix_test.cpp
	$(CPP) $(CFLAGS) mb_longest_prefix_test.cpp
	$(CPP) mb_longest_prefix_test.o -o mb_longest_prefix_test $(LDFLAGS)
mb_remove_test: mb_remove_test.cpp
	$(CPP) $(CFLAGS) mb_remove_test.cpp
	$(CPP) mb_remove_test.o -o mb_remove_test $(LDFLAGS)
mb_iterator_test: mb_iterator_test.cpp
	$(CPP) $(CFLAGS) mb_iterator_test.cpp
	$(CPP) mb_iterator_test.o -o mb_iterator_test $(LDFLAGS)

clean:
	rm ./*.o ./mb_*_test
