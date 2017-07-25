ifndef MABAIN_INSTALL_DIR
MABAIN_INSTALL_DIR=/usr/local
endif

all: db_comp

CPP=g++

# -DLEVEL_DB for leveldb test
# -DKYOTO_CABINET kyotocabinet test
# otherwise mabain test
CFLAGS = -O2 -g -std=c++11 -Wall -I/usr/local/include -I/home/cdeng/leveldb/include -I$(MABAIN_INSTALL_DIR)/include
#CFLAGS += -DLEVEL_DB
#CFLAGS += -DKYOTO_CABINET

LDFLAGS = -lpthread -lcrypto -L$(MABAIN_INSTALL_DIR)/lib -lleveldb -lkyotocabinet -lmabain

db_comp: db_comp.cpp
	$(CPP) $(CFLAGS) -o db_comp db_comp.cpp $(LDFLAGS)

clean:
	rm db_comp
