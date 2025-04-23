ifndef MABAIN_INSTALL_DIR
MABAIN_INSTALL_DIR=/usr/local
endif

all: db_comp_ldb db_comp_lmdb db_comp_mb db_comp_kc

CPP=/usr/bin/aarch64-wrs-linux/aarch64-wrs-linux-g++

CFLAGS = -O2 -g -std=c++17 -Wall -I/usr/local/include -I$(MABAIN_INSTALL_DIR)/include
CFLAGS += --sysroot=/opt/aarch64-wrs-linux-sysroot

LDFLAGS = -lpthread -lcrypto -L$(MABAIN_INSTALL_DIR)/lib -lleveldb -lkyotocabinet -lmabain -llmdb
LDFLAGS += --sysroot=/opt/aarch64-wrs-linux-sysroot

db_comp_ldb: db_comp.cpp
	$(CPP) $(CFLAGS) -DLEVEL_DB -o db_comp_ldb db_comp.cpp $(LDFLAGS)
db_comp_lmdb: db_comp.cpp
	$(CPP) $(CFLAGS) -DLMDB -o db_comp_lmdb db_comp.cpp $(LDFLAGS)
db_comp_mb: db_comp.cpp
	$(CPP) $(CFLAGS) -DMABAIN -o db_comp_mb db_comp.cpp $(LDFLAGS)
db_comp_kc: db_comp.cpp
	$(CPP) $(CFLAGS) -DKYOTO_CABINET -o db_comp_kc db_comp.cpp $(LDFLAGS)

build: all

clean:
	- rm db_comp_ldb db_comp_kc db_comp_lmdb db_comp_mb
