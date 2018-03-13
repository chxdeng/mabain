#mabain Makefile

ifndef MABAIN_INSTALL_DIR
MABAIN_INSTALL_DIR=/usr/local
endif

build:
	make -C src build
	make -C binaries build

install: build
	echo "mabain install directory: $(MABAIN_INSTALL_DIR)"
	mkdir -p $(MABAIN_INSTALL_DIR)/include/mabain
	cp src/db.h $(MABAIN_INSTALL_DIR)/include/mabain
	cp src/mb_data.h $(MABAIN_INSTALL_DIR)/include/mabain
	cp src/mabain_consts.h $(MABAIN_INSTALL_DIR)/include/mabain
	cp src/lock.h $(MABAIN_INSTALL_DIR)/include/mabain
	cp src/error.h $(MABAIN_INSTALL_DIR)/include/mabain

	mkdir -p $(MABAIN_INSTALL_DIR)/lib
	cp src/libmabain.so $(MABAIN_INSTALL_DIR)/lib

	mkdir -p $(MABAIN_INSTALL_DIR)/bin
	cp binaries/mbc $(MABAIN_INSTALL_DIR)/bin

uninstall:
	rm -rf $(MABAIN_INSTALL_DIR)/include/mabain
	rm -f $(MABAIN_INSTALL_DIR)/lib/libmabain.so
	rm -f $(MABAIN_INSTALL_DIR)/bin/mbc

clean:
	-make -C src clean
	-make -C binaries clean
	-make -C examples clean

distclean: clean
	-rm -rf doc/*
	-rm -f tags

index:
	-ctags -R *
	-doxygen doxygen.conf
	-echo "Generating documentation..."
	-echo "Use following index file : "
	-readlink -f doc/html/index.html
	-echo "Done with doxygen"

unit-test: build
	make -C src unit-test
