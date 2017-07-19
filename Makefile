INSTALLDIR=/usr/local

build:
	make -C src
	make -C binaries

install: build
	install -d $(INSTALLDIR)/include/mabain
	cp src/db.h $(INSTALLDIR)/include/mabain
	cp src/mb_data.h $(INSTALLDIR)/include/mabain
	cp src/mabain_consts.h $(INSTALLDIR)/include/mabain
	cp src/lock.h $(INSTALLDIR)/include/mabain
	cp src/error.h $(INSTALLDIR)/include/mabain
	cp src/libmabain.so $(INSTALLDIR)/lib
	cp binaries/mbc $(INSTALLDIR)/bin

uninstall:
	rm -rf $(INSTALLDIR)/include/mabain
	rm $(INSTALLDIR)/lib/libmabain.so
	rm $(INSTALLDIR)/bin/mbc

clean:
	make -C src clean
	make -C binaries clean
