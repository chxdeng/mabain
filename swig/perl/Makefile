ifndef MABAIN_INSTALL_DIR
MABAIN_INSTALL_DIR=/usr/local
endif

CPP = g++
all: mabain.so

CFLAGS = -fPIC -c -O2 -I$(MABAIN_INSTALL_DIR)/include/mabain -I/usr/lib/x86_64-linux-gnu/perl/5.26.1/CORE
LDFLAGS = -L$(MABAIN_INSTALL_DIR)/lib -lmabain

mabain_wrap.cxx: mabain.i
	swig -c++ -perl5 -noproxy mabain.i

mabain_c_interface.o: mabain_c_interface.cpp mabain_c_interface.h
	$(CPP) $(CFLAGS) mabain_c_interface.cpp

mabain_wrap.o: mabain_wrap.cxx
	$(CPP) $(CFLAGS) mabain_wrap.cxx

mabain.so: mabain_wrap.o mabain_c_interface.o
	$(CPP) -shared mabain_wrap.o mabain_c_interface.o -o mabain.so $(LDFLAGS)

clean:
	rm -rf *.o mabain.so mabain_wrap.cxx mabain.pm

