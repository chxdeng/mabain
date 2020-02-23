ifndef MABAIN_INSTALL_DIR
MABAIN_INSTALL_DIR=/usr/local
endif

CPP = g++
all: _mabain.so

CFLAGS = -fPIC -c -O2 -I$(MABAIN_INSTALL_DIR)/include/mabain -I/usr/include/python2.7
LDFLAGS = -L$(MABAIN_INSTALL_DIR)/lib -lmabain

mabain_wrap.cxx: mabain.i
	swig -c++ -python mabain.i

mabain_c_interface.o: mabain_c_interface.cpp mabain_c_interface.h
	$(CPP) $(CFLAGS) mabain_c_interface.cpp

mabain_wrap.o: mabain_wrap.cxx
	$(CPP) $(CFLAGS) mabain_wrap.cxx

_mabain.so: mabain_wrap.o mabain_c_interface.o
	$(CPP) -shared mabain_wrap.o mabain_c_interface.o -o _mabain.so $(LDFLAGS)

clean:
	rm *.o _mabain.so mabain_wrap.cxx mabain.py mabain.pyc

