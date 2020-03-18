CC=g++

ifeq ($(DEBUG),yes)
	CXXFLAGS=-Wall -g
	LDFLAGS=-Wall -g
else
	CXXFLAGS=-Wall
	LDFLAGS=-Wall
endif

INCPATH=.
SRCPATH=.
OBJPATH=.
LIBPATH=.
BINPATH=.

MABAINPATH=/usr/local/include/mabain
LDFLAGS = -L/usr/local/lib -lmabain

OBJS=$(OBJPATH)/mabain.o $(OBJPATH)/mabain_wrap.o
OUT=$(LIBPATH)/libMabainLib.so

INCLUDES=-I $(MABAINPATH) -I ./$(INCPATH)

default: $(OUT)

$(OUT): $(OBJS)
	$(CC) $(LDFLAGS) -shared -o $@ $^

mabain_wrap.o: mabain_wrap.cxx mabain_c_interface.h
	$(CC) $(CXXFLAGS) $(INCLUDES) -fpic -c $< -o $@

mabain.o: mabain_c_interface.cpp mabain_c_interface.h
	$(CC) $(CXXFLAGS) $(INCLUDES) -fpic -c $< -o $@


mabain_wrap.cxx:
	swig -Wall -go -c++ -intgosize 64 -soname libMabainLib.so mabain.swigcxx

.PHONY: clean cleanall

clean:
	rm -f $(OBJPATH)/*.o

prepare:
	rm -f *.so
	rm -f *.cxx

cleanall: clean prepare
	rm -f $(OUT)

build:
	@echo "Building bindings..."
	go build -x
	
install:
	@echo "Installing go package..."
	go install
