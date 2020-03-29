all:target/mabainji.jar

CPP=g++
DOMAIN=swigi
PACKAGE_NAME=mabainji
MABAIN_JAVA_DIR=src/com/$(DOMAIN)/$(PACKAGE_NAME)
MABAIN_JAVA_PACKAGE=com.$(DOMAIN).$(PACKAGE_NAME)
JVM_INSTALL_DIR=/usr/lib/jvm/java-11-openjdk-amd64
JVM_INCLUDE_DIR=$(JVM_INSTALL_DIR)/include
JVM_LINUX_INCLUDE_DIR=$(JVM_INSTALL_DIR)/include/linux

TEST_DIR=/var/tmp/mabain_test

CFLAGS = -fPIC -c -O2 -I. -I$(JVM_INCLUDE_DIR) -I$(JVM_INCLUDE_DIR)/nspr -I$(JVM_LINUX_INCLUDE_DIR)
LDFLAGS = -shared -fPIC

mabain_wrap.cxx: mabain.i
	mkdir --parents src/com/$(DOMAIN)/$(PACKAGE_NAME)
	mkdir -p target
	swig -c++ -java -package $(MABAIN_JAVA_PACKAGE) -outdir $(MABAIN_JAVA_DIR) mabain.i
	
mabain_wrap.o: mabain_wrap.cxx
	$(CPP) $(CFLAGS) mabain_wrap.cxx
mabain_interface.o: mabain_interface.cpp
	$(CPP) $(CFLAGS) mabain_interface.cpp
libmabainJava.so: mabain_interface.o mabain_wrap.o
	$(CPP) $(LDFLAGS) mabain_interface.o mabain_wrap.o -o libmabainJava.so -L/usr/local/lib -lmabain

target/mabainji.jar: libmabainJava.so
	#javac $(MABAIN_JAVA_DIR)/*.java -Xlint:deprecation
	javac $(MABAIN_JAVA_DIR)/*.java
	jar cf target/mabainji.jar -C src com/$(DOMAIN)/$(PACKAGE_NAME)

test: target/mabainji.jar
	javac -cp target/mabainji.jar MabainTest.java
	mkdir -p $(TEST_DIR)
	rm -rf $(TEST_DIR)/_*
	java -cp .:target/mabainji.jar MabainTest

clean:
	rm -rf *.o libmabainJava.so mabain_wrap.cxx *.class $(MABAIN_JAVA_DIR)/* target/*

