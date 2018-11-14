# mabain

[![Build Status](https://travis-ci.org/chxdeng/mabain.svg?branch=master)](https://travis-ci.org/chxdeng/mabain)

[![Coverage Status](https://coveralls.io/repos/github/chxdeng/mabain/badge.svg)](https://coveralls.io/github/chxdeng/mabain)

## Mabain: a key-value store library

Mabain is a light-weighted C++ library that provides a generic key-value store
based on a radix tree implementation. It supports multi-thread and multi-process
concurrency *(see Caveats below)*. Mabain can be used for exact and common
prefix key match. Please see the examples in the `examples` directory.

### Shared Memory and Memcap

Mabain stores all data on disk. However, you can specify how much data can be
mapped to shared memory using the memcap option. The memcap can be specified for
the key space and the value space. For example see the `-km` and `-dm` options to
the Mabain command line client below.

### Multi-Thread/Multi-Process Concurrency

Full multi-thread/multi-process concurrency is supported. Concurrent insertion
and queries are supported internally using a lock-free mechanism. Programs using
the library DO NOT need to perform any locking on concurrent insertion and
lookup in the multi-thread or multi-process reader/writer scenario. *(see
Caveats below)*

### Multi-Thread/Multi-Process Insertion/Update

When using mabain only one writer is allowed. However, all reader threads/processes can
perform DB insertion/update using asynchronous queue in shared memory if the writer
has ASYNC_WRITER_MODE specified in the option when opening the DB. In this case, a separate
thread in the writer process is started for internal DB operations. All DB writing
operations are performed sequentially in this thread.

## Build and Install Mabain Library

We now have two different build options. First is the traditional "Native
Build", and *experimentally* a Docker build based on the very slim [Alpine
Linux](https://wiki.alpinelinux.org/wiki/Docker) and
[MSCL](https://www.musl-libc.org/) instead of glibc++.

### Native Build & Install

#### Build/Runtime Dependencies
1. [GNU ncurses](https://www.gnu.org/software/ncurses/)
2. [GNU readline](https://www.gnu.org/software/ncurses/)
3. [GLIBC](https://www.gnu.org/software/libc/)
4. [g++ Compiler that supports C++11](https://gcc.gnu.org/)

You should be able to build Mabain on any modern linux machines. To build and
install, ensure you meet the dependency requirements called out above run
following command in the top level of your git clone:

```
make build
```

#### (Optional) - Run the unit tests

If you would like to run the unit tests you will need some additional dependencies:

##### Unit Test dependencies
1. [Google Test](https://github.com/google/googletest)
2. [OpenSSL](https://www.openssl.org/)
3. [gcovr & gcov](https://github.com/gcovr/gcovr)
4. [Gtest Tap Listener](https://github.com/kinow/gtest-tap-listener/)

The command :

```
make unit-test
```
### Install Mabain

By default, Mabain will atttempt to install into `/usr/local/`. If you would
like to override this behavior define the `MABAIN_INSTALL_DIR` variable before
running the following command:

```
make install
```

### Docker Build

You can skip the previous build step if you would like to just build for a
docker environment. From the top level of your repository clone just run:

    make docker

This will kick off a two stage build process. We will spin up a build container
with all the build time dependencies. Once the build is complete, a thin
container image is build with just the run-time dependencies.

This image is intended to serve as a base layer for an application that would
like to leverage Mabain. However, we can certainly validate that the docker
container built properly.

```
docker run -it --rm -v$(pwd)/data:/data chxdeng/mabain /usr/local/bin/mbc -d /data -w
```

This should drop you into the mbc command-line client. It should look something
like this:

```
mabain 1.1.0 shell
database directory: /data/
>>
```

We expose a VOLUME from the image `/data` where you should bind mount persistent
storage. In the above command I have a `data` sub-directory in my current
directory that I mount to `/data`. This way, my database is created in the host
filesystem and persists between instantiation of containers based on this image.

## Mabain Command-Line Client

The command-line client source is in the `binaries` directory. It is installed
into $MABAIN_INSTALL_DIR/bin via `make install`.

```
Usage: mbc -d mabain-directory [-im index-memcap] [-dm data-memcap] [-w] [-e query] [-s script-file]

-d   mabain databse directory
-im  index memcap
-dm  data memcap
-w   running in writer mode
-e   run query on command line
-s   run queries in a file
```

## Examples

Please follow these steps to run the examples  

1. Build and install mabain library
```
make clean build install
```

2. Build examples
```
cd ./examples
make
```
3. Create the temporary database directory for examples
```
mkdir ./tmp_dir  
./mb_insert_test  
```

## Caveats

* Only one writer is allowed. However, multiple readers can be running
  concurrently.  
* Mabain DB handle is not thread-safe. Each thread must have open its own DB
  instance when Using in multi-thread context.
* The longest key supported is 256 bytes.  
* The value/data size can not be bigger than 32767 bytes.  
* Using Mabain on network storage (NAS, SAN, NFS, SMB, etc..) has not been
  tested. Your mileage may vary  
* Please use `-D__BIG__ENDIAN__` in compilation flags when using Mabain on big
  endian machines.

## License

Copyright (C) 2017 Cisco Inc.  

This program is free software: you can redistribute it and/or  modify  
it under the terms of the GNU General Public License, version 2,  
as published by the Free Software Foundation.  

This program is distributed in the hope that it will be useful,  
but WITHOUT ANY WARRANTY; without even the implied warranty of  
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the  
GNU General Public License for more details.  

You should have received a copy of the GNU General Public License  
along with this program.  If not, see <http://www.gnu.org/licenses/>.

[![License: GPL
v2](https://img.shields.io/badge/License-GPL%20v2-blue.svg)](https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html)
