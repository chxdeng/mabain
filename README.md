# mabain

[![Build Status](https://travis-ci.org/spadalkar/mabain.svg?branch=master)](https://travis-ci.org/spadalkar/mabain)

[![Coverage Status](https://coveralls.io/repos/github/spadalkar/mabain/badge.svg)](https://coveralls.io/github/spadalkar/mabain)

## mabain: a key-value store library

Mabain is a light-weighted C++ library that can be used for generic
key-value store based on radix tree. It supports multi-thread and multi-process concurrency.
Mabain can be used for exact and common prefix key match. Please see the examples in
examples directory.

#### Shared Memory and Memcap

Mabain stores all data on disk. However, user can specify how much data can be
mapped to shared memory using memcap option. Memcap can be specified for key/index
and value separately.

#### Multi-Thread/Multi-Process Concurrency

Full multi-thread/multi-process concurrency is supported.  
Concurrent insertion and queries are supported internally using a lock-free mechanism.
Programs using the library DO NOT need to perform any locking on concurrent insertion
and lookup in the multi-thread or multi-process reader/writer scenario.

#### Library Dependencies
###
    1. GLIBC
    2. GNU readline library
    3. C++ compiler that supports C++11 standard.

## Build and Install Mabain Library

You should be able to build mabain on any modern linux machines. Please setup the mabain
install directory variable (MABAIN_INSTALL_DIR) first. Otherwise, mabain will be installed
in /usr/local. To build and install run following commands in mabain home 
###
    make build  
    make install  

## Mabain Command-Line Client

    The command-line client is in ./binaries directory.

    Usage: mbc -d mabain-directory [-im index-memcap] [-dm data-memcap] [-w] [-e query] [-s script-file]
	-d mabain databse directory
	-im index memcap
	-dm data memcap
	-w running in writer mode
	-e run query on command line
	-s run queries in a file 

## Examples

Please follow these steps to run the examples  

    1. Build and install mabain library

	make clean build install
	
    2. Build examples 
	
	cd ./examples
	make
	
    3. Create the temporary database directory for examples
	
	mkdir ./tmp_dir  
	./mb_insert_test  

## Caveats

    1. Only one writer is allowed. However, multiple readers can be running concurrently.  
    2. Mabain DB handle is not thread-safe. Each thread must have open its own DB instance when using in multi-thread context.
    3. The longest key supported is 256 bytes.  
    4. The value/data size can not be bigger than 32767 bytes.  
    5. Using mabain on NFS currently is not supported or tested.  
    6. Please use -D__BIG__ENDIAN__ in compilation flag when using mabain on big endian machines.

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

[![License: GPL v2](https://img.shields.io/badge/License-GPL%20v2-blue.svg)](https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html)
