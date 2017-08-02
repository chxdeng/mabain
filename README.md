# mabain

[![Build Status](https://travis-ci.org/spadalkar/mabain.svg?branch=master)](https://travis-ci.org/spadalkar/mabain)

## mabain: a key-value store library

Mabain is a light-weighted C++ library that can be used for generic
key-value store under GNU general public license, version 2.
It supports multi-thread and multi-process concurrency. Mabain can be
used for exact and common prefix key match.  Please see the examples in
examples directory.

#### Shared Memory and Memcap

Mabain stores all data on disk. However, user can specify how much data can be
mapped to shared memory using memcap option. Memcap can be specified for key/index
and value separately.

#### Multi-Thread/Multi-Process Concurrency

Full multi-thread/multi-process concurrency is supported. Mabain database
handler is also thread-safe. This means the the database handle can be used
in multiple reader threads once initialized. Concurrent insertion and queries are
supported internally. Programs using the library DO NOT need to perform any
locking on concurrent insertion and lookup in the reader/writer scenario.

## Build and Install Mabain Library

You should be able to build mabain on any modern linux machines. Please setup the mabain
install directory variable (MABAIN_INSTALL_DIR) first. Otherwise, mabain will be installed
in /usr/local. To build and install run following commands in mabain home 
###
    make build  
    make install  

## Mabain Command-Line Client

    The command-line client is in ./binaries directory.

    Usage: mbc -d mabain-directory [-im index-memcap] [-dm data-memcap] [-w]
	-d mabain databse directory
	-im index memcap
	-dm data memcap
	-w running in writer mode

## Examples

Please follow these steps to run the examples  

    1. Build and install mabain library

	make clean build install
	
    2. Build examples 
	
	cd ./examples
	make
	
    3. Create the temporary database directory for examples
	
	mkdir ./tmp_db_dir  
	./mb_insert_test  

## Caveats

    1. Only one writer is allowed. However, multiple readers can be running concurrently.  
    2. The longest key supported is 256 bytes.  
    3. The value/data size can not be bigger than 32767 bytes.  
    4. Using mabain on NFS currently is not supported or tested.  
    5. Please use -D__BIG__ENDIAN__ in compilation flag when using mabain on big endian machines.

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
