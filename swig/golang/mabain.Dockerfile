FROM ubuntu:bionic-20190515
FROM golang:1.12.5 as builder

ENV MABAIN_VERSION=1.3.0
ENV MABAIN_RELEASE=1

# Packages to install via APT for building.
ENV BUILD_DEPS=" \
    make \
    g++ \
    checkinstall \
    libreadline-dev \
    libncurses5-dev \
    libncursesw5-dev \
    libpcre3 libpcre3-dev \
    git \
    unzip \
    curl \
    wget \
    vim \
    swig \
    "

ENV MABAIN_SRC=/mabain_src
ENV MABAIN_GO=/go/src/mabain
ENV BUILD_OUTPUT=/target

# install dependencies required for bulding mabain lib
RUN apt-get update && apt -y install \
    ${BUILD_DEPS} && \
    rm -rf /var/lib/apt/lists/*

# Download mabain source
RUN wget -O mabain.zip "https://github.com/chxdeng/mabain/archive/master.zip" \
    && unzip -q -o mabain.zip -d ${MABAIN_SRC} \
    && rm -f mabain.zip

COPY ./ ${MABAIN_GO}/

# build Mabain
RUN  mkdir $BUILD_OUTPUT; \
     env >> $BUILD_OUTPUT/env.txt; \
     apt-get update -qq; \
     cd $MABAIN_SRC/mabain-master; \
     make build 2>&1 | tee -a $BUILD_OUTPUT/build_log.txt; \
     checkinstall -y -d0 --pkgname libmabain --pkgversion ${MABAIN_VERSION} --backup=no --strip=no --stripso=no --install=no --pakdir $BUILD_OUTPUT  2>&1 | tee -a $BUILD_OUTPUT/build_log.txt; \
     chmod -R 777 $BUILD_OUTPUT/ $MABAIN_SRC/;

RUN  apt-get update && apt -y install $BUILD_OUTPUT/libmabain_${MABAIN_VERSION}-${MABAIN_RELEASE}_amd64.deb

# Build & Install Go test code
# RUN cd ${MABAIN_SRC}/; go build -x -ldflags '-w -extldflags "-lmabain"'
RUN ldconfig -v; mkdir -p /var/tmp/mabain_test

ENV CGO_LDFLAGS=-lmabain

RUN cd ${MABAIN_GO}/; go build -i

#Build and run UT
RUN cd ${MABAIN_GO}/; go test -v

