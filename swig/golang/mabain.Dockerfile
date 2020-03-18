FROM ubuntu:bionic-20190515

ENV MABAIN_VERSION=1.1.0
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
ENV BUILD_OUTPUT=/target

ENV GOPATH /go
ENV PATH /usr/local/go/bin:$GOPATH/bin:$PATH
ENV GOROOT_BOOTSTRAP /usr/local/gobootstrap
ENV GO_BOOTSTRAP_VERSION go1.13
ARG GO_VERSION=go1.13
ENV GO_VERSION ${GO_VERSION}

# install dependencies required for bulding mabain lib
RUN apt-get update && apt -y install \
    ${BUILD_DEPS} && \
    rm -rf /var/lib/apt/lists/*

# Get a bootstrap version of Go for building from source.
RUN curl -sSL https://dl.google.com/go/$GO_BOOTSTRAP_VERSION.linux-amd64.tar.gz -o /tmp/go.tar.gz
RUN curl -sSL https://dl.google.com/go/$GO_BOOTSTRAP_VERSION.linux-amd64.tar.gz.sha256 -o /tmp/go.tar.gz.sha256
RUN echo "$(cat /tmp/go.tar.gz.sha256) /tmp/go.tar.gz" | sha256sum -c -
RUN mkdir -p $GOROOT_BOOTSTRAP
RUN tar --strip=1 -C $GOROOT_BOOTSTRAP -vxzf /tmp/go.tar.gz

# Fetch Go source for tag $GO_VERSION.
RUN git clone --depth=1 --branch=$GO_VERSION https://go.googlesource.com/go /usr/local/go
# Build the Go toolchain.
RUN cd /usr/local/go/src && GOOS=nacl GOARCH=amd64p32 ./make.bash --no-clean


# Download mabain source
RUN wget -O mabain.zip "https://github.com/chxdeng/mabain/archive/${MABAIN_VERSION}.zip" \
    && unzip -q -o mabain.zip -d ${MABAIN_SRC} \
    && rm -f mabain.zip

COPY ./ ${MABAIN_SRC}/

# build Mabain
RUN  mkdir $BUILD_OUTPUT; \
     env >> $BUILD_OUTPUT/env.txt; \
     apt-get update -qq; \
     cd $MABAIN_SRC/mabain-${MABAIN_VERSION}; \
     make build 2>&1 | tee -a $BUILD_OUTPUT/build_log.txt; \
     checkinstall -y -d0 --pkgname libmabain --pkgversion ${MABAIN_VERSION} --backup=no --strip=no --stripso=no --install=no --pakdir $BUILD_OUTPUT  2>&1 | tee -a $BUILD_OUTPUT/build_log.txt; \
     chmod -R 777 $BUILD_OUTPUT/ $MABAIN_SRC/;

RUN  apt-get update && apt -y install $BUILD_OUTPUT/libmabain_${MABAIN_VERSION}-${MABAIN_RELEASE}_amd64.deb

# Build & Install Go test code
# RUN cd ${MABAIN_SRC}/; go build -x -ldflags '-w -extldflags "-lmabain"'
RUN cd ${MABAIN_SRC}/; go install -x -i -ldflags '-w -extldflags "-lmabain"'

#Build and run UT
RUN cd ${MABAIN_SRC}/; go test -v

