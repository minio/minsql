#!/bin/bash

set -e

mkdir -p "$HOME/bin"

export PATH=$HOME/bin:$PATH

if [ ! -f "$BOOST_ROOT/lib/libboost_graph.a" ]; then
    wget "http://downloads.sourceforge.net/project/boost/boost/1.$BOOST_VERSION_MINOR.0/boost_1_${BOOST_VERSION_MINOR}_0.tar.gz" -O /tmp/boost.tar.gz
    tar -xzf /tmp/boost.tar.gz
    cd "boost_1_${BOOST_VERSION_MINOR}_0"
    ./bootstrap.sh
    ./b2 -q -d=0 install -j 8 --prefix="$BOOST_ROOT" link=static
else
    echo "Using cached boost v1.${BOOST_VERSION_MINOR}_0 @ ${BOOST_ROOT}.";
fi

llvm_version_triple() {
    if [ "$1" = "3.8" ]; then
	echo "3.8.1"
    elif [ "$1" = "3.9" ]; then
	echo "3.9.1"
    elif [ "$1" = "4.0" ]; then
	echo "4.0.1"
    elif [ "$1" = "5.0" ]; then
	echo "5.0.2"
    elif [ "$1" = "6.0" ]; then
	echo "6.0.1"
    fi
}

llvm_download() {
    LLVM_VERSION_TRIPLE=$(llvm_version_triple "${LLVM_VERSION}")
    export LLVM_VERSION_TRIPLE
    export LLVM=clang+llvm-${LLVM_VERSION_TRIPLE}-x86_64-$1

    wget "http://llvm.org/releases/${LLVM_VERSION_TRIPLE}/${LLVM}.tar.xz"
    mkdir -p "$LLVM_ROOT"
    tar -xf "${LLVM}.tar.xz" -C "$LLVM_ROOT" --strip-components=1

    echo "LLVM downloaded @ ${LLVM_ROOT}";
}

if [ ! -f "$LLVM_ROOT/bin/llvm-config" ]; then
    echo "Downloading LLVM ${LLVM_VERSION} ...";

    llvm_download linux-gnu-ubuntu-16.04
else
    echo "Using cached LLVM ${LLVM_VERSION} @ ${LLVM_ROOT}.";
fi
