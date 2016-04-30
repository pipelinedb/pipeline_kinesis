#!/bin/bash

set -e
set -u

rm -rf aws-sdk-cpp
#AWS_REPO=https://github.com/aws/aws-sdk-cpp

AWS_REPO=$HOME/tmp/aws-sdk-cpp
VERSION=0.10.9

git clone $AWS_REPO
cd aws-sdk-cpp
git checkout tags/$VERSION
mkdir build
cd build

cmake -DBUILD_ONLY=aws-cpp-sdk-kinesis -DCMAKE_BUILD_TYPE=Debug ../
make VERBOSE=1
sudo make install
