#!/bin/bash

set -e
set -u

# build script for aws cpp library
# NOTE: If you want to make this faster, clone the remote repo to somewhere
# local, and override AWS_REPO, like below
# AWS_REPO=$HOME/tmp/aws-sdk-cpp

rm -rf aws-sdk-cpp
AWS_REPO=https://github.com/aws/aws-sdk-cpp
VERSION=0.10.9

git clone $AWS_REPO
cd aws-sdk-cpp
git checkout tags/$VERSION
mkdir build
cd build

cmake -DBUILD_ONLY=aws-cpp-sdk-kinesis -DCMAKE_BUILD_TYPE=Debug ../
make VERBOSE=1
sudo make install
