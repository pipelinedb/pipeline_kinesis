# pipeline_kinesis

PipelineDB extension for Amazon Kinesis support

At present, this consists of a C wrapper for the aws-cpp-sdk, and a test driver program.

To run the program, you must first build and install the aws-cpp-sdk:

```
```
git clone https://github.com/aws/aws-sdk-cpp
cd aws-sdk-cpp
git checkout tags/0.10.9
mkdir build
cd build

cmake -DBUILD_ONLY=aws-cpp-sdk-kinesis -DCMAKE_BUILD_TYPE=Debug ../
make VERBOSE=1
sudo make install
cd /usr/lib
sudo ln -s /usr/local/lib/linux/intel64/Debug/libaws-cpp-sdk-core.so
sudo ln -s /usr/local/lib/linux/intel64/Debug/libaws-cpp-sdk-kinesis.so 
```

Once that succeeds, you can build the test program ```pipeline_kinesis```:

```
make
```

The test program expects a kinesis stream called test, with one shard.

To create the stream:

```
aws kinesis create-stream --stream-name test --shard-count 1
```

To populate the stream:

```
for i in $(seq 1 10); do aws kinesis put-record --stream-name test --data foo$i --partition-key foo$i; done
```

To run the consumer:

```
./pipeline_kinesis
```

If all goes well, you should see something like this:

```
1462215167.201261 request time 0.181243
1462215167.201310 producer pushed 41
1462215167.201323 consumer got 41 behind 0
1462215167.201356 rec pkey foo1 data foo1 seq 49561592855568525463209091806505879893624243491426009090
1462215167.201377 rec pkey foo2 data foo2 seq 49561592855568525463209091806507088819443858120600715266
1462215167.201391 rec pkey foo3 data foo3 seq 49561592855568525463209091806508297745263472887214374914
...
```
