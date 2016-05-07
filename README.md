# pipeline_kinesis

PipelineDB extension for Amazon Kinesis support

To run the program, you must first build and install the aws-cpp-sdk:

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

Once that succeeds, you can build the kinesis extension.

```
make install
```

To test the extension, first create a kinesis stream:

```
aws kinesis create-stream --stream-name test --shard-count 4
```

To populate the stream:

```
for i in $(seq 1 100); do aws kinesis put-record --stream-name test --data foo$i --partition-key foo$i; done
```

To load the extension:

```
create extension pipeline\_kinesis;
```

Add an endpoint:

```
select kinesis_add_endpoint('endpoint', 'us-west-2');
```

Create a pipeline stream to insert into:

```
CREATE STREAM foo_stream (payload text);
CREATE continuous view foo_view as select payload, count(*) from foo_stream group by payload;
```

Start ingestion:

```
select kinesis_consume_begin_sr('ep', 'test', 'foo_stream');
```

If all goes well, your view should have some data in it:

```
pipeline=# select * from foo_view;
 payload | count 
---------+-------
 foo5    |    11
 foo3    |    11
 foo2    |    11
 foo7    |    11
 foo9    |    11
 foo10   |    11
 ...
```
