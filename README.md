# pipeline_kinesis

PipelineDB extension for Amazon Kinesis support.

## Requirements

To run the program, you must first build and install the `aws-cpp-sdk`:

```
git clone https://github.com/aws/aws-sdk-cpp
cd aws-sdk-cpp
git checkout tags/0.10.9
mkdir build
cd build

cmake -DBUILD_ONLY=aws-cpp-sdk-kinesis -DCMAKE_BUILD_TYPE=Release ../
make VERBOSE=1
sudo make install
cd /usr/lib
sudo ln -s /usr/local/lib/linux/intel64/Release/libaws-cpp-sdk-core.so libaws-cpp-sdk-core.so
sudo ln -s /usr/local/lib/linux/intel64/Release/libaws-cpp-sdk-kinesis.so libaws-cpp-sdk-kinesis.so
sudo ldconfig -v
```

## Installing

Once that succeeds, you can build the kinesis extension.

```
make
make install
```

## Testing

To test the extension, first create a kinesis stream called 'test':

```
aws kinesis create-stream --stream-name test --shard-count 4
```

To populate the stream:

```
for i in $(seq 1 100); do aws kinesis put-record --stream-name test --data foo$i --partition-key foo$i; done
```

## Usage

To load the extension:

```
CREATE EXTENSION pipeline_kinesis;
```

Add an endpoint:

```
SELECT pipeline_kinesis.add_endpoint('endpoint', 'us-west-2');
```

Create a pipeline stream to insert into:

```
CREATE STREAM foo_stream (payload text);
CREATE CONTINUOUS VIEW foo_view as SELECT payload, count(*) FROM foo_stream GROUP BY payload;
```

Start ingestion:

```
SELECT pipeline_kinesis.consume_begin('endpoint', 'test', 'foo_stream');
```

If all goes well, your view should have some data in it:

```
pipeline=# SELECT * from foo_view;
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

To stop ingestion, run:

```
=# SELECT pipeline_kinesis.consume_end('endpoint', 'test', 'foo_stream');
 consume_end
-------------
 success
(1 row)
```

All existing consumers can be started or stopped at once by not passing any arguments to the previous functions:

```
=# SELECT pipeline_kinesis.consume_begin();
 consume_begin
---------------
 success
(1 row)

=# SELECT pipeline_kinesis.consume_end();
 consume_end
-------------
 success
(1 row)
```

To verify that ingestion is occurring, you can examine the kinesis metadata:

```
=# SELECT * FROM pipeline_kinesis.seqnums ORDER BY shard_id;

 consumer_id |       shard_id       |                          seqnum
-------------+----------------------+---------------------------------
           1 | shardId-000000000000 | 49561816305110483585113272267407
           1 | shardId-000000000001 | 49561816376495168965609796943461
           1 | shardId-000000000002 | 49561747276320427351761056013869
		   ...
```

Repeating this query a few times when the ingestion is happening should show the sequence numbers changing.
