/*-------------------------------------------------------------------------
 *
 * kinesis_consumer.cpp
 *	  Implementation for kinesis consumer C wrapper
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#include <aws/kinesis/KinesisClient.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/kinesis/model/GetRecordsResult.h>
#include <aws/kinesis/model/GetShardIteratorResult.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/core/client/RetryStrategy.h>
#include <aws/core/utils/logging/FormattedLogSystem.h>
#include <aws/core/client/DefaultRetryStrategy.h>

#include "kinesis_consumer.h"
#include "conc_queue.hpp"

#include <unordered_set>
#include <functional>
#include <thread>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/kinesis/model/DescribeStreamRequest.h>

using namespace Aws::Auth;
using namespace Aws::Client;
using namespace Aws::Kinesis;
using namespace Aws::Kinesis::Model;
using namespace Aws::Utils;

struct kinesis_consumer
{
	KinesisClient *client;
	concurrent_queue<GetRecordsOutcome*> *queue;
	std::atomic<bool> keep_running;
	std::thread *thread;
	Aws::String shard_iter;
};

void *logger_ctx = NULL;
void (*logger_fn) (void *ctx, const char *s) = 0;

// Override the default logging system to print to stderr
class AltLogSystem : public Logging::FormattedLogSystem
{
  public:
	AltLogSystem(Logging::LogLevel logLevel) : FormattedLogSystem(logLevel) {}
	virtual ~AltLogSystem() {}

  protected:
	virtual void ProcessFormattedStatement(Aws::String&& statement)
	{
		logger_fn(logger_ctx, statement.c_str());
	}
};

static double
get_time()
{
    struct timeval tv;
    gettimeofday(&tv, 0);
    return tv.tv_sec + (tv.tv_usec / 1000000.0);
}

static void consume_thread(kinesis_consumer *kc);

void
kinesis_set_logger(void *ctx, void (*l) (void *ctx, const char *s))
{
	logger_ctx = ctx;
	logger_fn = l;

	Aws::Utils::Logging::InitializeAWSLogging(Aws::MakeShared<AltLogSystem>("logging", Aws::Utils::Logging::LogLevel::Info));
}

kinesis_client * 
kinesis_client_create(const char *region,
					  const char *credfile,
					  const char *url)
{
	ClientConfiguration config;

	// XXX - unhardcode
	config.region = Aws::Region::US_WEST_2;
	config.retryStrategy = Aws::MakeShared<DefaultRetryStrategy>("kinesis_consumer", 5, 25);
	KinesisClient *kc = new KinesisClient(config);

	return (kinesis_client*)(kc);
}

kinesis_stream_metadata*
kinesis_client_create_stream_metadata(kinesis_client *client, 
		const char *stream)
{
	KinesisClient *kc = (KinesisClient*)(client);

	DescribeStreamRequest request;
	request.SetStreamName(stream);

	DescribeStreamOutcome *out = new DescribeStreamOutcome();
	*out = kc->DescribeStream(request);

	if (!out->IsSuccess())
	{
		delete out;
		return NULL;
	}

	return (kinesis_stream_metadata*) out;
}

int 
kinesis_stream_metadata_get_num_shards(kinesis_stream_metadata *meta)
{
	DescribeStreamOutcome *out = (DescribeStreamOutcome*)(meta);
	return out->GetResult().GetStreamDescription().GetShards().size();
}

const kinesis_shard_metadata *
kinesis_stream_metadata_get_shard(kinesis_stream_metadata *meta, int i)
{
	DescribeStreamOutcome *out = (DescribeStreamOutcome*)(meta);
	return (const kinesis_shard_metadata*) 
		&out->GetResult().GetStreamDescription().GetShards()[i];
}

const char * 
kinesis_shard_metadata_get_id(const kinesis_shard_metadata *meta)
{
	Shard *shard = (Shard*)(meta);

	return shard->GetShardId().c_str();
}

void
kinesis_client_destroy_stream_metadata(kinesis_stream_metadata *meta)
{
	delete (DescribeStreamOutcome*)(meta);
}

void
kinesis_client_destroy(kinesis_client *client)
{
	delete (KinesisClient *)(client);
}

// create the state for the consumer.
// note - this is a blocking call wrt obtaining the shard iterator.
// on error this returns NULL.
kinesis_consumer*
kinesis_consumer_create(kinesis_client *k, const char *stream, const char *shard)
{
	KinesisClient *kc = (KinesisClient*)(k);
	GetShardIteratorRequest request;

	// types
	//
	// at_seq
	// after_seq
	// trim_horizon
	// latest
	// timestamp
	//
	// default is to read from end of stream

	request.SetStreamName(stream);
	request.SetShardId(shard);
	request.SetShardIteratorType(ShardIteratorType::TRIM_HORIZON);

//	request.SetStartingSequenceNumber("49561655339045264437426724657642549718145953073006116866");
//	fprintf(stderr, "shard id %s\n", shard);
//	AWS_LOG_INFO("consumer", "shard id %s", shard);
//	printf("shard id %s\n", shard);
//	request.SetStreamName("test");
//	request.SetShardId("shardId-000000000000");
//	request.SetShardIteratorType(ShardIteratorType::TRIM_HORIZON);

	auto outcome = kc->GetShardIterator(request);

	if (!outcome.IsSuccess())
	{
		return NULL;
	}

	Aws::String shard_iter = outcome.GetResult().GetShardIterator();
	AWS_LOG_INFO("derp", "shard iter %s", shard_iter.c_str()); 

	auto *cq = new concurrent_queue<GetRecordsOutcome*>(100);
	return new kinesis_consumer{kc, cq, {true}, NULL, shard_iter};
}

// create and start the consumer thread
void
kinesis_consumer_start(kinesis_consumer *kc)
{
	kc->thread = new std::thread(consume_thread, kc);
}

// stop, join, and delete the consumer thread
void
kinesis_consumer_stop(kinesis_consumer *kc)
{
	kc->keep_running = false;
	kc->thread->join();

	delete kc->thread;
}

// clean up consumer resources
void
kinesis_consumer_destroy(kinesis_consumer *kc)
{
	if (kc->keep_running)
		kinesis_consumer_stop(kc);

	delete kc->queue;
	delete kc->client;
	delete kc;
}

// consumer thread continuously loops requesting records until the shard
// is finished or it is signalled to stop (via kc->keep_running).
static void
consume_thread(kinesis_consumer *kc)
{
	KinesisClient &client = *kc->client;

	double last_request_time = 0.0;

	GetRecordsRequest req;

	while (kc->keep_running)
	{
		req.SetShardIterator(kc->shard_iter);
		req.SetLimit(1000);

		GetRecordsOutcome *new_rec_out = new GetRecordsOutcome();

		last_request_time = get_time();
		*new_rec_out = client.GetRecords(req);

		double now = get_time();

		printf("%3.6f request time %3.6f\n", now, now - last_request_time);

		if (new_rec_out->IsSuccess())
		{
			kc->shard_iter = new_rec_out->GetResult().GetNextShardIterator();

			AWS_LOG_INFO("derp", "shard iter %s", kc->shard_iter.c_str()); 

			bool pushed = false;

			int num_rec = kinesis_batch_get_size((kinesis_batch*) new_rec_out);

			while (!pushed && kc->keep_running)
			{
				pushed = kc->queue->push_with_timeout(new_rec_out, 1000);

				if (pushed)
					printf("%3.6f producer pushed %d\n", get_time(), num_rec);
			}
		}
		else
		{
			printf("unsuccessful request\n");
		}

		// Throttling logic
		if (kc->keep_running)
		{
			double time_now = get_time();
			double delta = time_now - last_request_time;

			double sleep_time = 0.25 - delta;

			if (sleep_time > 0.0)
				usleep(sleep_time * 1000000.0);
		}
	}

	printf("consume thread stopping\n");
}

// API for external consumer to pop a batch from the queue
const kinesis_batch*
kinesis_consume(kinesis_consumer *kc, int timeout)
{
	GetRecordsOutcome *outcome = NULL;
	bool popped = kc->queue->pop_with_timeout(outcome, timeout);
	return popped ? (const kinesis_batch*) outcome : NULL;
}

// C -> C++ API wrappers
int64_t
kinesis_batch_get_millis_behind_latest(const kinesis_batch *rb)
{
	return ((const GetRecordsOutcome*)rb)->GetResult().GetMillisBehindLatest();
}

int
kinesis_batch_get_size(const kinesis_batch *rb)
{
	return ((const GetRecordsOutcome*)rb)->GetResult().GetRecords().size();
}

const kinesis_record*
kinesis_batch_get_record(const kinesis_batch *rb, int i)
{
	auto o = (const GetRecordsOutcome*) rb;
	auto r = &(o->GetResult().GetRecords()[i]);

	return (const kinesis_record*) r;
}

void
kinesis_batch_destroy(const kinesis_batch *rb)
{
	delete (const GetRecordsOutcome*)(rb);
}

const char*
kinesis_record_get_sequence_number(const kinesis_record *rec)
{
	return ((Record*)rec)->GetSequenceNumber().c_str();
}

const char*
kinesis_record_get_partition_key(const kinesis_record *rec)
{
	return ((Record*)rec)->GetPartitionKey().c_str();
}

double
kinesis_record_get_arrival_time(const kinesis_record *rec)
{
	return ((Record*)rec)->GetApproximateArrivalTimestamp().ComputeCurrentTimestampInAmazonFormat();
}

int
kinesis_record_get_data_size(const kinesis_record *rec)
{
	return ((Record*)rec)->GetData().GetLength();
}

const uint8_t*
kinesis_record_get_data(const kinesis_record *rec)
{
	return ((Record*)rec)->GetData().GetUnderlyingData();
}

