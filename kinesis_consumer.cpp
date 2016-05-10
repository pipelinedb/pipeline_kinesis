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
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/kinesis/model/DescribeStreamRequest.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/http/Scheme.h>

#include "kinesis_consumer.h"
#include "conc_queue.hpp"

#include <unordered_set>
#include <functional>
#include <thread>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>

using namespace Aws::Auth;
using namespace Aws::Client;
using namespace Aws::Kinesis;
using namespace Aws::Kinesis::Model;
using namespace Aws::Utils;

struct kinesis_consumer
{
	KinesisClient *client;
	concurrent_queue<GetRecordsOutcome *> *queue;
	std::atomic<bool> keep_running;
	std::thread *thread;
	Aws::String shard_iter;
	int batchsize;
};

void *logger_ctx = NULL;
kinesis_log_fn logger_fn = NULL;

// Override the default logging system to print to stderr
class AltLogSystem : public Logging::FormattedLogSystem
{
  public:
	AltLogSystem(Logging::LogLevel logLevel) : FormattedLogSystem(logLevel) {}
	virtual ~AltLogSystem() {}

	std::mutex mutex_;

  protected:
	virtual void ProcessFormattedStatement(Aws::String&& statement)
	{
		std::unique_lock<std::mutex> lock(mutex_);
		logger_fn(logger_ctx, statement.c_str(), statement.size());
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

static Aws::Utils::Logging::LogLevel
parse_level(const char *level)
{
	if (strcmp("off", level) == 0)
		return Aws::Utils::Logging::LogLevel::Off;

	if (strcmp("fatal", level) == 0)
		return Aws::Utils::Logging::LogLevel::Fatal;

	if (strcmp("error", level) == 0)
		return Aws::Utils::Logging::LogLevel::Error;

	if (strcmp("warn", level) == 0)
		return Aws::Utils::Logging::LogLevel::Warn;

	if (strcmp("info", level) == 0)
		return Aws::Utils::Logging::LogLevel::Info;

	if (strcmp("debug", level) == 0)
		return Aws::Utils::Logging::LogLevel::Debug;

	if (strcmp("trace", level) == 0)
		return Aws::Utils::Logging::LogLevel::Trace;

	// default to off

	return Aws::Utils::Logging::LogLevel::Off;
}

void
kinesis_set_logger(void *ctx, kinesis_log_fn fn, const char *level)
{
	logger_ctx = ctx;
	logger_fn = fn;

	Aws::Utils::Logging::InitializeAWSLogging(
			Aws::MakeShared<AltLogSystem>("logging", parse_level(level)));
}

static int
lookup_region(const char *r)
{
	if (strcmp(r, "us-east-1") == 0)
		return (int) Aws::Region::US_EAST_1;
	if (strcmp(r, "us-west-1") == 0)
		return (int) Aws::Region::US_WEST_1;
	if (strcmp(r, "us-west-2") == 0)
		return (int) Aws::Region::US_WEST_2;
	if (strcmp(r, "eu-west-1") == 0)
		return (int) Aws::Region::EU_WEST_1;
	if (strcmp(r, "eu-central-1") == 0)
		return (int) Aws::Region::EU_CENTRAL_1;
	if (strcmp(r, "ap-southeast-1") == 0)
		return (int) Aws::Region::AP_SOUTHEAST_1;
	if (strcmp(r, "ap-southeast-2") == 0)
		return (int) Aws::Region::AP_SOUTHEAST_2;
	if (strcmp(r, "ap-northeast-1") == 0)
		return (int) Aws::Region::AP_NORTHEAST_1;
	if (strcmp(r, "ap-northeast-2") == 0)
		return (int) Aws::Region::AP_NORTHEAST_2;
	if (strcmp(r, "sa-east-1") == 0)
		return (int) Aws::Region::SA_EAST_1;

	return -1;
}

static void
parse_url(ClientConfiguration &config,
		  const char *url)
{
	const char *iter = url;
	const char *tok = iter;

	while (*iter && *iter != ':') ++iter;

	Aws::String scheme(tok, iter);

	while (*iter && (*iter == ':' || *iter == '/')) ++iter;
	tok = iter;

	while (*iter) ++iter;

	Aws::String rest(tok, iter);

	config.scheme = Aws::Http::SchemeMapper::FromString(scheme.c_str());
	config.endpointOverride = rest;
}

kinesis_client * 
kinesis_client_create(const char *region,
					  const char *credfile,
					  const char *url)
{
	ClientConfiguration config;

	int r = lookup_region(region);

	if (r == -1)
		return NULL;

	config.region = (Aws::Region) r;
	config.retryStrategy = 
		Aws::MakeShared<DefaultRetryStrategy>("kinesis_consumer", 5, 25);

	if (credfile)
		setenv("AWS_SHARED_CREDENTIALS_FILE", credfile, 1);

	if (url)
		parse_url(config, url);

	KinesisClient *kc = new KinesisClient(config);
	return (kinesis_client *)(kc);
}

kinesis_stream_metadata *
kinesis_client_create_stream_metadata(kinesis_client *client, 
		const char *stream)
{
	KinesisClient *kc = (KinesisClient *)(client);

	DescribeStreamRequest request;
	request.SetStreamName(stream);

	DescribeStreamOutcome *out = new DescribeStreamOutcome();
	*out = kc->DescribeStream(request);

	if (!out->IsSuccess())
	{
		delete out;
		return NULL;
	}

	return (kinesis_stream_metadata *) out;
}

int 
kinesis_stream_metadata_get_num_shards(kinesis_stream_metadata *meta)
{
	DescribeStreamOutcome *out = (DescribeStreamOutcome *)(meta);
	return out->GetResult().GetStreamDescription().GetShards().size();
}

const kinesis_shard_metadata *
kinesis_stream_metadata_get_shard(kinesis_stream_metadata *meta, int i)
{
	DescribeStreamOutcome *out = (DescribeStreamOutcome *)(meta);
	return (const kinesis_shard_metadata *) 
		&out->GetResult().GetStreamDescription().GetShards()[i];
}

const char * 
kinesis_shard_metadata_get_id(const kinesis_shard_metadata *meta)
{
	Shard *shard = (Shard *)(meta);

	return shard->GetShardId().c_str();
}

void
kinesis_client_destroy_stream_metadata(kinesis_stream_metadata *meta)
{
	delete (DescribeStreamOutcome *)(meta);
}

void
kinesis_client_destroy(kinesis_client *client)
{
	delete (KinesisClient *)(client);
}

// parse a formatted seqnum such as 'after_sequence_number:324342423..' and
// set the appropriate options on a shard iterator request
static bool
parse_seqnum(GetShardIteratorRequest &request, const char *seqnum)
{
	Aws::Vector<Aws::String> tokens = StringUtils::Split(seqnum, ':');

	if (tokens.size() == 0)
		return false;

	if (tokens[0] == "after_sequence_number")
	{
		if (tokens.size() != 2)
			return false;

		request.SetShardIteratorType(ShardIteratorType::AFTER_SEQUENCE_NUMBER);
		request.SetStartingSequenceNumber(tokens[1]);
	}
	else if (tokens[0] == "trim_horizon")
	{
		if (tokens.size() != 1)
			return false;

		request.SetShardIteratorType(ShardIteratorType::TRIM_HORIZON);
	}
	else if (tokens[0] == "latest")
	{
		if (tokens.size() != 1)
			return false;
		request.SetShardIteratorType(ShardIteratorType::LATEST);
	}
	else
		return false;

	return true;
}

// create the state for the consumer.
// note - this is a blocking call wrt obtaining the shard iterator.
// on error this returns NULL.
kinesis_consumer *
kinesis_consumer_create(kinesis_client *k, 
		const char *stream, 
		const char *shard,
		const char *seqnum,
		int batchsize)
{
	KinesisClient *kc = (KinesisClient *)(k);
	GetShardIteratorRequest request;

	request.SetStreamName(stream);
	request.SetShardId(shard);

	bool ok = parse_seqnum(request, seqnum);

	if (!ok)
		AWS_LOG_ERROR("kinesis_consumer", "could not parse seq %s", seqnum);

	auto outcome = kc->GetShardIterator(request);

	if (!outcome.IsSuccess())
	{
		return NULL;
	}

	Aws::String shard_iter = outcome.GetResult().GetShardIterator();

	auto *cq = new concurrent_queue<GetRecordsOutcome *>(100);
	return new kinesis_consumer{kc, cq, {true}, NULL, shard_iter, batchsize};
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

	while (!kc->queue->unlocked_empty())
	{
		GetRecordsOutcome *popped;
		kc->queue->unlocked_pop(popped);
		delete popped;
	}

	delete kc->queue;
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
	int throttle = 0;

	while (kc->keep_running)
	{
		req.SetShardIterator(kc->shard_iter);
		req.SetLimit(kc->batchsize);

		GetRecordsOutcome *new_rec_out = new GetRecordsOutcome();

		last_request_time = get_time();
		*new_rec_out = client.GetRecords(req);

		if (new_rec_out->IsSuccess())
		{
			kc->shard_iter = new_rec_out->GetResult().GetNextShardIterator();
			bool pushed = false;

			while (!pushed && kc->keep_running)
				pushed = kc->queue->push_with_timeout(new_rec_out, 1000);

			throttle = 0;
		}
		else
		{
			auto &err = new_rec_out->GetError();

			if (err.GetErrorType() == 
					Aws::Kinesis::KinesisErrors::PROVISIONED_THROUGHPUT_EXCEEDED)
			{
				throttle++;
			}
			else
			{
				AWS_LOG_WARN("kinesis_consumer", "unrecoverable error %s %d", 
						err.GetMessage().c_str());
				break;
			}
		}

		// Throttling logic
		if (kc->keep_running)
		{
			double sleep_time = 0.0;

			if (!throttle)
			{
				// default behaviour try to stay at 4 reqs/sec
				double time_now = get_time();
				double delta = time_now - last_request_time;
				sleep_time = 0.25 - delta;
			}
			else
			{
				// linear backoff up to 5 secs
				sleep_time = std::min(throttle * 1.0, 5.0);
			}

			if (sleep_time > 0.0)
				usleep(sleep_time * 1000000.0);
		}
	}

	AWS_LOG_INFO("kinesis_consumer", "thread stopping");
}

// API for external consumer to pop a batch from the queue
const kinesis_batch *
kinesis_consume(kinesis_consumer *kc, int timeout)
{
	GetRecordsOutcome *outcome = NULL;
	bool popped = kc->queue->pop_with_timeout(outcome, timeout);
	return popped ? (const kinesis_batch *) outcome : NULL;
}

// C -> C++ API wrappers
int64_t
kinesis_batch_get_millis_behind_latest(const kinesis_batch *rb)
{
	return ((const GetRecordsOutcome *)rb)->GetResult().GetMillisBehindLatest();
}

int
kinesis_batch_get_size(const kinesis_batch *rb)
{
	return ((const GetRecordsOutcome *)rb)->GetResult().GetRecords().size();
}

const kinesis_record *
kinesis_batch_get_record(const kinesis_batch *rb, int i)
{
	auto o = (const GetRecordsOutcome *) rb;
	auto r = &(o->GetResult().GetRecords()[i]);

	return (const kinesis_record *) r;
}

void
kinesis_batch_destroy(const kinesis_batch *rb)
{
	delete (const GetRecordsOutcome *)(rb);
}

const char *
kinesis_record_get_sequence_number(const kinesis_record *rec)
{
	return ((Record *)rec)->GetSequenceNumber().c_str();
}

const char *
kinesis_record_get_partition_key(const kinesis_record *rec)
{
	return ((Record *)rec)->GetPartitionKey().c_str();
}

double
kinesis_record_get_arrival_time(const kinesis_record *rec)
{
	return ((Record *)rec)->GetApproximateArrivalTimestamp().ComputeCurrentTimestampInAmazonFormat();
}

int
kinesis_record_get_data_size(const kinesis_record *rec)
{
	return ((Record *)rec)->GetData().GetLength();
}

const uint8_t *
kinesis_record_get_data(const kinesis_record *rec)
{
	return ((Record *)rec)->GetData().GetUnderlyingData();
}

