#include <stdio.h>

#include <aws/kinesis/KinesisClient.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <unordered_set>
#include <aws/kinesis/model/GetRecordsResult.h>
#include <aws/kinesis/model/GetShardIteratorResult.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/kinesis/model/GetRecordsRequest.h>

#include <stdlib.h>
#include <unordered_set>
#include <functional>
#include <unistd.h>
#include <sys/time.h>

#include "util.h"

using namespace Aws::Auth;
using namespace Aws::Client;
using namespace Aws::Kinesis;
using namespace Aws::Kinesis::Model;

#include <thread>
#include "reader.h"
#include "conc_queue.h"

struct kinesis_consumer
{
	KinesisClient *kc;
	concurrent_queue<GetRecordsOutcome*> *cq;
	std::atomic<bool> keep_running;
	std::thread *thread;
};

// KinesisClient

//class AltRetryStrategy : public RetryStrategy
//{
//public:
//    AltRetryStrategy(long maxRetries = 10, long scaleFactor = 25) :
//        m_scaleFactor(scaleFactor), m_maxRetries(maxRetries)  
//    {}
//
//    bool ShouldRetry(const AWSError<CoreErrors>& error, long attemptedRetries)
//	{
//	}
//
//    long CalculateDelayBeforeNextRetry(const AWSError<CoreErrors>& error, long attemptedRetries) const
//	{
//	}
//
//private:
//    long m_scaleFactor;
//    long m_maxRetries;
//};

// DefaultRetryStrategy

static void consume_thread(void *k);

kinesis_consumer*
kinesis_consumer_create()
{
	std::string access_key_id;
	std::string access_key_secret;

	Aws::Utils::Logging::InitializeAWSLogging(Aws::MakeShared<Aws::Utils::Logging::DefaultLogSystem>("logging", Aws::Utils::Logging::LogLevel::Trace, "aws_sdk_"));

	get_credentials(access_key_id, access_key_secret);
	AWSCredentials creds(access_key_id.c_str(), access_key_secret.c_str());

	ClientConfiguration config;
	config.region = Aws::Region::US_WEST_2;
	config.endpointOverride = "localhost:4433";
	config.verifySSL = false;

	auto *kc = new KinesisClient(creds, config);
	auto *cq = new concurrent_queue<GetRecordsOutcome*>(1);
	return new kinesis_consumer{kc, cq, {true}, NULL};
}

void
kinesis_consumer_start(kinesis_consumer *kc)
{
	kc->thread = new std::thread(consume_thread, kc);
}

void
kinesis_consumer_stop(kinesis_consumer *kc)
{
	kc->keep_running = false;
	kc->thread->join();

	delete kc->thread;
}

void
kinesis_consumer_destroy(kinesis_consumer *kc)
{
	if (kc->keep_running)
		kinesis_consumer_stop(kc);

	delete kc->cq;
	delete kc->kc;
	delete kc;
}

static double
get_time()
{
    struct timeval tv;
    gettimeofday(&tv, 0);
    return tv.tv_sec + (tv.tv_usec / 1000000.0);
}

static void consume_thread(void *k)
{
	GetShardIteratorRequest request;

	request.SetStreamName("test");
	request.SetShardId("shardId-000000000000");
    request.SetShardIteratorType(ShardIteratorType::TRIM_HORIZON);

	kinesis_consumer *ks = (kinesis_consumer*)(k);
	KinesisClient &kc = *ks->kc;

	auto outcome = kc.GetShardIterator(request);

	// connecting to the stream and getting a shard iterator etc
	// should probably be blocking.

	if (!outcome.IsSuccess())
	{
		return;
//		std::cout << outcome.GetResult().GetShardIterator();
	}
	else
	{
//		std::cout << outcome.GetError().GetMessage();
	}

	double last_request_time = 0.0;

	Aws::String shard_iter = outcome.GetResult().GetShardIterator();
	GetRecordsRequest req;

	while (ks->keep_running)
	{
		req.SetShardIterator(shard_iter);
		req.SetLimit(1000);

		GetRecordsOutcome *new_rec_out = new GetRecordsOutcome();

		last_request_time = get_time();
		*new_rec_out = kc.GetRecords(req);

		if (new_rec_out->IsSuccess())
		{
			shard_iter = new_rec_out->GetResult().GetNextShardIterator();
			bool pushed = false;

			while (!pushed && ks->keep_running)
			{
				pushed = ks->cq->push_with_timeout(new_rec_out, 1000);
				printf("%3.6f producer pushed %d\n", last_request_time, pushed);
			}
		}
		else
		{
			printf("unsuccessful request\n");
		}

		if (ks->keep_running)
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

const kinesis_batch*
kinesis_consume(kinesis_consumer *kc, int timeout)
{
	GetRecordsOutcome *outcome = NULL;
	bool popped = kc->cq->pop_with_timeout(outcome, timeout);
	return popped ? (const kinesis_batch*) outcome : NULL;
}

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

