#include <stdio.h>

#include <aws/kinesis/KinesisClient.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/kinesis/model/GetRecordsResult.h>
#include <aws/kinesis/model/GetShardIteratorResult.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>

#include <stdlib.h>
#include <unordered_set>
#include <functional>
#include <unistd.h>
#include "util.h"
#include <aws/kinesis/model/GetRecordsRequest.h>

using namespace Aws::Auth;
using namespace Aws::Client;
using namespace Aws::Kinesis;
using namespace Aws::Kinesis::Model;

#include <thread>
#include "reader.h"
#include "conc_queue.h"

struct KinesisState
{
	KinesisClient *kc;
	concurrent_queue<void*> *cq;
	std::atomic<bool> keep_running;
	std::thread *thread;
};

void*
kinesis_create()
{
	std::string access_key_id;
	std::string access_key_secret;

//	Aws::Utils::Logging::InitializeAWSLogging(Aws::MakeShared<Aws::Utils::Logging::DefaultLogSystem>("logging", Aws::Utils::Logging::LogLevel::Trace, "aws_sdk_"));

	get_credentials(access_key_id, access_key_secret);
	AWSCredentials creds(access_key_id.c_str(), access_key_secret.c_str());

	ClientConfiguration config;
	config.region = Aws::Region::US_WEST_2;
//	config.endpointOverride = "localhost:4433";
	config.verifySSL = false;

	KinesisClient *kc = new KinesisClient(creds, config);
	concurrent_queue<void*> *cq = new concurrent_queue<void*>(1);
	return new KinesisState{kc, cq, {true}, NULL};
}

void
kinesis_destroy(void *k)
{
	KinesisState *ks = (KinesisState*)(k);

	if (ks->keep_running)
		kinesis_stop(ks);
}

void consume_thread(void *k)
{
	GetShardIteratorRequest request;

	request.SetStreamName("test");
	request.SetShardId("shardId-000000000000");
    request.SetShardIteratorType(ShardIteratorType::TRIM_HORIZON);

	KinesisState *ks = (KinesisState*)(k);
	KinesisClient &kc = *ks->kc;

	auto outcome = kc.GetShardIterator(request);

	if (outcome.IsSuccess())
	{
//		std::cout << outcome.GetResult().GetShardIterator();
	}
	else
	{
//		std::cout << outcome.GetError().GetMessage();
	}

	Aws::String shard_iter = outcome.GetResult().GetShardIterator();
	GetRecordsRequest req;

	while (ks->keep_running)
	{
		req.SetShardIterator(shard_iter);
		req.SetLimit(1000);

		GetRecordsOutcome *new_rec_out = new GetRecordsOutcome();
		*new_rec_out = kc.GetRecords(req);

		if (new_rec_out->IsSuccess())
		{
			shard_iter = new_rec_out->GetResult().GetNextShardIterator();
			bool pushed = false;

			while (!pushed && ks->keep_running)
			{
				pushed = ks->cq->push_with_timeout(new_rec_out, 1000);
				std::cout << "producer pushed " << pushed << std::endl;
			}
		}
		else
		{
			std::cout << "not success " << std::endl;
		}

		if (ks->keep_running)
			usleep(250000);
	}

	std::cout << "consume thread stopping" << std::endl;
}

void* 
kinesis_consume(void *k, int timeout)
{
	KinesisState *ks = (KinesisState*)(k);
	void *rec = 0;

	bool popped = ks->cq->pop_with_timeout(rec, timeout);
	return popped ? rec : NULL;
}

void
kinesis_start(void *k)
{
	KinesisState *ks = (KinesisState*)(k);
	ks->thread = new std::thread(consume_thread, k);
//	ks->thread->detach();
}

void
kinesis_stop(void *k)
{
	KinesisState *ks = (KinesisState*)(k);

	ks->keep_running = false;
	ks->thread->join();
}

int64_t
record_batch_get_millis_behind_latest(void *rb)
{
	return ((GetRecordsOutcome*)rb)->GetResult().GetMillisBehindLatest();
}

int
record_batch_get_size(void *rb)
{
	return ((GetRecordsOutcome*)rb)->GetResult().GetRecords().size();
}

const void*
record_batch_get_record(void *rb, int i)
{
	return &((GetRecordsOutcome*)rb)->GetResult().GetRecords()[i];
}

const char*
record_get_sequence_number(const void *rec)
{
	return ((Record*)rec)->GetSequenceNumber().c_str();
}

const char*
record_get_partition_key(const void *rec)
{
	return ((Record*)rec)->GetPartitionKey().c_str();
}

double
record_get_arrival_time(const void *rec)
{
	return ((Record*)rec)->GetApproximateArrivalTimestamp().ComputeCurrentTimestampInAmazonFormat();
}

int
record_get_data_size(void *rec)
{
	return ((Record*)rec)->GetData().GetLength();
}

const uint8_t*
record_get_data(void *rec)
{
	return ((Record*)rec)->GetData().GetUnderlyingData();
}

void
record_batch_destroy(void *rb)
{
	delete (GetRecordsOutcome*)(rb);
}
