/*-------------------------------------------------------------------------
 *
 * kinesis_consumer.h
 *	  C interface for kinesis consumer
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef KINESIS_CONSUMER_H
#define KINESIS_CONSUMER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

typedef struct kinesis_consumer kinesis_consumer;
typedef struct kinesis_batch kinesis_batch;
typedef struct kinesis_record kinesis_record;
typedef struct kinesis_client kinesis_client;
typedef struct kinesis_stream_metadata kinesis_stream_metadata;
typedef struct kinesis_shard_metadata kinesis_shard_metadata;

typedef void (*kinesis_log_fn) (void *ctx, const char *s, int len);

kinesis_client * kinesis_client_create(const char *region,
									   const char *credfile,
									   const char *url);

kinesis_stream_metadata * kinesis_client_create_stream_metadata(kinesis_client *client, const char *stream);

int kinesis_stream_metadata_get_num_shards(kinesis_stream_metadata *meta);

const kinesis_shard_metadata *
kinesis_stream_metadata_get_shard(kinesis_stream_metadata *meta, int i);

const char *
kinesis_shard_metadata_get_id(const kinesis_shard_metadata *meta);

void kinesis_client_destroy_stream_metadata(kinesis_stream_metadata *meta);

void kinesis_client_destroy(kinesis_client *client);

void kinesis_set_logger(void *ctx, kinesis_log_fn fn, const char *level);

kinesis_consumer * kinesis_consumer_create(kinesis_client *client,
										   const char *stream,
										   const char *shard,
										   const char *seqnum,
										   int batchsize);

void kinesis_consumer_start(kinesis_consumer *k);
void kinesis_consumer_stop(kinesis_consumer *k);
void kinesis_consumer_destroy(kinesis_consumer *k);

int64_t kinesis_batch_get_millis_behind_latest(const kinesis_batch *rb);
int	kinesis_batch_get_size(const kinesis_batch *rb);
const kinesis_record * kinesis_batch_get_record(const kinesis_batch *rb, int i);
void kinesis_batch_destroy(const kinesis_batch *rb);

const kinesis_batch * kinesis_consume(kinesis_consumer *k, int timeout);

const char * kinesis_record_get_sequence_number(const kinesis_record *rec);
const char * kinesis_record_get_partition_key(const kinesis_record *rec);
double kinesis_record_get_arrival_time(const kinesis_record *rec);
int kinesis_record_get_data_size(const kinesis_record *rec);
const uint8_t * kinesis_record_get_data(const kinesis_record *rec);

#ifdef __cplusplus
}
#endif

#endif
