#ifndef READER_H_7D853D17
#define READER_H_7D853D17

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

typedef struct kinesis_consumer kinesis_consumer;
typedef struct kinesis_batch kinesis_batch;
typedef struct kinesis_record kinesis_record;

kinesis_consumer* kinesis_consumer_create();

void kinesis_consumer_start(kinesis_consumer *k);
void kinesis_consumer_stop(kinesis_consumer *k);
void kinesis_consumer_destroy(kinesis_consumer *k);

int64_t kinesis_batch_get_millis_behind_latest(const kinesis_batch *rb);
int	kinesis_batch_get_size(const kinesis_batch *rb);
const kinesis_record* kinesis_batch_get_record(const kinesis_batch *rb, int i);
void kinesis_batch_destroy(const kinesis_batch *rb);

const kinesis_batch* kinesis_consume(kinesis_consumer *k, int timeout);

const char* kinesis_record_get_sequence_number(const kinesis_record *rec);
const char* kinesis_record_get_partition_key(const kinesis_record *rec);
double kinesis_record_get_arrival_time(const kinesis_record *rec);
int kinesis_record_get_data_size(const kinesis_record *rec);
const uint8_t* kinesis_record_get_data(const kinesis_record *rec);

#ifdef __cplusplus
}
#endif

#endif
