#ifndef READER_H_7D853D17
#define READER_H_7D853D17

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

void* kinesis_create();
void kinesis_destroy(void *k);

void kinesis_start(void *k);
void kinesis_stop(void *k);
int64_t record_batch_get_millis_behind_latest(void *rb);
int record_batch_get_size(void *rb);
const void* record_batch_get_record(void *rb, int i);
const char* record_get_sequence_number(const void *rec);
const char* record_get_partition_key(const void *rec);
double record_get_arrival_time(const void *rec);
int record_get_data_size(void *rec);
const uint8_t* record_get_data(void *rec);

void record_batch_destroy(void *rb);
void* kinesis_consume(void *k, int timeout);

#ifdef __cplusplus
}
#endif

#endif
