#include <stdio.h>
#include <signal.h>
#include <sys/time.h>

#include "kinesis_consumer.h"

volatile int flag = 0;

void sighandle(int s)
{
	flag = 1;
}

static double
get_time()
{
    struct timeval tv;
    gettimeofday(&tv, 0);
    return tv.tv_sec + (tv.tv_usec / 1000000.0);
}

/*
 * Simple C driver for kinesis_consumer.h
 *
 * Continually consumes batches until SIGINT
 *
 * Note - a number of empty batches are returned from kinesis when
 * consuming from TRIM_HORIZION.
 *
 * Empty batches are also returned when 'consuming' from the queue and there
 * is no data.
 *
 * Hence, we use 'empty' batches as heartbeat messages.
 */
int main(int argc, char** argv)
{
	int i = 0;
	signal(SIGINT, sighandle);

	kinesis_consumer *kc = kinesis_consumer_create();

	if (!kc)
	{
		fprintf(stderr, "failed to connect to kinesis\n");
		return 1;
	}

	kinesis_consumer_start(kc);

	int timeout = 1000;
	int num = 0;

	while (!flag)
	{
		const kinesis_batch *batch = kinesis_consume(kc, timeout);

		if (!batch)
		{
			printf("main timeout\n");
			continue;
		}

		printf("%3.6f consumer got %d behind %ld\n", get_time(),
				kinesis_batch_get_size(batch),
				kinesis_batch_get_millis_behind_latest(batch));

		int size = kinesis_batch_get_size(batch);

		for (i = 0; i < size; ++i)
		{
			const kinesis_record *r = kinesis_batch_get_record(batch, i);
			const char *pk = kinesis_record_get_partition_key(r);
			double t = kinesis_record_get_arrival_time(r);

			int n = kinesis_record_get_data_size(r);
			const uint8_t *d = kinesis_record_get_data(r);
			const char *seq = kinesis_record_get_sequence_number(r);

			printf("%f rec pkey %s data %.*s seq %s\n", t, pk, n, d, seq);
		}

		kinesis_batch_destroy(batch);
	}

	kinesis_consumer_destroy(kc);
}
