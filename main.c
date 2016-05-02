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

void log_fn(void *ctx, const char *s)
{
	fprintf(stderr, "%s", s);
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
	int c = 0;
	signal(SIGINT, sighandle);

	kinesis_set_logger(NULL, log_fn);
	kinesis_consumer *kcs[2];

	for (i = 0; i < 2; ++i)
	{
		char buf[32]; sprintf(buf, "%d", i);
		kcs[i] = kinesis_consumer_create("test", buf);

		if (!kcs[i])
		{
			fprintf(stderr, "failed to connect to shard %s\n", buf);
			return 1;
		}
	}

	for (i = 0; i < 2; ++i)
	{
		kinesis_consumer_start(kcs[i]);
	}

	int timeout = 1000;
	int num = 0;

	while (!flag)
	{
		for (c = 0; c < 2; ++c)
		{
			kinesis_consumer *kc = kcs[c];
			const kinesis_batch *batch = kinesis_consume(kc, timeout);

			if (!batch)
			{
				printf("main timeout\n");
				continue;
			}

			printf("%3.6f %d consumer got %d behind %ld\n", get_time(), c,
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
	}

//	kinesis_consumer_destroy(kc);
}
