#include <stdio.h>
#include <signal.h>
#include "reader.h"

volatile int flag = 0;

void sighandle(int s)
{
	flag = 1;
}

int main(int argc, char** argv)
{
	int i = 0;

	signal(SIGINT, sighandle);

	kinesis_consumer *kc = kinesis_consumer_create();
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

		printf("got batch %p %ld %d\n", batch,
				kinesis_batch_get_millis_behind_latest(batch),
				kinesis_batch_get_size(batch));

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
