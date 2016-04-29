#include <stdio.h>
#include "reader.h"

int main(int argc, char** argv)
{
	int i = 0;

	void *kc = kinesis_create();
	kinesis_start(kc);

	int timeout = 1000;
	int num = 0;

	while (1)
	{
		void *rec = kinesis_consume(kc, timeout);

		if (!rec)
		{
			printf("main timeout\n");
			continue;
		}

		printf("got rec %p %ld %d\n", rec,
				record_batch_get_millis_behind_latest(rec),
				record_batch_get_size(rec));

		int size = record_batch_get_size(rec);

		for (i = 0; i < size; ++i)
		{
			const void *r = record_batch_get_record(rec, i);
			const char *pk = record_get_partition_key(r);
			double t = record_get_arrival_time(r);

			int n = record_get_data_size(r);
			const uint8_t *d = record_get_data(r);

			printf("%f rec pkey %s data %.*s seq %s\n", t, pk, n, d, record_get_sequence_number(r));
		}

		record_batch_destroy(rec);
	}

	printf("main stop\n");
	kinesis_stop(kc);
}
