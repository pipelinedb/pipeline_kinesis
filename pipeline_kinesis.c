/*-------------------------------------------------------------------------
 *
 * pipeline_kinesis.c
 *	  Extension for Kinesis support
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "nodes/makefuncs.h"
#include "nodes/print.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "postmaster/bgworker.h"
#include "miscadmin.h"
#include "commands/dbcommands.h"
#include "kinesis_consumer.h"

#include <unistd.h>

#define RETURN_SUCCESS() PG_RETURN_DATUM(CStringGetTextDatum("success"))

PG_MODULE_MAGIC;

extern void _PG_init(void);
extern void _PG_fini(void);

void
_PG_init(void)
{
}

void
_PG_fini(void)
{
}

/*
 * kinesis_add_endpoint
 */
PG_FUNCTION_INFO_V1(kinesis_add_endpoint);
Datum
kinesis_add_endpoint(PG_FUNCTION_ARGS)
{
	HeapTuple tup;
	Datum values[4];
	bool nulls[4];
	text *name;
	text *region;
	text *credfile;
	text *url;
	ScanKeyData skey[1];
	HeapScanDesc scan;
	Relation endpoints;

	name = PG_GETARG_TEXT_P(0);
	region = PG_GETARG_TEXT_P(1);
	credfile = PG_GETARG_TEXT_P(2);
	url = PG_ARGISNULL(3) ? NULL : PG_GETARG_TEXT_P(3);

	endpoints = heap_openrv(makeRangeVar(NULL,
				"pipeline_kinesis_endpoints", -1),
			AccessExclusiveLock);

	/* don't allow duplicate endpoints */
	ScanKeyInit(&skey[0], 1, BTEqualStrategyNumber, F_TEXTEQ,
			PointerGetDatum(name));
	scan = heap_beginscan(endpoints, GetTransactionSnapshot(), 1, skey);
	tup = heap_getnext(scan, ForwardScanDirection);

	if (HeapTupleIsValid(tup))
	{
		heap_endscan(scan);
		heap_close(endpoints, NoLock);

		elog(ERROR, "endpoint %s already exists", TextDatumGetCString(name));
	}

	values[0] = PointerGetDatum(name);
	values[1] = PointerGetDatum(region);
	values[2] = PointerGetDatum(credfile);

	MemSet(nulls, false, sizeof(nulls));

	if (url == NULL)
		nulls[3] = true;
	else
		values[3] = PointerGetDatum(url);

	tup = heap_form_tuple(RelationGetDescr(endpoints), values, nulls);
	simple_heap_insert(endpoints, tup);

	heap_endscan(scan);
	heap_close(endpoints, NoLock);

	RETURN_SUCCESS();
}

PG_FUNCTION_INFO_V1(kinesis_remove_endpoint);
Datum
kinesis_remove_endpoint(PG_FUNCTION_ARGS)
{
	HeapTuple tup;
	Relation endpoints;
	text *host;
	ScanKeyData skey[1];
	HeapScanDesc scan;

	if (PG_ARGISNULL(0))
		elog(ERROR, "endpoint cannot be null");

	host = PG_GETARG_TEXT_P(0);

	endpoints = heap_openrv(makeRangeVar(NULL,
				"pipeline_kinesis_endpoints", -1), AccessExclusiveLock);

	/* don't allow duplicate endpoints */
	ScanKeyInit(&skey[0], 1, BTEqualStrategyNumber,
			F_TEXTEQ, PointerGetDatum(host));
	scan = heap_beginscan(endpoints, GetTransactionSnapshot(), 1, skey);
	tup = heap_getnext(scan, ForwardScanDirection);

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "endpoint %s does not exist", TextDatumGetCString(host));

	simple_heap_delete(endpoints, &tup->t_self);

	heap_endscan(scan);
	heap_close(endpoints, NoLock);

	RETURN_SUCCESS();
}

static Oid
create_consumer(Relation consumers, text *endpoint,
		text *relation, text *stream,
		int batchsize, int parallelism)
{
	HeapTuple tup;
	Datum values[5];
	bool nulls[5];
	Oid oid;
	ScanKeyData skey[3];
	HeapScanDesc scan;

	// insert or update into pipeline_kinesis_consumers

	MemSet(nulls, false, sizeof(nulls));

	ScanKeyInit(&skey[0], 1, BTEqualStrategyNumber, F_TEXTEQ, PointerGetDatum(endpoint));
	ScanKeyInit(&skey[1], 2, BTEqualStrategyNumber, F_TEXTEQ, PointerGetDatum(relation));
	ScanKeyInit(&skey[2], 3, BTEqualStrategyNumber, F_TEXTEQ, PointerGetDatum(stream));

	scan = heap_beginscan(consumers, GetTransactionSnapshot(), 3, skey);
	tup = heap_getnext(scan, ForwardScanDirection);

	values[3] = Int32GetDatum(batchsize);
	values[4] = Int32GetDatum(parallelism);

	if (HeapTupleIsValid(tup))
	{
		/* consumer already exists, so just update it with the given parameters */
		bool replace[5];
		MemSet(replace, true, sizeof(nulls));

		replace[0] = false;
		replace[1] = false;
		replace[2] = false;

		tup = heap_modify_tuple(tup, RelationGetDescr(consumers), values, nulls, replace);
		simple_heap_update(consumers, &tup->t_self, tup);

		oid = HeapTupleGetOid(tup);
	}
	else
	{
		/* consumer doesn't exist yet, create it with the given parameters */
		values[0] = PointerGetDatum(endpoint);
		values[1] = PointerGetDatum(relation);
		values[2] = PointerGetDatum(stream);

		tup = heap_form_tuple(RelationGetDescr(consumers), values, nulls);
		oid = simple_heap_insert(consumers, tup);
	}

	heap_endscan(scan);
	CommandCounterIncrement();

	return oid;
}

static void
kinesis_consume_main_sigterm(SIGNAL_ARGS)
{
//	int save_errno = errno;
//
//	got_sigterm = true;
//	if (MyProc)
//		SetLatch(&MyProc->procLatch);
//
//	errno = save_errno;
}

extern void kinesis_consume_main(Datum arg);

#include <sys/time.h>

static double
get_time()
{
    struct timeval tv;
    gettimeofday(&tv, 0);
    return tv.tv_sec + (tv.tv_usec / 1000000.0);
}

static void log_fn(void *ctx, const char *s)
{
	elog(LOG, "%s", s);
}

void kinesis_consume_main(Datum arg)
{
	int i;
	Oid dbid;
	char *dbname;
	pqsignal(SIGTERM, kinesis_consume_main_sigterm);
#define BACKTRACE_SEGFAULTS
#ifdef BACKTRACE_SEGFAULTS
	pqsignal(SIGSEGV, debug_segfault);
#endif

	/* we're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	dbid = DatumGetObjectId(arg);
	elog(LOG, "start consumer on db %d", dbid);

//	dbname = get_database_name(dbid);
//	elog(LOG, "dbname %s", dbname);

	BackgroundWorkerInitializeConnectionByOid(dbid, 0);

	kinesis_set_logger(NULL, log_fn);
	kinesis_consumer *kc = kinesis_consumer_create("test", "0");
	kinesis_consumer_start(kc);

	while (true)
	{
		const kinesis_batch *batch = kinesis_consume(kc, 1000);

		if (!batch)
		{
			elog(LOG, "main timeout");
			continue;
		}

		elog(LOG, "%3.6f consumer got %d behind %ld", get_time(),
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

			elog(LOG, "%f rec pkey %s data %.*s seq %s\n", t, pk, n, d, seq);
		}

		kinesis_batch_destroy(batch);
	}
}

static bool
launch_worker(text *endpoint, text *stream, text *relation, int batchsize, int parallelism)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;

	worker.bgw_main_arg = MyDatabaseId;
	worker.bgw_flags = BGWORKER_BACKEND_DATABASE_CONNECTION |
		BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = NULL;
	worker.bgw_notify_pid = 0;

	sprintf(worker.bgw_library_name, "pipeline_kinesis");
	sprintf(worker.bgw_function_name, "kinesis_consume_main");

	snprintf(worker.bgw_name, BGW_MAXLEN, "[kinesis consumer] %s <- %s:%s", TextDatumGetCString(relation), TextDatumGetCString(endpoint), TextDatumGetCString(stream));

//	proc->consumer_id = consumer->id;
//	proc->partition_group = i;
//	proc->start_offset = offset;
//	namestrcpy(&proc->dbname, get_database_name(MyDatabaseId));

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		return false;

	return true;
}

// endpoint, stream, relation,

PG_FUNCTION_INFO_V1(kinesis_consume_begin_sr);
Datum
kinesis_consume_begin_sr(PG_FUNCTION_ARGS)
{
	HeapTuple tup;
	Relation endpoints;
	Relation consumers;
	ScanKeyData skey[1];
	HeapScanDesc scan;

	text *endpoint;
	text *stream;
	text *relation;

	int batchsize = 0;
	int parallelism = 0;

	text *start_seq = NULL;

	Oid oid;

	(void) (start_seq);
	(void) (oid);

	if (PG_ARGISNULL(0))
		elog(ERROR, "endpoint cannot be null");
	if (PG_ARGISNULL(1))
		elog(ERROR, "stream cannot be null");
	if (PG_ARGISNULL(2))
		elog(ERROR, "relation cannot be null");

	endpoint = PG_GETARG_TEXT_P(0);
	stream = PG_GETARG_TEXT_P(1);
	relation = PG_GETARG_TEXT_P(2);


	if (PG_ARGISNULL(3))
		batchsize = 1000;
	else
		batchsize = PG_GETARG_INT32(3);

	if (PG_ARGISNULL(4))
		parallelism = 1;
	else
		parallelism = PG_GETARG_INT32(4);


	if (PG_ARGISNULL(5))
		start_seq = NULL;
	else
		start_seq = PG_GETARG_TEXT_P(5);

	// find the matching endpoint
	endpoints = heap_openrv(makeRangeVar(NULL,
				"pipeline_kinesis_endpoints", -1), AccessExclusiveLock);

	ScanKeyInit(&skey[0], 1, BTEqualStrategyNumber,
			F_TEXTEQ, PointerGetDatum(endpoint));

	scan = heap_beginscan(endpoints, GetTransactionSnapshot(), 1, skey);
	tup = heap_getnext(scan, ForwardScanDirection);

	if (!HeapTupleIsValid(tup))
	{
		elog(ERROR, "endpoint %s does not exist",
				TextDatumGetCString(endpoint));
	}

	consumers =
		heap_openrv(makeRangeVar(NULL, "pipeline_kinesis_consumers", -1),
				AccessExclusiveLock);

	oid = create_consumer(consumers, endpoint, relation, stream,
			batchsize, parallelism);

	launch_worker(endpoint, stream, relation, batchsize, parallelism);

	heap_close(consumers, NoLock);

	heap_endscan(scan);
	heap_close(endpoints, NoLock);

	RETURN_SUCCESS();
}
