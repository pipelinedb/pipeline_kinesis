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
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "postmaster/bgworker.h"
#include "miscadmin.h"
#include "commands/dbcommands.h"
#include "kinesis_consumer.h"
#include "pipeline/stream.h"
#include "catalog/pipeline_stream_fn.h"
#include "commands/copy.h"
#include "storage/proc.h"

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

volatile int got_sigterm = 0;

static void
kinesis_consume_main_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sigterm = true;

	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
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
	// aws log messages have a newline on them

	int n = strlen(s);
	elog(LOG, "%.*s", n-1, s);
}

static CopyStmt *
get_copy_statement(RangeVar *rv)
{
	MemoryContext old = MemoryContextSwitchTo(CacheMemoryContext);
	CopyStmt *stmt = makeNode(CopyStmt);
	Relation rel;
	TupleDesc desc;
	DefElem *format = makeNode(DefElem);
	int i;

	stmt->relation = rv;
	stmt->filename = NULL;
	stmt->options = NIL;
	stmt->is_from = true;
	stmt->query = NULL;
	stmt->attlist = NIL;

	rel = heap_openrv(rv, AccessShareLock);
	desc = RelationGetDescr(rel);

	for (i = 0; i < desc->natts; i++)
	{
		/*
		 * Users can't supply values for arrival_timestamp, so make
		 * sure we exclude it from the copy attr list
		 */
		char *name = NameStr(desc->attrs[i]->attname);
		if (IsStream(RelationGetRelid(rel)) && pg_strcasecmp(name,
					ARRIVAL_TIMESTAMP) == 0)
			continue;
		stmt->attlist = lappend(stmt->attlist, makeString(name));
	}

//	if (consumer->delimiter)
//	{
//		DefElem *delim = makeNode(DefElem);
//		delim->defname = OPTION_DELIMITER;
//		delim->arg = (Node *) makeString(consumer->delimiter);
//		stmt->options = lappend(stmt->options, delim);
//	}
//
//	format->defname = OPTION_FORMAT;
//	format->arg = (Node *) makeString(consumer->format);
//	stmt->options = lappend(stmt->options, format);
//
//	if (consumer->quote)
//	{
//		DefElem *quote = makeNode(DefElem);
//		quote->defname = OPTION_QUOTE;
//		quote->arg = (Node *) makeString(consumer->quote);
//		stmt->options = lappend(stmt->options, quote);
//	}
//
//	if (consumer->escape)
//	{
//		DefElem *escape = makeNode(DefElem);
//		escape->defname = OPTION_ESCAPE;
//		escape->arg = (Node *) makeString(consumer->escape);
//		stmt->options = lappend(stmt->options, escape);
//	}

	heap_close(rel, NoLock);
	MemoryContextSwitchTo(old);

	return stmt;
}

static int
copy_next(void *args, void *buf, int minread, int maxread)
{
	StringInfo messages = (StringInfo) args;
	int remaining = messages->len - messages->cursor;
	int read = 0;

	if (maxread <= remaining)
		read = maxread;
	else
		read = remaining;

	if (read == 0)
		return 0;

	memcpy(buf, messages->data + messages->cursor, read);
	messages->cursor += read;

	return read;
}

static uint64
execute_copy(CopyStmt *stmt, StringInfo buf)
{
	uint64 processed;

	copy_iter_hook = copy_next;
	copy_iter_arg = buf;

	DoCopy(stmt, "COPY", &processed);

	return processed;
}

static void
save_consumer_state(Relation seqnums, StringInfo seq)
{
	// write the sequence number to the pipeline_kinesis_seqnums table

	HeapTuple tup;
	Datum values[3];
	bool nulls[3];

	ScanKeyData skey[2];
	HeapScanDesc scan;

	MemSet(nulls, false, sizeof(nulls));

	ScanKeyInit(&skey[0], 1, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(0));
	ScanKeyInit(&skey[1], 2, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(0));

	scan = heap_beginscan(seqnums, GetTransactionSnapshot(), 2, skey);
	tup = heap_getnext(scan, ForwardScanDirection);

	values[2] = CStringGetTextDatum(seq->data);

	if (HeapTupleIsValid(tup))
	{
		/* seq already exists, update it */
		bool replace[3];
		MemSet(replace, true, sizeof(nulls));

		replace[0] = false;
		replace[1] = false;

		tup = heap_modify_tuple(tup, RelationGetDescr(seqnums),
				values, nulls, replace);
		simple_heap_update(seqnums, &tup->t_self, tup);
	}
	else
	{
		/* consumer doesn't exist yet, create it with the given parameters */
		values[0] = ObjectIdGetDatum(0);
		values[1] = Int32GetDatum(0);

		tup = heap_form_tuple(RelationGetDescr(seqnums), values, nulls);
		simple_heap_insert(seqnums, tup);
	}

	heap_endscan(scan);
	CommandCounterIncrement();
}

void kinesis_consume_main(Datum arg)
{
	int i;
	Oid dbid;
	char *dbname;
	CopyStmt *copy;

	pqsignal(SIGTERM, kinesis_consume_main_sigterm);
#define BACKTRACE_SEGFAULTS
#ifdef BACKTRACE_SEGFAULTS
	pqsignal(SIGSEGV, debug_segfault);
#endif

	/* we're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	dbid = DatumGetObjectId(arg);

	// need to load the consumer state from the db.

	BackgroundWorkerInitializeConnectionByOid(dbid, 0);
	text *rel_text = cstring_to_text("foo");

	RangeVar *relname =
		makeRangeVarFromNameList(textToQualifiedNameList(rel_text));

	StartTransactionCommand();
	copy = get_copy_statement(relname);
	CommitTransactionCommand();

	kinesis_set_logger(NULL, log_fn);
	kinesis_consumer *kc = kinesis_consumer_create("test", "0");
	kinesis_consumer_start(kc);

	StringInfo info = makeStringInfo();
	StringInfo last_seq = makeStringInfo();

	int ctr = 0;

	while (!got_sigterm)
	{
		const kinesis_batch *batch = kinesis_consume(kc, 1000);

		if (!batch)
		{
			elog(LOG, "main timeout");
			continue;
		}

		resetStringInfo(info);

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

			elog(LOG, "seq %s rec %.*s", seq, n, d);

			appendBinaryStringInfo(info, (const char*) d, n);
			appendStringInfoChar(info, '\n');

			resetStringInfo(last_seq);
			appendStringInfoString(last_seq, seq);
		}

		kinesis_batch_destroy(batch);

		if (!size)
			continue;

		StartTransactionCommand();
		execute_copy(copy, info);

		Relation seqrel = heap_openrv(makeRangeVar(NULL, "pipeline_kinesis_seqnums", -1), RowExclusiveLock);

		save_consumer_state(seqrel, last_seq);

		heap_close(seqrel, NoLock);

		CommitTransactionCommand();
	}

	kinesis_consumer_destroy(kc);
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
