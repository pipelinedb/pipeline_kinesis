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

#include <sys/time.h>
#include "catalog/pg_type.h"
#include "executor/spi.h"

#include <unistd.h>

#define RETURN_SUCCESS() PG_RETURN_DATUM(CStringGetTextDatum("success"))

PG_MODULE_MAGIC;

extern void _PG_init(void);

HTAB *consumer_info;

typedef struct KinesisConsumerInfo
{
	Oid oid;
	Oid dboid;
	BackgroundWorkerHandle handle;

} KinesisConsumerInfo;

typedef struct ShardState
{
	const char *id;
	StringInfo seqnum;
	kinesis_consumer *kc;
} ShardState;

typedef struct KinesisConsumerState
{
	Oid id;
	char *endpoint_name;
	char *endpoint_region;
	char *endpoint_credfile;
	char *endpoint_url;

	char *relation;
	char *kinesis_stream;

	int batchsize;
	int num_shards;

	ShardState *shards;

} KinesisConsumerState;

void
_PG_init(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(HASHCTL));

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(KinesisConsumerInfo);
	ctl.hash = oid_hash;

	consumer_info = ShmemInitHash("KinsesisConsumerInfo", 4, 64,
			&ctl, HASH_ELEM | HASH_FUNCTION);
}

/*
 * kinesis_add_endpoint
 */
PG_FUNCTION_INFO_V1(kinesis_add_endpoint);
Datum
kinesis_add_endpoint(PG_FUNCTION_ARGS)
{
	char *query = "INSERT INTO pipeline_kinesis_endpoints VALUES ($1, $2, $3, $4)";
    Oid argtypes[4] = { TEXTOID, TEXTOID, TEXTOID, TEXTOID };

	Datum values[4];
    char nulls[4];

	MemSet(nulls, 0, sizeof(nulls));

	values[0] = PG_GETARG_DATUM(0);
	values[1] = PG_GETARG_DATUM(1);
	values[2] = PG_GETARG_DATUM(2);

	if (PG_ARGISNULL(3))
		nulls[3] = 'n';
	else
		values[3] = PG_GETARG_DATUM(3);

	SPI_connect();

    if (SPI_execute_with_args(query, 4, argtypes,
				values, nulls, false, 1) != SPI_OK_INSERT)
		elog(ERROR, "could not add endpoint");

	SPI_finish();

	RETURN_SUCCESS();
}

PG_FUNCTION_INFO_V1(kinesis_remove_endpoint);
Datum
kinesis_remove_endpoint(PG_FUNCTION_ARGS)
{
	char *query = "DELETE FROM pipeline_kinesis_endpoints WHERE name = $1;";
    Oid argtypes[1] = { TEXTOID };

	Datum values[1];
    char nulls[1];

	MemSet(nulls, 0, sizeof(nulls));

	if (PG_ARGISNULL(0))
		elog(ERROR, "endpoint cannot be null");

	values[0] = PG_GETARG_DATUM(0);

	SPI_connect();

    if (SPI_execute_with_args(query, 1, argtypes,
				values, nulls, false, 1) != SPI_OK_DELETE)
		elog(ERROR, "could not remove endpoint");

	SPI_finish();

	RETURN_SUCCESS();
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

static void log_fn(void *ctx, const char *s)
{
	// aws log messages have a newline on them

	int n = strlen(s);

	if (n > 0)
		elog(LOG, "%.*s", n-1, s);
}

static CopyStmt *
get_copy_statement(const char *relname)
{
	MemoryContext old = MemoryContextSwitchTo(CacheMemoryContext);
	CopyStmt *stmt = makeNode(CopyStmt);
	Relation rel;
	TupleDesc desc;
	int i;

	RangeVar *rv = makeRangeVar(NULL, (char*) relname, -1);

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
load_consumer_state(KinesisConsumerState *state, Oid oid)
{
	TupleTableSlot *slot;
	int rv;
	const char *query = "select e.*, c.* from pipeline_kinesis_consumers c inner join pipeline_kinesis_endpoints e on c.endpoint = e.name where c.oid = $1;";

    Oid argtypes[1] = { OIDOID };
	Datum values[1];
    char nulls[1];

	MemSet(state, 0, sizeof(KinesisConsumerState));

	MemSet(nulls, 0, sizeof(nulls));
	values[0] = ObjectIdGetDatum(oid);

	SPI_connect();

	rv = SPI_execute_with_args(query, 1, argtypes, values, nulls, false, 1);

	if (rv != SPI_OK_SELECT)
		elog(ERROR, "could not load consumer state %d", oid);

	slot = MakeSingleTupleTableSlot(SPI_tuptable->tupdesc);
	ExecStoreTuple(SPI_tuptable->vals[0], slot, InvalidBuffer, false);

	{
		Datum url;
		bool isnull = false;
		MemoryContext old = MemoryContextSwitchTo(CacheMemoryContext);

		state->id = oid;
		state->endpoint_name =
			TextDatumGetCString(slot_getattr(slot, 1, &isnull));
		state->endpoint_region =
			TextDatumGetCString(slot_getattr(slot, 2, &isnull));
		state->endpoint_credfile =
			TextDatumGetCString(slot_getattr(slot, 3, &isnull));

		url = slot_getattr(slot, 5, &isnull);

		if (!isnull)
		{
			state->endpoint_credfile =
				TextDatumGetCString(url);
		}

		state->kinesis_stream =
			TextDatumGetCString(slot_getattr(slot, 6, &isnull));

		state->relation =
			TextDatumGetCString(slot_getattr(slot, 7, &isnull));

		state->batchsize = DatumGetInt32(slot_getattr(slot, 8, &isnull));

		MemoryContextSwitchTo(old);
	}

	SPI_finish();
}

static int
find_shard_id(kinesis_stream_metadata *meta, const char *id)
{
	int i = 0;
	int num_shards = kinesis_stream_metadata_get_num_shards(meta);

	for (i = 0; i < num_shards; ++i)
	{
		const kinesis_shard_metadata *smeta =
			kinesis_stream_metadata_get_shard(meta, i);
		const char *sid = kinesis_shard_metadata_get_id(smeta);

		if (strcmp(sid, id) == 0)
			return i;
	}

	return -1;
}

static void
query_meta(KinesisConsumerState *state, kinesis_stream_metadata *meta)
{
	const char *query = "select * from pipeline_kinesis_seqnums "
		"where consumer_id = $1";

	int num_shards = kinesis_stream_metadata_get_num_shards(meta);
	int i = 0;
	Oid argtypes[1] = { OIDOID };
	Datum values[1];
    char nulls[1];
	int rv = 0;
	TupleTableSlot *slot = NULL;

	MemoryContext old = MemoryContextSwitchTo(CacheMemoryContext);
	state->num_shards = num_shards;
	state->shards = palloc0(num_shards * sizeof(ShardState));

	for (i = 0; i < num_shards; ++i)
	{
		const kinesis_shard_metadata *smeta =
			kinesis_stream_metadata_get_shard(meta, i);

		state->shards[i].seqnum = makeStringInfo();
		state->shards[i].id = pstrdup(kinesis_shard_metadata_get_id(smeta));
	}

	MemoryContextSwitchTo(old);

	MemSet(nulls, 0, sizeof(nulls));

	values[0] = ObjectIdGetDatum(state->id);

	SPI_connect();

	rv = SPI_execute_with_args(query, 1, argtypes, values, nulls, false, 0);

	if (rv != SPI_OK_SELECT)
		elog(ERROR, "could not find seqnums");

	slot = MakeSingleTupleTableSlot(SPI_tuptable->tupdesc);

	for (i = 0; i < SPI_processed; ++i)
	{
		bool isnull = false;
		Datum d;
		const char *shard_id;
		int out_ind = 0;
		const char *seq = 0;
		MemoryContext old;

		ExecStoreTuple(SPI_tuptable->vals[i], slot, InvalidBuffer, false);

		print_slot(slot);

		d = slot_getattr(slot, 2, &isnull);
		shard_id = TextDatumGetCString(d);

		out_ind = find_shard_id(meta, shard_id);

		if (out_ind == -1)
			continue;

		d = slot_getattr(slot, 3, &isnull);
		seq = TextDatumGetCString(d);

		old = MemoryContextSwitchTo(CacheMemoryContext);
		appendStringInfoString(state->shards[out_ind].seqnum, seq);
		MemoryContextSwitchTo(old);
	}

	SPI_finish();
}

static void
save_consumer_state(KinesisConsumerState *state)
{
	int i = 0;
	char *query = "INSERT INTO pipeline_kinesis_seqnums VALUES ($1, $2, $3)"
		" on conflict(consumer_id, shard_id) do update set (seqnum) = ($3);";

	Oid argtypes[3] = { OIDOID, TEXTOID, TEXTOID };
	Datum values[3];
    char nulls[3];
	SPIPlanPtr ptr;

	MemSet(nulls, 0, sizeof(nulls));

	SPI_connect();

	ptr = SPI_prepare(query, 3, argtypes);
	Assert(ptr);

	for (i = 0; i < state->num_shards; ++i)
	{
		int rv = 0;
		values[0] = ObjectIdGetDatum(state->id);
		values[1] = CStringGetTextDatum(state->shards[i].id);
		values[2] = CStringGetTextDatum(state->shards[i].seqnum->data);

		rv = SPI_execute_plan(ptr, values, nulls, false, 0);

		if (rv != SPI_OK_INSERT)
			elog(ERROR, "could not update seqnums %d", rv);
	}

	SPI_finish();
}

void kinesis_consume_main(Datum arg)
{
	int si;
	int i;
	CopyStmt *copy;
	Oid oid;
	bool found = false;
	KinesisConsumerInfo *cinfo;
	kinesis_client *client;
	kinesis_stream_metadata *stream_meta;
	StringInfo batch_buffer;

	KinesisConsumerState state;

	pqsignal(SIGTERM, kinesis_consume_main_sigterm);
	pqsignal(SIGSEGV, debug_segfault);

	/* we're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	oid = DatumGetObjectId(arg);

	cinfo = hash_search(consumer_info, &oid, HASH_FIND, &found);

	Assert(found);
	Assert(cinfo);

	BackgroundWorkerInitializeConnectionByOid(cinfo->dboid, 0);

	StartTransactionCommand();
	load_consumer_state(&state, oid);

	copy = get_copy_statement(state.relation);

	kinesis_set_logger(NULL, log_fn);

	client = kinesis_client_create(state.endpoint_region,
			state.endpoint_credfile,
			state.endpoint_url);

	stream_meta =
		kinesis_client_create_stream_metadata(client, state.kinesis_stream);

	if (!stream_meta)
		elog(ERROR, "failed to get kinesis metadata for %s",
				state.kinesis_stream);

	query_meta(&state, stream_meta);

	kinesis_client_destroy_stream_metadata(stream_meta);
	CommitTransactionCommand();

	elog(LOG, "derp");

	for (si = 0; si < state.num_shards; ++si)
	{
		state.shards[si].kc = kinesis_consumer_create(client,
				state.kinesis_stream,
				state.shards[si].id,
				state.shards[si].seqnum->data);

		kinesis_consumer_start(state.shards[si].kc);
	}

	batch_buffer = makeStringInfo();

	while (!got_sigterm)
	{
		resetStringInfo(batch_buffer);

		for (si = 0; si < state.num_shards; ++si)
		{
			int size = 0;
			ShardState *shard = &state.shards[si];
			const kinesis_batch *batch = kinesis_consume(shard->kc, 1000);

			if (!batch)
			{
				elog(LOG, "main timeout");
				continue;
			}

			size = kinesis_batch_get_size(batch);

			for (i = 0; i < size; ++i)
			{
				const kinesis_record *r = kinesis_batch_get_record(batch, i);
				int n = kinesis_record_get_data_size(r);
				const uint8_t *d = kinesis_record_get_data(r);
				const char *seq = kinesis_record_get_sequence_number(r);

				appendBinaryStringInfo(batch_buffer, (const char*) d, n);
				appendStringInfoChar(batch_buffer, '\n');

				resetStringInfo(shard->seqnum);
				appendStringInfoString(shard->seqnum, seq);
			}

			kinesis_batch_destroy(batch);
		}

		if (batch_buffer->len == 0)
			continue;

		StartTransactionCommand();
		execute_copy(copy, batch_buffer);

		save_consumer_state(&state);
		CommitTransactionCommand();
	}

	for (si = 0; si < state.num_shards; ++si)
		kinesis_consumer_destroy(state.shards[si].kc);

	kinesis_client_destroy(client);
}

static void
launch_worker(Oid oid)
{
	bool found = false;
	BackgroundWorker worker;

	KinesisConsumerInfo *info =
		hash_search(consumer_info, &oid, HASH_ENTER, &found);

	BackgroundWorkerHandle *tmp_handle;

	if (found)
		return;

	info->oid = oid;
	info->dboid = MyDatabaseId;

	worker.bgw_main_arg = ObjectIdGetDatum(oid);
	worker.bgw_flags = BGWORKER_BACKEND_DATABASE_CONNECTION |
		BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = NULL;
	worker.bgw_notify_pid = 0;

	sprintf(worker.bgw_library_name, "pipeline_kinesis");
	sprintf(worker.bgw_function_name, "kinesis_consume_main");

	snprintf(worker.bgw_name, BGW_MAXLEN, "[kinesis consumer] %d", oid);

	if (!RegisterDynamicBackgroundWorker(&worker, &tmp_handle))
		elog(ERROR, "could not launch worker");

	info->handle = *tmp_handle;
}

static Relation
acquire_consumer_lock()
{
	Relation consumers =
		heap_openrv(makeRangeVar(NULL, "pipeline_kinesis_consumers", -1),
				AccessExclusiveLock);

	return consumers;
}

static void
drop_consumer_lock(Relation rel)
{
	heap_close(rel, NoLock);
}

PG_FUNCTION_INFO_V1(kinesis_consume_begin_sr);
Datum
kinesis_consume_begin_sr(PG_FUNCTION_ARGS)
{
	char *query = "INSERT INTO pipeline_kinesis_consumers VALUES "
		"($1, $2, $3, $4) on conflict(endpoint, stream, relation) "
		"do update set (batchsize) = ($4) RETURNING oid;";

	Relation lockrel;
	int rv;

    Oid argtypes[5] = { TEXTOID, TEXTOID, TEXTOID, INT4OID, INT4OID };
	Datum values[5];
    char nulls[5];

	TupleTableSlot *slot;
	bool isnull = false;
	Datum d;
	Oid oid;

	MemSet(nulls, 0, sizeof(nulls));

	lockrel = acquire_consumer_lock();

	if (PG_ARGISNULL(0))
		elog(ERROR, "endpoint cannot be null");
	if (PG_ARGISNULL(1))
		elog(ERROR, "stream cannot be null");
	if (PG_ARGISNULL(2))
		elog(ERROR, "relation cannot be null");

	values[0] = PG_GETARG_DATUM(0);
	values[1] = PG_GETARG_DATUM(1);
	values[2] = PG_GETARG_DATUM(2);

	if (PG_ARGISNULL(3))
		values[3] = Int32GetDatum(1000);
	else
		values[3] = PG_GETARG_DATUM(3);

	SPI_connect();

	rv = SPI_execute_with_args(query, 4, argtypes, values, nulls, false, 1);

	if (rv != SPI_OK_INSERT_RETURNING)
		elog(ERROR, "could not create consumer %d", rv);

	slot = MakeSingleTupleTableSlot(SPI_tuptable->tupdesc);
	ExecStoreTuple(SPI_tuptable->vals[0], slot, InvalidBuffer, false);

	d = slot_getattr(slot, 1, &isnull);
	oid = DatumGetObjectId(d);

	launch_worker(oid);
	SPI_finish();

	drop_consumer_lock(lockrel);

	RETURN_SUCCESS();
}

static Oid
find_consumer(Datum endpoint, Datum stream, Datum relation)
{
	char *query = "SELECT oid from pipeline_kinesis_consumers where endpoint = $1 and stream = $2 and relation = $3;";

    Oid argtypes[3] = { TEXTOID, TEXTOID, TEXTOID };
	Datum values[3];
    char nulls[3];
	int rv;

	TupleTableSlot *slot;
	bool isnull = false;
	Datum d;

	values[0] = endpoint;
	values[1] = stream;
	values[2] = relation;

	MemSet(nulls, 0, sizeof(nulls));

	rv = SPI_execute_with_args(query, 4, argtypes, values, nulls, false, 1);

	if (rv != SPI_OK_SELECT)
		elog(ERROR, "could not find");

	slot = MakeSingleTupleTableSlot(SPI_tuptable->tupdesc);
	ExecStoreTuple(SPI_tuptable->vals[0], slot, InvalidBuffer, false);

	isnull = false;
	d = slot_getattr(slot, 1, &isnull);

	return DatumGetObjectId(d);
}

static void
delete_consumer(Oid oid)
{
	char *query = "DELETE from pipeline_kinesis_consumers where oid = $1;";

    Oid argtypes[1] = { OIDOID };
	Datum values[1];
    char nulls[1];
	int rv;

	values[0] = ObjectIdGetDatum(oid);
	MemSet(nulls, 0, sizeof(nulls));

	rv = SPI_execute_with_args(query, 1, argtypes, values, nulls, false, 1);

	if (rv != SPI_OK_DELETE)
		elog(ERROR, "could not delete");
}

PG_FUNCTION_INFO_V1(kinesis_consume_end_sr);
Datum
kinesis_consume_end_sr(PG_FUNCTION_ARGS)
{
	Relation lockrel = acquire_consumer_lock();
	Oid oid;
	bool found = false;
	KinesisConsumerInfo *info;

	SPI_connect();

	oid = find_consumer(PG_GETARG_DATUM(0),
						    PG_GETARG_DATUM(1),
						    PG_GETARG_DATUM(2));

	info = hash_search(consumer_info, &oid, HASH_FIND, &found);

	TerminateBackgroundWorker(&info->handle);
	hash_search(consumer_info, &oid, HASH_REMOVE, &found);
	delete_consumer(oid);

	SPI_finish();
	drop_consumer_lock(lockrel);

	RETURN_SUCCESS();
}

/*
 * kinesis_consume_begin_all
 *
 * Start all consumers
 */
PG_FUNCTION_INFO_V1(kinesis_consume_begin_all);
Datum
kinesis_consume_begin_all(PG_FUNCTION_ARGS)
{
	Relation lockrel = acquire_consumer_lock();

	const char *query = "SELECT oid from pipeline_kinesis_consumers;";
	int rv = 0;
	int i = 0;
	TupleTableSlot *slot;
	Datum d;
	Oid oid;
	bool isnull;

	SPI_connect();

	rv = SPI_execute(query, false, 0);

	if (rv != SPI_OK_SELECT)
		elog(ERROR, "could not select consumers");

	slot = MakeSingleTupleTableSlot(SPI_tuptable->tupdesc);

	elog(LOG, "got %d", SPI_processed);

	for (i = 0; i < SPI_processed; ++i)
	{
		ExecStoreTuple(SPI_tuptable->vals[i], slot, InvalidBuffer, false);
		d = slot_getattr(slot, 1, &isnull);

		oid = DatumGetObjectId(d);
		launch_worker(oid);
	}

	SPI_finish();

	drop_consumer_lock(lockrel);
	RETURN_SUCCESS();
}

///*
// * kinesis_consume_end_all
// *
// * Start all consumers
// */
//PG_FUNCTION_INFO_V1(kinesis_consume_begin_all);
//Datum
//kinesis_consume_begin_all(PG_FUNCTION_ARGS)
//{
//	Relation lockrel = acquire_consumer_lock();
//
//	const char *query = "SELECT oid from pipeline_kinesis_consumers;";
//	int rv = 0;
//	int i = 0;
//	TupleTableSlot *slot;
//	Datum d;
//	Oid oid;
//	bool isnull;
//	List *list;
//
//	SPI_connect();
//
//	rv = SPI_execute(query, false, 0);
//
//	if (rv != SPI_OK_SELECT)
//		elog(ERROR, "could not select consumers");
//
//	slot = MakeSingleTupleTableSlot(SPI_tuptable->tupdesc);
//
//
//	for (i = 0; i < SPI_processed; ++i)
//	{
//		ExecStoreTuple(SPI_tuptable->vals[i], slot, InvalidBuffer, false);
//		d = slot_getattr(slot, 1, &isnull);
//
//		oid = DatumGetObjectId(d);
//		lappend_oid(list, oid);
//
//		info = hash_search(consumer_info, &oid, HASH_FIND, &found);
//		TerminateBackgroundWorker(&info->handle);
//		hash_search(consumer_info, &oid, HASH_REMOVE, &found);
//		delete_consumer(oid);
//	}
//
//	SPI_finish();
//
//	drop_consumer_lock(lockrel);
//	RETURN_SUCCESS();
//}
