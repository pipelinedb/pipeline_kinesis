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

#define OPTION_DELIMITER "delimiter"
#define OPTION_FORMAT "format"
#define FORMAT_CSV "csv"
#define OPTION_QUOTE "quote"
#define OPTION_ESCAPE "escape"

PG_MODULE_MAGIC;

extern void _PG_init(void);
HTAB *consumer_info;

#define MAX_PROCS 8

/*
 * Shared state - used for starting / terminating workers
 */
typedef struct KinesisConsumerInfo
{
	int id;
	Oid dboid;

	int start_seq;
	int num_procs;

	BackgroundWorkerHandle handle[MAX_PROCS];
} KinesisConsumerInfo;

/*
 * Local state to a worker
 */
typedef struct ShardState
{
	const char *id;
	StringInfo seqnum;
	kinesis_consumer *kc;
} ShardState;

typedef struct KinesisConsumerState
{
	int consumer_id;
	int proc_id;
	int num_procs;
	char *endpoint_name;
	char *endpoint_region;
	char *endpoint_credfile;
	char *endpoint_url;
	char *relation;
	char *kinesis_stream;
	int batchsize;
	int num_shards;
	ShardState *shards;
	char *format;
	char *delimiter;
	char *quote;
	char *escape;
} KinesisConsumerState;

void
_PG_init(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(HASHCTL));

	ctl.keysize = sizeof(int);
	ctl.entrysize = sizeof(KinesisConsumerInfo);
	ctl.hash = oid_hash;

	consumer_info = ShmemInitHash("KinsesisConsumerInfo", 4, 64,
			&ctl, HASH_ELEM | HASH_FUNCTION);
}

/*
 * kinesis_add_endpoint
 *
 * Add endpoint with name, region, credfile, url
 */
PG_FUNCTION_INFO_V1(kinesis_add_endpoint);
Datum
kinesis_add_endpoint(PG_FUNCTION_ARGS)
{
	const char *query =
		"INSERT INTO pipeline_kinesis.endpoints VALUES ($1, $2, $3, $4)";
    Oid argtypes[4] = { TEXTOID, TEXTOID, TEXTOID, TEXTOID };

	Datum values[4];
    char nulls[4];

	MemSet(nulls, ' ', sizeof(nulls));

	values[0] = PG_GETARG_DATUM(0);
	values[1] = PG_GETARG_DATUM(1);

	if (PG_ARGISNULL(2))
		nulls[2] = 'n';
	else
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

/*
 * kinesis_remove_endpoint
 *
 * Remove kinesis endpoint matching name
 */
PG_FUNCTION_INFO_V1(kinesis_remove_endpoint);
Datum
kinesis_remove_endpoint(PG_FUNCTION_ARGS)
{
	const char *query =
		"DELETE FROM pipeline_kinesis.endpoints WHERE name = $1;";
    Oid argtypes[1] = { TEXTOID };

	Datum values[1];
    char nulls[1];

	MemSet(nulls, ' ', sizeof(nulls));

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

static volatile sig_atomic_t got_sigterm = false;

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

static void
log_fn(void *ctx, const char *s, int size)
{
	/* aws log messages have a newline on them, so only log n-1 */

	/* Note - there is a mutex in the cpp caller protecting this */
	/* it may be more appropriate to put the mutex on this side */

	if (size > 0)
		elog(LOG, "%.*s", size-1, s);
}

/*
 * get_copy_statement
 *
 * Get the COPY statement that will be used to write messages to a stream
 */
static CopyStmt *
get_copy_statement(KinesisConsumerState *state)
{
	text *rel_text = cstring_to_text(state->relation);
	MemoryContext old = MemoryContextSwitchTo(CacheMemoryContext);
	DefElem *format = makeNode(DefElem);
	CopyStmt *stmt = makeNode(CopyStmt);
	Relation rel;
	TupleDesc desc;
	int i;

	RangeVar *rv = makeRangeVarFromNameList(textToQualifiedNameList(rel_text));

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

	if (state->delimiter)
	{
		DefElem *delim = makeNode(DefElem);
		delim->defname = OPTION_DELIMITER;
		delim->arg = (Node *) makeString(state->delimiter);
		stmt->options = lappend(stmt->options, delim);
	}

	format->defname = OPTION_FORMAT;
	format->arg = (Node *) makeString(state->format);
	stmt->options = lappend(stmt->options, format);

	if (state->quote)
	{
		DefElem *quote = makeNode(DefElem);
		quote->defname = OPTION_QUOTE;
		quote->arg = (Node *) makeString(state->quote);
		stmt->options = lappend(stmt->options, quote);
	}

	if (state->escape)
	{
		DefElem *escape = makeNode(DefElem);
		escape->defname = OPTION_ESCAPE;
		escape->arg = (Node *) makeString(state->escape);
		stmt->options = lappend(stmt->options, escape);
	}

	heap_close(rel, NoLock);
	MemoryContextSwitchTo(old);

	return stmt;
}

/*
 * copy_next
 */
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

/*
 * execute_copy
 *
 * Perform the COPY into the stream
 */
static uint64
execute_copy(CopyStmt *stmt, StringInfo buf)
{
	uint64 processed;

	copy_iter_hook = copy_next;
	copy_iter_arg = buf;

	DoCopy(stmt, "COPY", &processed);

	return processed;
}

/*
 * load_consumer_state
 *
 * For a given consumer oid, load the relevant details out of the kinesis
 * tables
 */
static void
load_consumer_state(KinesisConsumerState *state, int id)
{
	TupleTableSlot *slot;
	int rv;

	const char *query = "SELECT e.name, e.region, e.credfile, e.url, "
		"c.endpoint, c.stream, c.relation, c.format, c.delimiter, c.quote, "
		"c.escape, c.batchsize FROM pipeline_kinesis.consumers c "
		"INNER JOIN pipeline_kinesis.endpoints e ON c.endpoint = e.name "
		"WHERE c.id = $1;";

	Oid argtypes[1] = { INT4OID };
	Datum values[1];
	char nulls[1];

	Datum d;
	bool isnull = false;
	MemoryContext old;

	MemSet(nulls, ' ', sizeof(nulls));
	values[0] = Int32GetDatum(id);

	SPI_connect();

	rv = SPI_execute_with_args(query, 1, argtypes, values, nulls, false, 1);

	if (rv != SPI_OK_SELECT)
		elog(ERROR, "could not load consumer state %d", id);

	slot = MakeSingleTupleTableSlot(SPI_tuptable->tupdesc);
	ExecStoreTuple(SPI_tuptable->vals[0], slot, InvalidBuffer, false);

	old = MemoryContextSwitchTo(CacheMemoryContext);

	state->consumer_id = id;
	state->endpoint_name =
		TextDatumGetCString(slot_getattr(slot, 1, &isnull));
	state->endpoint_region =
		TextDatumGetCString(slot_getattr(slot, 2, &isnull));

	d = slot_getattr(slot, 3, &isnull);

	if (!isnull)
		state->endpoint_credfile = TextDatumGetCString(d);

	d = slot_getattr(slot, 4, &isnull);

	if (!isnull)
		state->endpoint_url = TextDatumGetCString(d);

	/* skip 5 == endpoint name */

	state->kinesis_stream = TextDatumGetCString(slot_getattr(slot, 6, &isnull));
	state->relation = TextDatumGetCString(slot_getattr(slot, 7, &isnull));
	state->format = TextDatumGetCString(slot_getattr(slot, 8, &isnull));
	state->delimiter = TextDatumGetCString(slot_getattr(slot, 9, &isnull));

	d = slot_getattr(slot, 10, &isnull);
	state->quote = isnull ? NULL : TextDatumGetCString(d);

	d = slot_getattr(slot, 11, &isnull);
	state->escape = isnull ? NULL : TextDatumGetCString(d);

	state->batchsize = DatumGetInt32(slot_getattr(slot, 12, &isnull));

	MemoryContextSwitchTo(old);

	SPI_finish();
}

/*
 * find_shard_id
 *
 * Locate the index of a particular shard given its shard id
 */
static int
find_shard_id(KinesisConsumerState *state, const char *id)
{
	int i = 0;
	int num_shards = state->num_shards;

	for (i = 0; i < num_shards; ++i)
	{
		if (strcmp(state->shards[i].id, id) == 0)
			return i;
	}

	return -1;
}


/*
 * count_shards
 *
 * Determine how many shards this worker handles
 */
static int
count_shards(int num_shards, int p, int id)
{
	int s = 0;
	int i = 0;

	for (i = 0; i < num_shards; ++i)
	{
		s += (i % p) == id;
	}

	return s;
}

/*
 * load_shard_state
 *
 * Given a partially setup consumer state and some stream metadata,
 * initialize and load shard state (seqnums) from the kinesis tables
 */
static void
load_shard_state(KinesisConsumerState *state, kinesis_stream_metadata *meta)
{
	const char *query = "SELECT * FROM pipeline_kinesis.seqnums "
		"WHERE consumer_id = $1";

	int num_shards = kinesis_stream_metadata_get_num_shards(meta);
	int i = 0;
	Oid argtypes[1] = { INT4OID };
	Datum values[1];
    char nulls[1];
	int rv = 0;
	TupleTableSlot *slot = NULL;
	int out_i = 0;

	MemoryContext old = MemoryContextSwitchTo(CacheMemoryContext);

	state->num_shards = count_shards(num_shards, state->num_procs, state->proc_id);
	state->shards = palloc0(state->num_shards * sizeof(ShardState));

	for (i = 0; i < num_shards; ++i)
	{
		const kinesis_shard_metadata *smeta;

		if ((i % state->num_procs) != state->proc_id)
			continue;

		smeta = kinesis_stream_metadata_get_shard(meta, i);

		state->shards[out_i].seqnum = makeStringInfo();
		state->shards[out_i].id = pstrdup(kinesis_shard_metadata_get_id(smeta));

		out_i++;
	}

	MemoryContextSwitchTo(old);

	MemSet(nulls, ' ', sizeof(nulls));
	values[0] = Int32GetDatum(state->consumer_id);

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

		d = slot_getattr(slot, 2, &isnull);
		shard_id = TextDatumGetCString(d);

		out_ind = find_shard_id(state, shard_id);

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

/*
 * save_consumer_state
 *
 * Save consumer shard sequence numbers
 */
static void
save_consumer_state(KinesisConsumerState *state)
{
	int i = 0;
	const char *query = "INSERT INTO pipeline_kinesis.seqnums VALUES "
		"($1, $2, $3) ON CONFLICT(consumer_id, shard_id) "
		"DO UPDATE SET (seqnum) = ($3);";

	Oid argtypes[3] = { INT4OID, TEXTOID, TEXTOID };

	Datum values[3];
    char nulls[3];
	SPIPlanPtr ptr;

	MemSet(nulls, ' ', sizeof(nulls));

	SPI_connect();

	ptr = SPI_prepare(query, 3, argtypes);
	Assert(ptr);

	for (i = 0; i < state->num_shards; ++i)
	{
		int rv = 0;

		values[0] = Int32GetDatum(state->consumer_id);
		values[1] = CStringGetTextDatum(state->shards[i].id);
		values[2] = CStringGetTextDatum(state->shards[i].seqnum->data);

		rv = SPI_execute_plan(ptr, values, nulls, false, 0);

		if (rv != SPI_OK_INSERT)
			elog(ERROR, "could not update seqnums %d", rv);
	}

	SPI_finish();
}

/*
 * format_seqnum
 *
 * Convert saved sequence number (if any) and start offset into a
 * formatted sequence number
 */
static void
format_seqnum(StringInfo out, StringInfo in, int offset)
{
	resetStringInfo(out);

	if (in->len != 0)
	{
		appendStringInfoString(out, "after_sequence_number:");
		appendBinaryStringInfo(out, in->data, in->len);
	}
	else if (offset == -2)
	{
		appendStringInfoString(out, "trim_horizon");
	}
	else if (offset == -1)
	{
		appendStringInfoString(out, "latest");
	}
}

/*
 * kinesis_consume_main
 *
 * Kinesis bgworker main loop.
 */
void
kinesis_consume_main(Datum arg)
{
	int si;
	int i;
	CopyStmt *copy;
	int id;
	bool found = false;
	KinesisConsumerInfo *cinfo;
	kinesis_client *client;
	kinesis_stream_metadata *stream_meta;
	StringInfo batch_buffer;
	KinesisConsumerState state;
	StringInfo seqbuf;

	pqsignal(SIGTERM, kinesis_consume_main_sigterm);
	pqsignal(SIGSEGV, debug_segfault);

	BackgroundWorkerUnblockSignals();

	MemSet(&state, 0, sizeof(KinesisConsumerState));
	memcpy(&state.proc_id, MyBgworkerEntry->bgw_extra, sizeof(int));

	id = Int32GetDatum(arg);
	cinfo = hash_search(consumer_info, &id, HASH_FIND, &found);

	if (!found)
		elog(ERROR, "could not find consumer info for %d %d", id,
				state.proc_id);

	Assert(cinfo);

	BackgroundWorkerInitializeConnectionByOid(cinfo->dboid, 0);
	state.num_procs = cinfo->num_procs;

	/* Load relevant metadata from kinesis tables, and grab the
	 * stream metadata from AWS */

	StartTransactionCommand();

	load_consumer_state(&state, id);
	copy = get_copy_statement(&state);

	kinesis_set_logger(NULL, log_fn, "warn");

	client = kinesis_client_create(state.endpoint_region,
			state.endpoint_credfile,
			state.endpoint_url);

	if (!client)
		elog(ERROR, "failed to create kinesis client for endpoint %s %s %s %s",
				state.endpoint_name,
				state.endpoint_region,
				state.endpoint_credfile,
				state.endpoint_url);

	stream_meta =
		kinesis_client_create_stream_metadata(client, state.kinesis_stream);

	if (!stream_meta)
		elog(ERROR, "failed to get kinesis metadata for %s",
				state.kinesis_stream);

	load_shard_state(&state, stream_meta);

	kinesis_client_destroy_stream_metadata(stream_meta);
	CommitTransactionCommand();

	seqbuf = makeStringInfo();

	/* Create all the shard consumers and start them */

	for (si = 0; si < state.num_shards; ++si)
	{
		format_seqnum(seqbuf, state.shards[si].seqnum, cinfo->start_seq);

		state.shards[si].kc = kinesis_consumer_create(client,
				state.kinesis_stream,
				state.shards[si].id,
				seqbuf->data,
				state.batchsize);

		if (!state.shards[si].kc)
			elog(ERROR, "could not create consumer");

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

			/* timeout */
			if (!batch)
				continue;

			size = kinesis_batch_get_size(batch);

			for (i = 0; i < size; ++i)
			{
				const kinesis_record *r = kinesis_batch_get_record(batch, i);
				int n = kinesis_record_get_data_size(r);
				const uint8_t *d = kinesis_record_get_data(r);
				const char *seq = kinesis_record_get_sequence_number(r);

				appendBinaryStringInfo(batch_buffer, (const char *) d, n);
				appendStringInfoChar(batch_buffer, '\n');

				resetStringInfo(shard->seqnum);
				appendStringInfoString(shard->seqnum, seq);
			}

			kinesis_batch_destroy(batch);
		}

		if (batch_buffer->len == 0)
			continue;

		StartTransactionCommand();

		PG_TRY();
		{
			execute_copy(copy, batch_buffer);
		}
		PG_CATCH();
		{
			elog(LOG, "failed to process batch");

			EmitErrorReport();
			FlushErrorState();
			AbortCurrentTransaction();
		}
		PG_END_TRY();

		if (!IsTransactionState())
			StartTransactionCommand();

		save_consumer_state(&state);
		CommitTransactionCommand();
	}

	/* This terminates and joins all the threads */

	for (si = 0; si < state.num_shards; ++si)
		kinesis_consumer_destroy(state.shards[si].kc);

	kinesis_client_destroy(client);
}

/*
 * launch_worker
 *
 * Setup and launch parallel bgworkers to consume kinesis shards
 */
static void
launch_worker(int id, int para, int seq)
{
	int i = 0;
	bool found = false;

	KinesisConsumerInfo *info =
		hash_search(consumer_info, &id, HASH_ENTER, &found);

	if (found)
		return;

	if (para > MAX_PROCS)
	{
		elog(LOG, "capping parallelism %d to %d for consumer %d",
				para, MAX_PROCS, id);
		para = MAX_PROCS;
	}

	info->id = id;
	info->dboid = MyDatabaseId;
	info->start_seq = seq;
	info->num_procs = para;

	for (i = 0; i < para; ++i)
	{
		BackgroundWorker worker;
		BackgroundWorkerHandle *handle;

		worker.bgw_main_arg = Int32GetDatum(id);
		memcpy(worker.bgw_extra, &i, sizeof(int));

		worker.bgw_flags = BGWORKER_BACKEND_DATABASE_CONNECTION |
			BGWORKER_SHMEM_ACCESS;
		worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
		worker.bgw_restart_time = BGW_NEVER_RESTART;
		worker.bgw_main = NULL;
		worker.bgw_notify_pid = 0;

		sprintf(worker.bgw_library_name, "pipeline_kinesis");
		sprintf(worker.bgw_function_name, "kinesis_consume_main");

		snprintf(worker.bgw_name, BGW_MAXLEN, "[kinesis consumer] %d %d", id, i);

		if (!RegisterDynamicBackgroundWorker(&worker, &handle))
			elog(ERROR, "could not launch worker");

		info->handle[i] = *handle;
	}
}

/*
 * acquire_consumer_lock
 *
 * Open and return the consumers relation (use AccessExclusiveLock)
 */
static Relation
acquire_consumer_lock()
{
	Relation consumers =
		heap_openrv(makeRangeVar("pipeline_kinesis", "consumers", -1),
				AccessExclusiveLock);

	return consumers;
}

/*
 * drop_consumer_lock
 *
 * Drop the consumers relation with NoLock
 */
static void
drop_consumer_lock(Relation rel)
{
	heap_close(rel, NoLock);
}

/*
 * kinesis_consume_begin_sr
 *
 * creates/updates an entry in pipeline_kinesis.consumers table
 * launches a bgworker if neccessary
 */
PG_FUNCTION_INFO_V1(kinesis_consume_begin_sr);
Datum
kinesis_consume_begin_sr(PG_FUNCTION_ARGS)
{
	const char *query = "INSERT INTO pipeline_kinesis.consumers VALUES "
		"(default,$1,$2,$3,$4,$5,$6,$7,$8,$9) "
		"ON CONFLICT(endpoint, "stream", relation) "
		"DO UPDATE SET (format,delimiter,quote,escape,batchsize,parallelism) = "
		"($4,$5,$6,$7,$8,$9) RETURNING id;";

	Relation lockrel;
	int rv;

    Oid argtypes[9] = { TEXTOID, TEXTOID, TEXTOID, TEXTOID, TEXTOID, TEXTOID,
		TEXTOID, INT4OID, INT4OID };
	Datum values[9];
    char nulls[9];

	TupleTableSlot *slot;
	bool isnull = false;
	Datum d;
	int id;
	int i;
	int para;
	int seq;

	MemSet(nulls, ' ', sizeof(nulls));

	lockrel = acquire_consumer_lock();

	if (PG_ARGISNULL(0))
		elog(ERROR, "endpoint cannot be null");
	if (PG_ARGISNULL(1))
		elog(ERROR, "stream cannot be null");
	if (PG_ARGISNULL(2))
		elog(ERROR, "relation cannot be null");

	/* endpoint */
	values[0] = PG_GETARG_DATUM(0);
	/* Kinesis source stream */
	values[1] = PG_GETARG_DATUM(1);
	/* PipelineDB destination stream */
	values[2] = PG_GETARG_DATUM(2);

	/* format, delimiter, quote, escape */
	for (i = 3; i < 7; i++)
	{
		if (PG_ARGISNULL(i))
			nulls[i] = 'n';
		else
			values[i] = PG_GETARG_DATUM(i);
	}

	/* batch size */
	if (PG_ARGISNULL(7))
		values[7] = Int32GetDatum(1000);
	else
		values[7] = PG_GETARG_DATUM(7);

	/* parallelism */
	if (PG_ARGISNULL(8))
		values[8] = 1;
	else
		values[8] = PG_GETARG_DATUM(8);

	if (PG_ARGISNULL(9))
		seq = -1;
	else
		seq = PG_GETARG_INT32(9);

	para = DatumGetInt32(values[8]);

	SPI_connect();

	rv = SPI_execute_with_args(query, 9, argtypes, values, nulls, false, 1);

	if (rv != SPI_OK_INSERT_RETURNING)
		elog(ERROR, "could not create consumer %d", rv);

	slot = MakeSingleTupleTableSlot(SPI_tuptable->tupdesc);
	ExecStoreTuple(SPI_tuptable->vals[0], slot, InvalidBuffer, false);

	d = slot_getattr(slot, 1, &isnull);
	id = DatumGetInt32(d);

	launch_worker(id, para, seq);
	SPI_finish();

	drop_consumer_lock(lockrel);

	RETURN_SUCCESS();
}

/*
 * find_consumer
 *
 * find id for a kinesis consumer matching (endpoint, stream, relation)
 */
static int
find_consumer(Datum endpoint, Datum stream, Datum relation)
{
	const char *query = "SELECT id FROM pipeline_kinesis.consumers "
		"WHERE endpoint = $1 AND stream = $2 AND relation = $3;";

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

	MemSet(nulls, ' ', sizeof(nulls));

	rv = SPI_execute_with_args(query, 4, argtypes, values, nulls, false, 1);

	if (rv != SPI_OK_SELECT)
		elog(ERROR, "find_consumer query failure %d", rv);

	if (SPI_processed == 0)
		return 0;

	slot = MakeSingleTupleTableSlot(SPI_tuptable->tupdesc);
	ExecStoreTuple(SPI_tuptable->vals[0], slot, InvalidBuffer, false);

	isnull = false;
	d = slot_getattr(slot, 1, &isnull);
	Assert(!isnull);

	return DatumGetInt32(d);
}

/*
 * kinesis_consume_end_sr
 *
 * Stop consumer matching (endpoint, stream, relation).
 */
PG_FUNCTION_INFO_V1(kinesis_consume_end_sr);
Datum
kinesis_consume_end_sr(PG_FUNCTION_ARGS)
{
	Relation lockrel = acquire_consumer_lock();
	int id;
	bool found = false;
	KinesisConsumerInfo *info;
	int i = 0;

	SPI_connect();

	id = find_consumer(PG_GETARG_DATUM(0),
						PG_GETARG_DATUM(1),
						PG_GETARG_DATUM(2));

	if (id == 0)
		elog(ERROR, "could not find consumer");

	info = hash_search(consumer_info, &id, HASH_FIND, &found);
	Assert(found);
	Assert(info);

	for (i = 0; i < info->num_procs; ++i)
		TerminateBackgroundWorker(&info->handle[i]);

	hash_search(consumer_info, &id, HASH_REMOVE, &found);

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

	const char *query = "SELECT id,parallelism FROM pipeline_kinesis.consumers;";
	int rv = 0;
	int i = 0;
	TupleTableSlot *slot;
	Datum d;
	Oid oid;
	bool isnull;
	int para;

	SPI_connect();

	rv = SPI_execute(query, false, 0);

	if (rv != SPI_OK_SELECT)
		elog(ERROR, "could not select consumers");

	slot = MakeSingleTupleTableSlot(SPI_tuptable->tupdesc);

	for (i = 0; i < SPI_processed; ++i)
	{
		ExecStoreTuple(SPI_tuptable->vals[i], slot, InvalidBuffer, false);
		d = slot_getattr(slot, 1, &isnull);

		oid = DatumGetInt32(d);

		d = slot_getattr(slot, 2, &isnull);
		para = DatumGetInt32(d);

		launch_worker(oid, para, -1);
	}

	SPI_finish();

	drop_consumer_lock(lockrel);
	RETURN_SUCCESS();
}

/*
 * kinesis_consume_end_all
 *
 * Stop all consumers
 */
PG_FUNCTION_INFO_V1(kinesis_consume_end_all);
Datum
kinesis_consume_end_all(PG_FUNCTION_ARGS)
{
	Relation lockrel = acquire_consumer_lock();

	HASH_SEQ_STATUS iter;
	List *ids = NIL;
	ListCell *lc;
	KinesisConsumerInfo *info;
	int i;

	hash_seq_init(&iter, consumer_info);

	while ((info = (KinesisConsumerInfo *) hash_seq_search(&iter)) != NULL)
	{
		for (i = 0; i < info->num_procs; ++i)
		{
			TerminateBackgroundWorker(&info->handle[i]);
		}

		ids = lappend_int(ids, info->id);
	}

	foreach(lc, ids)
	{
		int id = lfirst_int(lc);
		hash_search(consumer_info, &id, HASH_REMOVE, NULL);
	}

	drop_consumer_lock(lockrel);
	RETURN_SUCCESS();
}
