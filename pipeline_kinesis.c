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
