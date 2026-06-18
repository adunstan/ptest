/*-------------------------------------------------------------------------
 *
 * cs_slot.c
 *	  Custom TupleTableSlot implementation for columnstore tables.
 *
 * CSTupleTableSlot supports lazy column decompression.  For columnar
 * rows, the getsomeattrs callback decompresses only the columns that
 * the executor actually requests, avoiding I/O and CPU work for columns
 * that are never referenced by the query.
 *
 * Delta store rows are handled like virtual tuples -- all attributes
 * are filled eagerly by the scan, so getsomeattrs simply validates.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/columnstore/cs_slot.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cs_internal.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/xact.h"
#include "executor/tuptable.h"
#include "storage/bufmgr.h"
#include "utils/expandeddatum.h"
#include "utils/numeric.h"

static void tts_columnstore_init(TupleTableSlot *slot);
static void tts_columnstore_release(TupleTableSlot *slot);
static void tts_columnstore_clear(TupleTableSlot *slot);
static void tts_columnstore_getsomeattrs(TupleTableSlot *slot, int natts);
static bool tts_columnstore_read_delta_header(TupleTableSlot *slot,
											  TransactionId *xmin,
											  TransactionId *xmax,
											  CommandId *cid);
static Datum tts_columnstore_getsysattr(TupleTableSlot *slot, int attnum,
										bool *isnull);
static bool tts_columnstore_is_current_xact_tuple(TupleTableSlot *slot);
static void tts_columnstore_materialize(TupleTableSlot *slot);
static void tts_columnstore_copyslot(TupleTableSlot *dstslot,
									 TupleTableSlot *srcslot);
static HeapTuple tts_columnstore_copy_heap_tuple(TupleTableSlot *slot);
static MinimalTuple tts_columnstore_copy_minimal_tuple(TupleTableSlot *slot,
													   Size extra);

static void
tts_columnstore_init(TupleTableSlot *slot)
{
	CSTupleTableSlot *csslot = (CSTupleTableSlot *) slot;

	csslot->data = NULL;
	csslot->cs_scan = NULL;
	csslot->cs_rel = NULL;
	csslot->cs_colcache = NULL;
	csslot->cs_row = 0;
	csslot->cs_is_columnar = false;
}

static void
tts_columnstore_release(TupleTableSlot *slot)
{
}

static void
tts_columnstore_clear(TupleTableSlot *slot)
{
	CSTupleTableSlot *csslot = (CSTupleTableSlot *) slot;

	if (unlikely(TTS_SHOULDFREE(slot)))
	{
		pfree(csslot->data);
		csslot->data = NULL;
		slot->tts_flags &= ~TTS_FLAG_SHOULDFREE;
	}

	csslot->cs_scan = NULL;
	csslot->cs_colcache = NULL;
	csslot->cs_is_columnar = false;
	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;
	ItemPointerSetInvalid(&slot->tts_tid);
}

/*
 * Lazy column decompression callback.
 *
 * For columnar rows, decompress columns on demand up to the requested natts.
 * For delta rows (or non-scan contexts), all attrs are already valid.
 */
static void
tts_columnstore_getsomeattrs(TupleTableSlot *slot, int natts)
{
	CSTupleTableSlot *csslot = (CSTupleTableSlot *) slot;
	CSColumnCache *cache;
	CScanDesc	scan;
	Relation	rel;
	MemoryContext oldcxt;
	int			col;

	/*
	 * Callers must not ask for more attributes than the slot's descriptor has
	 * (the column-cache arrays below are sized to the relation's attribute
	 * count).
	 */
	Assert(natts <= slot->tts_tupleDescriptor->natts);

	if (!csslot->cs_is_columnar || csslot->cs_colcache == NULL)
	{
		/*
		 * Delta row or non-scan use: all attrs were filled eagerly. Just mark
		 * them all valid.
		 */
		slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
		return;
	}

	/*
	 * Columnar row: decompress columns lazily.
	 *
	 * For seq/bitmap scans, cs_scan provides the Relation and projection
	 * info.  For index scans, cs_scan is NULL and cs_rel provides the
	 * Relation for on-demand column decompression.
	 *
	 * Switch to the slot's memory context for column buffer allocations since
	 * getsomeattrs may be called from the per-tuple context.
	 */
	cache = csslot->cs_colcache;

	/*
	 * Record the descriptor the executor handed us as the authoritative
	 * stored format for the decoders: during an ALTER TABLE rewrite it is the
	 * OLD descriptor, while RelationGetDescr() already shows the new column
	 * types (see CSColumnCache.cc_tupdesc).
	 */
	cache->cc_tupdesc = slot->tts_tupleDescriptor;
	scan = csslot->cs_scan;

	if (scan != NULL)
		rel = scan->rs_base.rs_rd;
	else
		rel = csslot->cs_rel;
	oldcxt = MemoryContextSwitchTo(slot->tts_mcxt);

	if (scan != NULL && scan->needed_col_list != NULL)
	{
		/*
		 * Fast path with projection pushdown.
		 *
		 * Iterate only the pre-computed needed column list -- O(needed) per
		 * row instead of O(natts).  Unneeded columns were pre-set to NULL at
		 * the start of each row group by cs_columnar_getnext and persist
		 * across rows since ExecClearTuple does not zero
		 * tts_values/tts_isnull.
		 */
		for (int i = 0; i < scan->needed_col_count; i++)
		{
			col = scan->needed_col_list[i];
			if (col >= natts)
				break;

			/*
			 * Point-read optimization: for FOR/NI64 columns that haven't been
			 * fully decoded yet, extract just the needed value instead of
			 * decoding the entire column (100K values).  This is a large win
			 * when few rows match a selective filter but many columns must be
			 * materialized (e.g. SELECT * ... LIMIT).
			 */
			if (!cache->col_loaded[col])
			{
				CSColumnChunkDesc *chk = &cache->cur_rg_desc->rg_columns[col];
				uint8		comp = chk->cc_compression;

				if ((CS_PREENC(comp) == CS_PREENC_FOR ||
					 CS_PREENC(comp) == CS_PREENC_NI64 ||
					 CS_PREENC(comp) == CS_PREENC_NI64_FOR) &&
					cache->col_point_reads[col] < CS_POINT_READ_THRESHOLD)
				{
					cs_base_decompress_column(cache, rel, col);
					slot->tts_values[col] =
						cs_column_point_read(cache, rel, col,
											 csslot->cs_row,
											 &slot->tts_isnull[col]);
					cache->col_point_reads[col]++;
					continue;
				}
			}

			cs_ensure_column_loaded(cache, rel, col);

			if (cache->col_dict && cache->col_dict[col] != NULL)
			{
				/* Dictionary-encoded column */
				CSDictColumn *dict = cache->col_dict[col];
				uint32		idx;

				switch (dict->index_width)
				{
					case 1:
						idx = ((uint8 *) dict->index_data)[csslot->cs_row];
						break;
					case 2:
						idx = cs_read_u16((const char *) dict->index_data + (Size) (csslot->cs_row) * sizeof(uint16));
						break;
					default:
						idx = cs_read_u32((const char *) dict->index_data + (Size) (csslot->cs_row) * sizeof(uint32));
						break;
				}

				if (dict->has_null && idx == dict->dict_count)
				{
					slot->tts_isnull[col] = true;
					slot->tts_values[col] = (Datum) 0;
				}
				else
				{
					slot->tts_isnull[col] = false;
					slot->tts_values[col] = dict->dict_values[idx];
				}
			}
			else if (cache->col_values[col] != NULL)
			{
				slot->tts_values[col] = cache->col_values[col][csslot->cs_row];
				slot->tts_isnull[col] = cache->col_nulls[col][csslot->cs_row];
			}
			else
			{
				CompactAttribute *att = TupleDescCompactAttr(slot->tts_tupleDescriptor, col);

				if (cache->col_null_bitmap[col] != NULL)
				{
					if (CS_ISNULL(cache->col_null_bitmap[col], csslot->cs_row))
					{
						slot->tts_isnull[col] = true;
						slot->tts_values[col] = (Datum) 0;
						continue;
					}
				}

				slot->tts_isnull[col] = false;
				if (cache->col_ni64_buf[col] != NULL)
				{
					int64		ival = ((int64 *) cache->col_raw_data[col])[csslot->cs_row];

					slot->tts_values[col] =
						cs_int64_to_numeric_buf(ival,
												(int) cache->col_ni64_dscale[col],
												cache->col_ni64_buf[col]);
				}
				else
				{
					slot->tts_values[col] =
						cs_fetch_att(cache->col_raw_data[col] +
									 (Size) csslot->cs_row * att->attlen,
									 true, att->attlen);
				}
			}
		}
	}
	else
	{
		/*
		 * Standard path without projection: load all columns in the requested
		 * range sequentially.
		 */
		for (col = slot->tts_nvalid; col < natts; col++)
		{
			/* Point-read optimization (see fast path above) */
			if (!cache->col_loaded[col])
			{
				CSColumnChunkDesc *chk = &cache->cur_rg_desc->rg_columns[col];
				uint8		comp = chk->cc_compression;

				if ((CS_PREENC(comp) == CS_PREENC_FOR ||
					 CS_PREENC(comp) == CS_PREENC_NI64 ||
					 CS_PREENC(comp) == CS_PREENC_NI64_FOR) &&
					cache->col_point_reads[col] < CS_POINT_READ_THRESHOLD)
				{
					cs_base_decompress_column(cache, rel, col);
					slot->tts_values[col] =
						cs_column_point_read(cache, rel, col,
											 csslot->cs_row,
											 &slot->tts_isnull[col]);
					cache->col_point_reads[col]++;
					continue;
				}
			}

			cs_ensure_column_loaded(cache, rel, col);

			if (cache->col_dict && cache->col_dict[col] != NULL)
			{
				/* Dictionary-encoded column */
				CSDictColumn *dict = cache->col_dict[col];
				uint32		idx;

				switch (dict->index_width)
				{
					case 1:
						idx = ((uint8 *) dict->index_data)[csslot->cs_row];
						break;
					case 2:
						idx = cs_read_u16((const char *) dict->index_data + (Size) (csslot->cs_row) * sizeof(uint16));
						break;
					default:
						idx = cs_read_u32((const char *) dict->index_data + (Size) (csslot->cs_row) * sizeof(uint32));
						break;
				}

				if (dict->has_null && idx == dict->dict_count)
				{
					slot->tts_isnull[col] = true;
					slot->tts_values[col] = (Datum) 0;
				}
				else
				{
					slot->tts_isnull[col] = false;
					slot->tts_values[col] = dict->dict_values[idx];
				}
			}
			else if (cache->col_values[col] != NULL)
			{
				slot->tts_values[col] = cache->col_values[col][csslot->cs_row];
				slot->tts_isnull[col] = cache->col_nulls[col][csslot->cs_row];
			}
			else
			{
				CompactAttribute *att = TupleDescCompactAttr(slot->tts_tupleDescriptor, col);

				if (cache->col_null_bitmap[col] != NULL)
				{
					if (CS_ISNULL(cache->col_null_bitmap[col], csslot->cs_row))
					{
						slot->tts_isnull[col] = true;
						slot->tts_values[col] = (Datum) 0;
						continue;
					}
				}

				slot->tts_isnull[col] = false;
				if (cache->col_ni64_buf[col] != NULL)
				{
					int64		ival = ((int64 *) cache->col_raw_data[col])[csslot->cs_row];

					slot->tts_values[col] =
						cs_int64_to_numeric_buf(ival,
												(int) cache->col_ni64_dscale[col],
												cache->col_ni64_buf[col]);
				}
				else
				{
					slot->tts_values[col] =
						cs_fetch_att(cache->col_raw_data[col] +
									 (Size) csslot->cs_row * att->attlen,
									 true, att->attlen);
				}
			}
		}
	}

	MemoryContextSwitchTo(oldcxt);

	slot->tts_nvalid = natts;
}

/*
 * Read the heap header fields of the delta tuple this slot was filled
 * from, by TID.  Returns false if the TID no longer references a delta
 * tuple (e.g. the row has been compacted into columnar form, which can
 * only happen once it is visible to every transaction).
 */
static bool
tts_columnstore_read_delta_header(TupleTableSlot *slot,
								  TransactionId *xmin, TransactionId *xmax,
								  CommandId *cid)
{
	Relation	rel;
	Buffer		buf;
	Page		page;
	ItemId		itemid;
	HeapTupleHeader htup;
	bool		found = false;

	if (!ItemPointerIsValid(&slot->tts_tid) ||
		!OidIsValid(slot->tts_tableOid))
		return false;

	/* the executor holds a lock on the relation already */
	rel = relation_open(slot->tts_tableOid, NoLock);

	if (ItemPointerGetBlockNumber(&slot->tts_tid) <
		RelationGetNumberOfBlocks(rel))
	{
		buf = ReadBuffer(rel, ItemPointerGetBlockNumber(&slot->tts_tid));
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);

		if (CSPageIsDelta(page))
		{
			itemid = PageGetItemId(page,
								   ItemPointerGetOffsetNumber(&slot->tts_tid));
			if (ItemIdIsNormal(itemid))
			{
				htup = (HeapTupleHeader) PageGetItem(page, itemid);
				*xmin = HeapTupleHeaderGetRawXmin(htup);
				*xmax = (htup->t_infomask & HEAP_XMAX_INVALID) ?
					InvalidTransactionId : HeapTupleHeaderGetRawXmax(htup);
				*cid = HeapTupleHeaderGetRawCommandId(htup);
				found = true;
			}
		}
		UnlockReleaseBuffer(buf);
	}

	relation_close(rel, NoLock);
	return found;
}

/*
 * System columns for columnstore rows.
 *
 * Columnar rows carry no transaction information: compaction only moves
 * rows already visible to every snapshot, so they behave as frozen.
 * Delta rows are heap-format; their header is re-read by TID.  This is
 * what lets ExecOnConflictUpdate's self-insertion check (xmin) work.
 */
static Datum
tts_columnstore_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	TransactionId xmin = FrozenTransactionId;
	TransactionId xmax = InvalidTransactionId;
	CommandId	cid = FirstCommandId;

	Assert(!TTS_EMPTY(slot));

	*isnull = false;

	if (attnum == SelfItemPointerAttributeNumber)
		return PointerGetDatum(&slot->tts_tid);
	if (attnum == TableOidAttributeNumber)
		return ObjectIdGetDatum(slot->tts_tableOid);

	if (ItemPointerIsValid(&slot->tts_tid) &&
		ItemPointerGetBlockNumber(&slot->tts_tid) < CS_COLUMNAR_BLKNO_BASE)
		(void) tts_columnstore_read_delta_header(slot, &xmin, &xmax, &cid);

	switch (attnum)
	{
		case MinTransactionIdAttributeNumber:
			return TransactionIdGetDatum(xmin);
		case MaxTransactionIdAttributeNumber:
			return TransactionIdGetDatum(xmax);
		case MinCommandIdAttributeNumber:
		case MaxCommandIdAttributeNumber:
			/* as in heap: cmin and cmax are not distinguishable here */
			return CommandIdGetDatum(cid);
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot retrieve a system column in this context")));
			return 0;			/* keep compiler quiet */
	}
}

static bool
tts_columnstore_is_current_xact_tuple(TupleTableSlot *slot)
{
	TransactionId xmin = FrozenTransactionId;
	TransactionId xmax = InvalidTransactionId;
	CommandId	cid = FirstCommandId;

	Assert(!TTS_EMPTY(slot));

	/*
	 * Columnar rows are visible to every transaction by construction, so they
	 * cannot have been written by the current one.
	 */
	if (!ItemPointerIsValid(&slot->tts_tid) ||
		ItemPointerGetBlockNumber(&slot->tts_tid) >= CS_COLUMNAR_BLKNO_BASE)
		return false;

	if (!tts_columnstore_read_delta_header(slot, &xmin, &xmax, &cid))
		return false;

	return TransactionIdIsCurrentTransactionId(xmin);
}

/*
 * Materialize the slot: copy all by-reference datums into slot's memory.
 * Must ensure all attributes are loaded first.
 */
static void
tts_columnstore_materialize(TupleTableSlot *slot)
{
	CSTupleTableSlot *csslot = (CSTupleTableSlot *) slot;
	TupleDesc	desc = slot->tts_tupleDescriptor;
	Size		sz = 0;
	char	   *data;
	int			natt;

	/* already materialized */
	if (TTS_SHOULDFREE(slot))
		return;

	/* ensure all columns are loaded */
	slot_getsomeattrs(slot, desc->natts);

	/* compute size of memory required */
	for (natt = 0; natt < desc->natts; natt++)
	{
		CompactAttribute *att = TupleDescCompactAttr(desc, natt);
		Datum		val;

		if (att->attbyval || slot->tts_isnull[natt])
			continue;

		val = slot->tts_values[natt];

		if (att->attlen == -1 &&
			VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			sz = att_nominal_alignby(sz, att->attalignby);
			sz += EOH_get_flat_size(DatumGetEOHP(val));
		}
		else
		{
			sz = att_nominal_alignby(sz, att->attalignby);
			sz = att_addlength_datum(sz, att->attlen, val);
		}
	}

	/* all data is byval */
	if (sz == 0)
	{
		/* Detach from scan so slot is self-contained */
		csslot->cs_scan = NULL;
		csslot->cs_colcache = NULL;
		csslot->cs_is_columnar = false;
		return;
	}

	/* allocate memory */
	csslot->data = data = MemoryContextAlloc(slot->tts_mcxt, sz);
	slot->tts_flags |= TTS_FLAG_SHOULDFREE;

	/* copy all by-reference attributes into the pre-allocated space */
	for (natt = 0; natt < desc->natts; natt++)
	{
		CompactAttribute *att = TupleDescCompactAttr(desc, natt);
		Datum		val;

		if (att->attbyval || slot->tts_isnull[natt])
			continue;

		val = slot->tts_values[natt];

		if (att->attlen == -1 &&
			VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			ExpandedObjectHeader *eoh = DatumGetEOHP(val);
			Size		data_length;

			data = (char *) att_nominal_alignby(data, att->attalignby);
			data_length = EOH_get_flat_size(eoh);
			EOH_flatten_into(eoh, data, data_length);

			slot->tts_values[natt] = PointerGetDatum(data);
			data += data_length;
		}
		else
		{
			Size		data_length = 0;

			data = (char *) att_nominal_alignby(data, att->attalignby);
			data_length = att_addlength_datum(data_length, att->attlen, val);

			memcpy(data, DatumGetPointer(val), data_length);

			slot->tts_values[natt] = PointerGetDatum(data);
			data += data_length;
		}
	}

	/* Detach from scan so slot is self-contained */
	csslot->cs_scan = NULL;
	csslot->cs_colcache = NULL;
	csslot->cs_is_columnar = false;
}

static void
tts_columnstore_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	TupleDesc	srcdesc = srcslot->tts_tupleDescriptor;
	int			natt;

	tts_columnstore_clear(dstslot);

	slot_getallattrs(srcslot);

	for (natt = 0; natt < srcdesc->natts; natt++)
	{
		dstslot->tts_values[natt] = srcslot->tts_values[natt];
		dstslot->tts_isnull[natt] = srcslot->tts_isnull[natt];
	}

	dstslot->tts_nvalid = srcdesc->natts;
	dstslot->tts_flags &= ~TTS_FLAG_EMPTY;

	/* make sure storage doesn't depend on external memory */
	tts_columnstore_materialize(dstslot);
}

static HeapTuple
tts_columnstore_copy_heap_tuple(TupleTableSlot *slot)
{
	HeapTuple	tuple;

	Assert(!TTS_EMPTY(slot));

	slot_getallattrs(slot);

	tuple = heap_form_tuple(slot->tts_tupleDescriptor,
							slot->tts_values,
							slot->tts_isnull);

	/* Propagate TID so callers like ANALYZE's compare_rows can sort. */
	tuple->t_self = slot->tts_tid;
	tuple->t_tableOid = slot->tts_tableOid;

	return tuple;
}

static MinimalTuple
tts_columnstore_copy_minimal_tuple(TupleTableSlot *slot, Size extra)
{
	Assert(!TTS_EMPTY(slot));

	slot_getallattrs(slot);

	return heap_form_minimal_tuple(slot->tts_tupleDescriptor,
								   slot->tts_values,
								   slot->tts_isnull,
								   extra);
}

const TupleTableSlotOps TTSOpsColumnStore = {
	.base_slot_size = sizeof(CSTupleTableSlot),
	.init = tts_columnstore_init,
	.release = tts_columnstore_release,
	.clear = tts_columnstore_clear,
	.getsomeattrs = tts_columnstore_getsomeattrs,
	.getsysattr = tts_columnstore_getsysattr,
	.is_current_xact_tuple = tts_columnstore_is_current_xact_tuple,
	.materialize = tts_columnstore_materialize,
	.copyslot = tts_columnstore_copyslot,
	.get_heap_tuple = NULL,
	.get_minimal_tuple = NULL,
	.copy_heap_tuple = tts_columnstore_copy_heap_tuple,
	.copy_minimal_tuple = tts_columnstore_copy_minimal_tuple,
};
