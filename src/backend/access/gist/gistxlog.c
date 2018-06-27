/*-------------------------------------------------------------------------
 *
 * gistxlog.c
 *	  WAL replay logic for GiST.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			 src/backend/access/gist/gistxlog.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bufmask.h"
#include "access/gist_private.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "utils/memutils.h"

static MemoryContext opCtx;		/* working memory for operations */

/*
 * Replay the clearing of F_FOLLOW_RIGHT flag on a child page.
 *
 * Even if the WAL record includes a full-page image, we have to update the
 * follow-right flag, because that change is not included in the full-page
 * image.  To be sure that the intermediate state with the wrong flag value is
 * not visible to concurrent Hot Standby queries, this function handles
 * restoring the full-page image as well as updating the flag.  (Note that
 * we never need to do anything else to the child page in the current WAL
 * action.)
 */
static void
<<<<<<< HEAD
gistRedoClearFollowRight(XLogRecPtr lsn, XLogRecord *record, int block_index,
						 RelFileNode node, BlockNumber childblkno)
=======
gistRedoClearFollowRight(XLogReaderState *record, uint8 block_id)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
{
	XLogRecPtr	lsn = record->EndRecPtr;
	Buffer		buffer;
	Page		page;
<<<<<<< HEAD

	if (record->xl_info & XLR_BKP_BLOCK(block_index))
		buffer = RestoreBackupBlock(lsn, record, block_index, false, true);
	else
	{
		buffer = XLogReadBuffer(node, childblkno, false);
		if (!BufferIsValid(buffer))
			return;				/* page was deleted, nothing to do */
	}
	page = (Page) BufferGetPage(buffer);

	/*
	 * Note that we still update the page even if page LSN is equal to the LSN
	 * of this record, because the updated NSN is not included in the full
	 * page image.
	 */
	if (!XLByteLT(lsn, PageGetLSN(page)))
	{
		GistPageGetOpaque(page)->nsn = lsn;
=======
	XLogRedoAction action;

	/*
	 * Note that we still update the page even if it was restored from a full
	 * page image, because the updated NSN is not included in the image.
	 */
	action = XLogReadBufferForRedo(record, block_id, &buffer);
	if (action == BLK_NEEDS_REDO || action == BLK_RESTORED)
	{
		page = BufferGetPage(buffer);

		GistPageSetNSN(page, lsn);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		GistClearFollowRight(page);

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
<<<<<<< HEAD
	UnlockReleaseBuffer(buffer);
=======
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
}

/*
 * redo any page update (except page split)
 */
static void
gistRedoPageUpdateRecord(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	gistxlogPageUpdate *xldata = (gistxlogPageUpdate *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;

<<<<<<< HEAD
	/*
	 * We need to acquire and hold lock on target page while updating the left
	 * child page.  If we have a full-page image of target page, getting the
	 * lock is a side-effect of restoring that image.  Note that even if the
	 * target page no longer exists, we'll still attempt to replay the change
	 * on the child page.
	 */
	if (record->xl_info & XLR_BKP_BLOCK(0))
		buffer = RestoreBackupBlock(lsn, record, 0, false, true);
	else
		buffer = XLogReadBuffer(xldata->node, xldata->blkno, false);

	/* Fix follow-right data on left child page */
	if (BlockNumberIsValid(xldata->leftchild))
		gistRedoClearFollowRight(lsn, record, 1,
								 xldata->node, xldata->leftchild);

	/* Done if target page no longer exists */
	if (!BufferIsValid(buffer))
		return;

	/* nothing more to do if page was backed up (and no info to do it with) */
	if (IsBkpBlockApplied(record, 0))
	{
		UnlockReleaseBuffer(buffer);
		return;
	}

	page = (Page) BufferGetPage(buffer);

	/* nothing more to do if change already applied */
	if (XLByteLE(lsn, PageGetLSN(page)))
=======
	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	{
		char	   *begin;
		char	   *data;
		Size		datalen;
		int			ninserted = 0;

		data = begin = XLogRecGetBlockData(record, 0, &datalen);

		page = (Page) BufferGetPage(buffer);

		/* Delete old tuples */
		if (xldata->ntodelete > 0)
		{
			int			i;
			OffsetNumber *todelete = (OffsetNumber *) data;

			data += sizeof(OffsetNumber) * xldata->ntodelete;

			for (i = 0; i < xldata->ntodelete; i++)
				PageIndexTupleDelete(page, todelete[i]);
			if (GistPageIsLeaf(page))
				GistMarkTuplesDeleted(page);
		}

		/* add tuples */
		if (data - begin < datalen)
		{
			OffsetNumber off = (PageIsEmpty(page)) ? FirstOffsetNumber :
			OffsetNumberNext(PageGetMaxOffsetNumber(page));

			while (data - begin < datalen)
			{
				IndexTuple	itup = (IndexTuple) data;
				Size		sz = IndexTupleSize(itup);
				OffsetNumber l;

				data += sz;

				l = PageAddItem(page, (Item) itup, sz, off, false, false);
				if (l == InvalidOffsetNumber)
					elog(ERROR, "failed to add item to GiST index page, size %d bytes",
						 (int) sz);
				off++;
				ninserted++;
			}
		}

		Assert(ninserted == xldata->ntoinsert);

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}

	/*
	 * Fix follow-right data on left child page
	 *
	 * This must be done while still holding the lock on the target page. Note
	 * that even if the target page no longer exists, we still attempt to
	 * replay the change on the child page.
	 */
	if (XLogRecHasBlockRef(record, 1))
		gistRedoClearFollowRight(record, 1);

	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
<<<<<<< HEAD
		return;
	}

	data = begin + sizeof(gistxlogPageUpdate);

	/* Delete old tuples */
	if (xldata->ntodelete > 0)
	{
		int			i;
		OffsetNumber *todelete = (OffsetNumber *) data;

		data += sizeof(OffsetNumber) * xldata->ntodelete;

		for (i = 0; i < xldata->ntodelete; i++)
			PageIndexTupleDelete(page, todelete[i]);
		if (GistPageIsLeaf(page))
			GistMarkTuplesDeleted(page);
	}

	/* add tuples */
	if (data - begin < record->xl_len)
	{
		OffsetNumber off = (PageIsEmpty(page)) ? FirstOffsetNumber :
		OffsetNumberNext(PageGetMaxOffsetNumber(page));

		while (data - begin < record->xl_len)
		{
			IndexTuple	itup = (IndexTuple) data;
			Size		sz = IndexTupleSize(itup);
			OffsetNumber l;

			data += sz;

			l = PageAddItem(page, (Item) itup, sz, off, false, false);
			if (l == InvalidOffsetNumber)
				elog(ERROR, "failed to add item to GiST index page, size %d bytes",
					 (int) sz);
			off++;
		}
	}
	else
	{
		/*
		 * special case: leafpage, nothing to insert, nothing to delete, then
		 * vacuum marks page
		 */
		if (GistPageIsLeaf(page) && xldata->ntodelete == 0)
			GistClearTuplesDeleted(page);
	}

	if (!GistPageIsLeaf(page) &&
		PageGetMaxOffsetNumber(page) == InvalidOffsetNumber &&
		xldata->blkno == GIST_ROOT_BLKNO)
	{
		/*
		 * all links on non-leaf root page was deleted by vacuum full, so root
		 * page becomes a leaf
		 */
		GistPageSetLeaf(page);
	}

	GistPageGetOpaque(page)->rightlink = InvalidBlockNumber;
	PageSetLSN(page, lsn);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
}

static void
decodePageSplitRecord(PageSplitRecord *decoded, XLogRecord *record)
{
	char	   *begin = XLogRecGetData(record),
			   *ptr;
	int			j,
				i = 0;
=======
}

/*
 * Returns an array of index pointers.
 */
static IndexTuple *
decodePageSplitRecord(char *begin, int len, int *n)
{
	char	   *ptr;
	int			i = 0;
	IndexTuple *tuples;

	/* extract the number of tuples */
	memcpy(n, begin, sizeof(int));
	ptr = begin + sizeof(int);

	tuples = palloc(*n * sizeof(IndexTuple));
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	for (i = 0; i < *n; i++)
	{
		Assert(ptr - begin < len);
		tuples[i] = (IndexTuple) ptr;
		ptr += IndexTupleSize((IndexTuple) ptr);
	}
	Assert(ptr - begin == len);

	return tuples;
}

static void
gistRedoPageSplitRecord(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	gistxlogPageSplit *xldata = (gistxlogPageSplit *) XLogRecGetData(record);
<<<<<<< HEAD
	PageSplitRecord xlrec;
=======
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	Buffer		firstbuffer = InvalidBuffer;
	Buffer		buffer;
	Page		page;
	int			i;
	bool		isrootsplit = false;

<<<<<<< HEAD
	decodePageSplitRecord(&xlrec, record);
=======
	/*
	 * We must hold lock on the first-listed page throughout the action,
	 * including while updating the left child page (if any).  We can unlock
	 * remaining pages in the list as soon as they've been written, because
	 * there is no path for concurrent queries to reach those pages without
	 * first visiting the first-listed page.
	 */
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/*
	 * We must hold lock on the first-listed page throughout the action,
	 * including while updating the left child page (if any).  We can unlock
	 * remaining pages in the list as soon as they've been written, because
	 * there is no path for concurrent queries to reach those pages without
	 * first visiting the first-listed page.
	 */

	/* loop around all pages */
	for (i = 0; i < xldata->npage; i++)
	{
		int			flags;
		char	   *data;
		Size		datalen;
		int			num;
		BlockNumber blkno;
		IndexTuple *tuples;

		XLogRecGetBlockTag(record, i + 1, NULL, NULL, &blkno);
		if (blkno == GIST_ROOT_BLKNO)
		{
			Assert(i == 0);
			isrootsplit = true;
		}

		buffer = XLogInitBufferForRedo(record, i + 1);
		page = (Page) BufferGetPage(buffer);
		data = XLogRecGetBlockData(record, i + 1, &datalen);

		tuples = decodePageSplitRecord(data, datalen, &num);

		/* ok, clear buffer */
		if (xldata->origleaf && blkno != GIST_ROOT_BLKNO)
			flags = F_LEAF;
		else
			flags = 0;
		GISTInitBuffer(buffer, flags);

		/* and fill it */
		gistfillbuffer(page, tuples, num, FirstOffsetNumber);

		if (blkno == GIST_ROOT_BLKNO)
		{
			GistPageGetOpaque(page)->rightlink = InvalidBlockNumber;
			GistPageSetNSN(page, xldata->orignsn);
			GistClearFollowRight(page);
		}
		else
		{
			if (i < xldata->npage - 1)
			{
				BlockNumber nextblkno;

				XLogRecGetBlockTag(record, i + 2, NULL, NULL, &nextblkno);
				GistPageGetOpaque(page)->rightlink = nextblkno;
			}
			else
				GistPageGetOpaque(page)->rightlink = xldata->origrlink;
			GistPageSetNSN(page, xldata->orignsn);
			if (i < xldata->npage - 1 && !isrootsplit &&
				xldata->markfollowright)
				GistMarkFollowRight(page);
			else
				GistClearFollowRight(page);
		}

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);

		if (i == 0)
			firstbuffer = buffer;
		else
			UnlockReleaseBuffer(buffer);
	}

	/* Fix follow-right data on left child page, if any */
<<<<<<< HEAD
	if (BlockNumberIsValid(xldata->leftchild))
		gistRedoClearFollowRight(lsn, record, 0,
								 xldata->node, xldata->leftchild);
=======
	if (XLogRecHasBlockRef(record, 0))
		gistRedoClearFollowRight(record, 0);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* Finally, release lock on the first page */
	UnlockReleaseBuffer(firstbuffer);
}

static void
gistRedoCreateIndex(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	Buffer		buffer;
	Page		page;

<<<<<<< HEAD
	/* Backup blocks are not used in create_index records */
	Assert(!(record->xl_info & XLR_BKP_BLOCK_MASK));

	buffer = XLogReadBuffer(*node, GIST_ROOT_BLKNO, true);
	Assert(BufferIsValid(buffer));
=======
	buffer = XLogInitBufferForRedo(record, 0);
	Assert(BufferGetBlockNumber(buffer) == GIST_ROOT_BLKNO);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	page = (Page) BufferGetPage(buffer);

	GISTInitBuffer(buffer, F_LEAF);

	PageSetLSN(page, lsn);

	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
}

void
<<<<<<< HEAD
gist_redo(XLogRecPtr beginLoc, XLogRecPtr lsn, XLogRecord *record)
=======
gist_redo(XLogReaderState *record)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	MemoryContext oldCxt;

	/*
	 * GiST indexes do not require any conflict processing. NB: If we ever
	 * implement a similar optimization we have in b-tree, and remove killed
	 * tuples outside VACUUM, we'll need to handle that here.
	 */

	oldCxt = MemoryContextSwitchTo(opCtx);
	switch (info)
	{
		case XLOG_GIST_PAGE_UPDATE:
<<<<<<< HEAD
			gistRedoPageUpdateRecord(lsn, record);
=======
			gistRedoPageUpdateRecord(record);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
			break;
		case XLOG_GIST_PAGE_SPLIT:
			gistRedoPageSplitRecord(record);
			break;
		case XLOG_GIST_CREATE_INDEX:
			gistRedoCreateIndex(record);
			break;
		default:
			elog(PANIC, "gist_redo: unknown op code %u", info);
	}

	MemoryContextSwitchTo(oldCxt);
	MemoryContextReset(opCtx);
}

<<<<<<< HEAD
static void
out_target(StringInfo buf, RelFileNode node)
{
	appendStringInfo(buf, "rel %u/%u/%u",
					 node.spcNode, node.dbNode, node.relNode);
}

static void
out_gistxlogPageUpdate(StringInfo buf, gistxlogPageUpdate *xlrec)
{
	out_target(buf, xlrec->node);
	appendStringInfo(buf, "; block number %u", xlrec->blkno);
}

static void
out_gistxlogPageSplit(StringInfo buf, gistxlogPageSplit *xlrec)
{
	appendStringInfo(buf, "page_split: ");
	out_target(buf, xlrec->node);
	appendStringInfo(buf, "; block number %u splits to %d pages",
					 xlrec->origblkno, xlrec->npage);
}

void
gist_desc(StringInfo buf, XLogRecPtr beginLoc, XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;
	char		*rec = XLogRecGetData(record);

	switch (info)
	{
		case XLOG_GIST_PAGE_UPDATE:
			appendStringInfo(buf, "page_update: ");
			out_gistxlogPageUpdate(buf, (gistxlogPageUpdate *) rec);
			break;
		case XLOG_GIST_PAGE_SPLIT:
			out_gistxlogPageSplit(buf, (gistxlogPageSplit *) rec);
			break;
		case XLOG_GIST_CREATE_INDEX:
			appendStringInfo(buf, "create_index: rel %u/%u/%u",
							 ((RelFileNode *) rec)->spcNode,
							 ((RelFileNode *) rec)->dbNode,
							 ((RelFileNode *) rec)->relNode);
			break;
		default:
			appendStringInfo(buf, "unknown gist op code %u", info);
			break;
	}
}

/*
 * Mask a Gist page before running consistency checks on it.
 */
void
gist_mask(char *pagedata, BlockNumber blkno)
{
	Page		page = (Page) pagedata;

	mask_page_lsn_and_checksum(page);

	mask_page_hint_bits(page);
	mask_unused_space(page);

	/*
	 * NSN is nothing but a special purpose LSN. Hence, mask it for the same
	 * reason as mask_page_lsn.
	 */
	PageXLogRecPtrSet(GistPageGetOpaque(page)->nsn, (uint64) MASK_MARKER);

#if PG_VERSION_NUM >= 90100
	/*
	 * We update F_FOLLOW_RIGHT flag on the left child after writing WAL
	 * record. Hence, mask this flag. See gistplacetopage() for details.
	 */
	GistMarkFollowRight(page);
#endif

	if (GistPageIsLeaf(page))
	{
		/*
		 * In gist leaf pages, it is possible to modify the LP_FLAGS without
		 * emitting any WAL record. Hence, mask the line pointer flags. See
		 * gistkillitems() for details.
		 */
		mask_lp_flags(page);
	}

#if PG_VERSION_NUM >= 90600
	/*
	 * During gist redo, we never mark a page as garbage. Hence, mask it to
	 * ignore any differences.
	 */
	GistClearPageHasGarbage(page);
#endif
}

=======
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
void
gist_xlog_startup(void)
{
	opCtx = createTempGistContext();
}

void
gist_xlog_cleanup(void)
{
	MemoryContextDelete(opCtx);
}

/*
 * Write WAL record of a page split.
 */
XLogRecPtr
gistXLogSplit(RelFileNode node, BlockNumber blkno, bool page_is_leaf,
			  SplitedPageLayout *dist,
			  BlockNumber origrlink, GistNSN orignsn,
			  Buffer leftchildbuf, bool markfollowright)
{
	gistxlogPageSplit xlrec;
	SplitedPageLayout *ptr;
	int			npage = 0;
	XLogRecPtr	recptr;
	int			i;

	for (ptr = dist; ptr; ptr = ptr->next)
		npage++;

	xlrec.origrlink = origrlink;
	xlrec.orignsn = orignsn;
	xlrec.origleaf = page_is_leaf;
	xlrec.npage = (uint16) npage;
	xlrec.markfollowright = markfollowright;

	XLogBeginInsert();

	/*
	 * Include a full page image of the child buf. (only necessary if a
	 * checkpoint happened since the child page was split)
	 */
	if (BufferIsValid(leftchildbuf))
		XLogRegisterBuffer(0, leftchildbuf, REGBUF_STANDARD);

	/*
	 * NOTE: We register a lot of data. The caller must've called
	 * XLogEnsureRecordSpace() to prepare for that. We cannot do it here,
	 * because we're already in a critical section. If you change the number
	 * of buffer or data registrations here, make sure you modify the
	 * XLogEnsureRecordSpace() calls accordingly!
	 */
	XLogRegisterData((char *) &xlrec, sizeof(gistxlogPageSplit));

	i = 1;
	for (ptr = dist; ptr; ptr = ptr->next)
	{
		XLogRegisterBuffer(i, ptr->buffer, REGBUF_WILL_INIT);
		XLogRegisterBufData(i, (char *) &(ptr->block.num), sizeof(int));
		XLogRegisterBufData(i, (char *) ptr->list, ptr->lenlist);
		i++;
	}

	recptr = XLogInsert(RM_GIST_ID, XLOG_GIST_PAGE_SPLIT);

	return recptr;
}

/*
 * Write XLOG record describing a page update. The update can include any
 * number of deletions and/or insertions of tuples on a single index page.
 *
 * If this update inserts a downlink for a split page, also record that
 * the F_FOLLOW_RIGHT flag on the child page is cleared and NSN set.
 *
 * Note that both the todelete array and the tuples are marked as belonging
 * to the target buffer; they need not be stored in XLOG if XLogInsert decides
 * to log the whole buffer contents instead.
 */
XLogRecPtr
gistXLogUpdate(RelFileNode node, Buffer buffer,
			   OffsetNumber *todelete, int ntodelete,
			   IndexTuple *itup, int ituplen,
			   Buffer leftchildbuf)
{
<<<<<<< HEAD
	XLogRecData *rdata;
	gistxlogPageUpdate xlrec;
	int			cur,
				i;
	XLogRecPtr	recptr;

	rdata = (XLogRecData *) palloc(sizeof(XLogRecData) * (3 + ituplen));

	xlrec.node = node;
	xlrec.blkno = BufferGetBlockNumber(buffer);
	xlrec.ntodelete = ntodelete;
	xlrec.leftchild =
		BufferIsValid(leftchildbuf) ? BufferGetBlockNumber(leftchildbuf) : InvalidBlockNumber;

	rdata[0].data = (char *) &xlrec;
	rdata[0].len = sizeof(gistxlogPageUpdate);
	rdata[0].buffer = InvalidBuffer;
	rdata[0].next = &(rdata[1]);

	rdata[1].data = (char *) todelete;
	rdata[1].len = sizeof(OffsetNumber) * ntodelete;
	rdata[1].buffer = buffer;
	rdata[1].buffer_std = true;

	cur = 2;
=======
	gistxlogPageUpdate xlrec;
	int			i;
	XLogRecPtr	recptr;

	xlrec.ntodelete = ntodelete;
	xlrec.ntoinsert = ituplen;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(gistxlogPageUpdate));

	XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);
	XLogRegisterBufData(0, (char *) todelete, sizeof(OffsetNumber) * ntodelete);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/* new tuples */
	for (i = 0; i < ituplen; i++)
		XLogRegisterBufData(0, (char *) (itup[i]), IndexTupleSize(itup[i]));

	/*
	 * Include a full page image of the child buf. (only necessary if a
	 * checkpoint happened since the child page was split)
	 */
	if (BufferIsValid(leftchildbuf))
		XLogRegisterBuffer(1, leftchildbuf, REGBUF_STANDARD);

	recptr = XLogInsert(RM_GIST_ID, XLOG_GIST_PAGE_UPDATE);

	return recptr;
}
