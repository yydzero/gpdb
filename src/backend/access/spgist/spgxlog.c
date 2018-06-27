/*-------------------------------------------------------------------------
 *
 * spgxlog.c
 *	  WAL replay logic for SP-GiST
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			 src/backend/access/spgist/spgxlog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/spgist_private.h"
#include "access/transam.h"
<<<<<<< HEAD
=======
#include "access/xlog.h"
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
#include "access/xlogutils.h"
#include "storage/standby.h"
#include "utils/memutils.h"


static MemoryContext opCtx;		/* working memory for operations */


/*
 * Prepare a dummy SpGistState, with just the minimum info needed for replay.
 *
 * At present, all we need is enough info to support spgFormDeadTuple(),
 * plus the isBuild flag.
 */
static void
fillFakeState(SpGistState *state, spgxlogState stateSrc)
{
	memset(state, 0, sizeof(*state));

	state->myXid = stateSrc.myXid;
	state->isBuild = stateSrc.isBuild;
	state->deadTupleStorage = palloc0(SGDTSIZE);
}

/*
 * Add a leaf tuple, or replace an existing placeholder tuple.  This is used
 * to replay SpGistPageAddNewItem() operations.  If the offset points at an
 * existing tuple, it had better be a placeholder tuple.
 */
static void
addOrReplaceTuple(Page page, Item tuple, int size, OffsetNumber offset)
{
	if (offset <= PageGetMaxOffsetNumber(page))
	{
		SpGistDeadTuple dt = (SpGistDeadTuple) PageGetItem(page,
												PageGetItemId(page, offset));

		if (dt->tupstate != SPGIST_PLACEHOLDER)
			elog(ERROR, "SPGiST tuple to be replaced is not a placeholder");

		Assert(SpGistPageGetOpaque(page)->nPlaceholder > 0);
		SpGistPageGetOpaque(page)->nPlaceholder--;

		PageIndexTupleDelete(page, offset);
	}

	Assert(offset <= PageGetMaxOffsetNumber(page) + 1);

	if (PageAddItem(page, tuple, size, offset, false, false) != offset)
		elog(ERROR, "failed to add item of size %u to SPGiST index page",
			 size);
}

static void
spgRedoCreateIndex(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	Buffer		buffer;
	Page		page;

<<<<<<< HEAD
	/* Backup blocks are not used in create_index records */
	Assert(!(record->xl_info & XLR_BKP_BLOCK_MASK));

	buffer = XLogReadBuffer(*node, SPGIST_METAPAGE_BLKNO, true);
	Assert(BufferIsValid(buffer));
=======
	buffer = XLogInitBufferForRedo(record, 0);
	Assert(BufferGetBlockNumber(buffer) == SPGIST_METAPAGE_BLKNO);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	page = (Page) BufferGetPage(buffer);
	SpGistInitMetapage(page);
	PageSetLSN(page, lsn);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);

	buffer = XLogInitBufferForRedo(record, 1);
	Assert(BufferGetBlockNumber(buffer) == SPGIST_ROOT_BLKNO);
	SpGistInitBuffer(buffer, SPGIST_LEAF);
	page = (Page) BufferGetPage(buffer);
	PageSetLSN(page, lsn);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);

	buffer = XLogInitBufferForRedo(record, 2);
	Assert(BufferGetBlockNumber(buffer) == SPGIST_NULL_BLKNO);
	SpGistInitBuffer(buffer, SPGIST_LEAF | SPGIST_NULLS);
	page = (Page) BufferGetPage(buffer);
	PageSetLSN(page, lsn);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
}

static void
spgRedoAddLeaf(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	char	   *ptr = XLogRecGetData(record);
	spgxlogAddLeaf *xldata = (spgxlogAddLeaf *) ptr;
	char	   *leafTuple;
	SpGistLeafTupleData leafTupleHdr;
	Buffer		buffer;
	Page		page;
	XLogRedoAction action;

	ptr += sizeof(spgxlogAddLeaf);
	leafTuple = ptr;
	/* the leaf tuple is unaligned, so make a copy to access its header */
	memcpy(&leafTupleHdr, leafTuple, sizeof(SpGistLeafTupleData));

	/*
	 * In normal operation we would have both current and parent pages locked
	 * simultaneously; but in WAL replay it should be safe to update the leaf
	 * page before updating the parent.
	 */
<<<<<<< HEAD
	if (record->xl_info & XLR_BKP_BLOCK(0))
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
	else
=======
	if (xldata->newPage)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	{
		buffer = XLogInitBufferForRedo(record, 0);
		SpGistInitBuffer(buffer,
					 SPGIST_LEAF | (xldata->storesNulls ? SPGIST_NULLS : 0));
		action = BLK_NEEDS_REDO;
	}
	else
		action = XLogReadBufferForRedo(record, 0, &buffer);

	if (action == BLK_NEEDS_REDO)
	{
		page = BufferGetPage(buffer);

		/* insert new tuple */
		if (xldata->offnumLeaf != xldata->offnumHeadLeaf)
		{
			/* normal cases, tuple was added by SpGistPageAddNewItem */
			addOrReplaceTuple(page, (Item) leafTuple, leafTupleHdr.size,
							  xldata->offnumLeaf);

			/* update head tuple's chain link if needed */
			if (xldata->offnumHeadLeaf != InvalidOffsetNumber)
			{
				SpGistLeafTuple head;

				head = (SpGistLeafTuple) PageGetItem(page,
								PageGetItemId(page, xldata->offnumHeadLeaf));
				Assert(head->nextOffset == leafTupleHdr.nextOffset);
				head->nextOffset = xldata->offnumLeaf;
			}
		}
		else
		{
			/* replacing a DEAD tuple */
			PageIndexTupleDelete(page, xldata->offnumLeaf);
			if (PageAddItem(page,
							(Item) leafTuple, leafTupleHdr.size,
					 xldata->offnumLeaf, false, false) != xldata->offnumLeaf)
<<<<<<< HEAD
						elog(ERROR, "failed to add item of size %u to SPGiST index page",
							 leafTuple->size);
				}

				PageSetLSN(page, lsn);
				MarkBufferDirty(buffer);
			}
			UnlockReleaseBuffer(buffer);
=======
				elog(ERROR, "failed to add item of size %u to SPGiST index page",
					 leafTupleHdr.size);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		}

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

	/* update parent downlink if necessary */
<<<<<<< HEAD
	if (record->xl_info & XLR_BKP_BLOCK(1))
		(void) RestoreBackupBlock(lsn, record, 1, false, false);
	else if (xldata->blknoParent != InvalidBlockNumber)
=======
	if (xldata->offnumParent != InvalidOffsetNumber)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	{
		if (XLogReadBufferForRedo(record, 1, &buffer) == BLK_NEEDS_REDO)
		{
			SpGistInnerTuple tuple;
			BlockNumber blknoLeaf;

			XLogRecGetBlockTag(record, 0, NULL, NULL, &blknoLeaf);

			page = BufferGetPage(buffer);

			tuple = (SpGistInnerTuple) PageGetItem(page,
								  PageGetItemId(page, xldata->offnumParent));

			spgUpdateNodeLink(tuple, xldata->nodeI,
							  blknoLeaf, xldata->offnumLeaf);

<<<<<<< HEAD
				PageSetLSN(page, lsn);
				MarkBufferDirty(buffer);
			}
			UnlockReleaseBuffer(buffer);
=======
			PageSetLSN(page, lsn);
			MarkBufferDirty(buffer);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);
	}
}

static void
spgRedoMoveLeafs(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	char	   *ptr = XLogRecGetData(record);
	spgxlogMoveLeafs *xldata = (spgxlogMoveLeafs *) ptr;
	SpGistState state;
	OffsetNumber *toDelete;
	OffsetNumber *toInsert;
	int			nInsert;
	Buffer		buffer;
	Page		page;
	XLogRedoAction action;
	BlockNumber blknoDst;

	XLogRecGetBlockTag(record, 1, NULL, NULL, &blknoDst);

	fillFakeState(&state, xldata->stateSrc);

	nInsert = xldata->replaceDead ? 1 : xldata->nMoves + 1;

	ptr += SizeOfSpgxlogMoveLeafs;
	toDelete = (OffsetNumber *) ptr;
	ptr += sizeof(OffsetNumber) * xldata->nMoves;
	toInsert = (OffsetNumber *) ptr;
	ptr += sizeof(OffsetNumber) * nInsert;

	/* now ptr points to the list of leaf tuples */

	/*
	 * In normal operation we would have all three pages (source, dest, and
	 * parent) locked simultaneously; but in WAL replay it should be safe to
	 * update them one at a time, as long as we do it in the right order.
	 */

	/* Insert tuples on the dest page (do first, so redirect is valid) */
<<<<<<< HEAD
	if (record->xl_info & XLR_BKP_BLOCK(1))
		(void) RestoreBackupBlock(lsn, record, 1, false, false);
	else
=======
	if (xldata->newPage)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	{
		buffer = XLogInitBufferForRedo(record, 1);
		SpGistInitBuffer(buffer,
					 SPGIST_LEAF | (xldata->storesNulls ? SPGIST_NULLS : 0));
		action = BLK_NEEDS_REDO;
	}
	else
		action = XLogReadBufferForRedo(record, 1, &buffer);

	if (action == BLK_NEEDS_REDO)
	{
		int			i;

		page = BufferGetPage(buffer);

		for (i = 0; i < nInsert; i++)
		{
			char	   *leafTuple;
			SpGistLeafTupleData leafTupleHdr;

<<<<<<< HEAD
				PageSetLSN(page, lsn);
				MarkBufferDirty(buffer);
			}
			UnlockReleaseBuffer(buffer);
=======
			/*
			 * the tuples are not aligned, so must copy to access the size
			 * field.
			 */
			leafTuple = ptr;
			memcpy(&leafTupleHdr, leafTuple,
				   sizeof(SpGistLeafTupleData));

			addOrReplaceTuple(page, (Item) leafTuple,
							  leafTupleHdr.size, toInsert[i]);
			ptr += leafTupleHdr.size;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		}

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

	/* Delete tuples from the source page, inserting a redirection pointer */
<<<<<<< HEAD
	if (record->xl_info & XLR_BKP_BLOCK(0))
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
	else
=======
	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	{
		page = BufferGetPage(buffer);

		spgPageIndexMultiDelete(&state, page, toDelete, xldata->nMoves,
						state.isBuild ? SPGIST_PLACEHOLDER : SPGIST_REDIRECT,
								SPGIST_PLACEHOLDER,
								blknoDst,
								toInsert[nInsert - 1]);

<<<<<<< HEAD
				PageSetLSN(page, lsn);
				MarkBufferDirty(buffer);
			}
			UnlockReleaseBuffer(buffer);
		}
=======
		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

	/* And update the parent downlink */
<<<<<<< HEAD
	if (record->xl_info & XLR_BKP_BLOCK(2))
		(void) RestoreBackupBlock(lsn, record, 2, false, false);
	else
=======
	if (XLogReadBufferForRedo(record, 2, &buffer) == BLK_NEEDS_REDO)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	{
		SpGistInnerTuple tuple;

		page = BufferGetPage(buffer);

		tuple = (SpGistInnerTuple) PageGetItem(page,
								  PageGetItemId(page, xldata->offnumParent));

		spgUpdateNodeLink(tuple, xldata->nodeI,
						  blknoDst, toInsert[nInsert - 1]);

<<<<<<< HEAD
				PageSetLSN(page, lsn);
				MarkBufferDirty(buffer);
			}
			UnlockReleaseBuffer(buffer);
		}
=======
		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

static void
spgRedoAddNode(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	char	   *ptr = XLogRecGetData(record);
	spgxlogAddNode *xldata = (spgxlogAddNode *) ptr;
	char	   *innerTuple;
	SpGistInnerTupleData innerTupleHdr;
	SpGistState state;
	Buffer		buffer;
	Page		page;
	XLogRedoAction action;

	ptr += sizeof(spgxlogAddNode);
	innerTuple = ptr;
	/* the tuple is unaligned, so make a copy to access its header */
	memcpy(&innerTupleHdr, innerTuple, sizeof(SpGistInnerTupleData));

	fillFakeState(&state, xldata->stateSrc);

	if (!XLogRecHasBlockRef(record, 1))
	{
		/* update in place */
<<<<<<< HEAD
		Assert(xldata->blknoParent == InvalidBlockNumber);
		if (record->xl_info & XLR_BKP_BLOCK(0))
			(void) RestoreBackupBlock(lsn, record, 0, false, false);
		else
=======
		Assert(xldata->parentBlk == -1);
		if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		{
			page = BufferGetPage(buffer);

<<<<<<< HEAD
					PageSetLSN(page, lsn);
					MarkBufferDirty(buffer);
				}
				UnlockReleaseBuffer(buffer);
			}
=======
			PageIndexTupleDelete(page, xldata->offnum);
			if (PageAddItem(page, (Item) innerTuple, innerTupleHdr.size,
							xldata->offnum,
							false, false) != xldata->offnum)
				elog(ERROR, "failed to add item of size %u to SPGiST index page",
					 innerTupleHdr.size);

			PageSetLSN(page, lsn);
			MarkBufferDirty(buffer);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);
	}
	else
	{
<<<<<<< HEAD
=======
		BlockNumber blkno;
		BlockNumber blknoNew;

		XLogRecGetBlockTag(record, 0, NULL, NULL, &blkno);
		XLogRecGetBlockTag(record, 1, NULL, NULL, &blknoNew);

>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		/*
		 * In normal operation we would have all three pages (source, dest,
		 * and parent) locked simultaneously; but in WAL replay it should be
		 * safe to update them one at a time, as long as we do it in the right
<<<<<<< HEAD
		 * order.
		 *
		 * The logic here depends on the assumption that blkno != blknoNew,
		 * else we can't tell which BKP bit goes with which page, and the LSN
		 * checks could go wrong too.
		 */
		Assert(xldata->blkno != xldata->blknoNew);

		/* Install new tuple first so redirect is valid */
		if (record->xl_info & XLR_BKP_BLOCK(1))
			(void) RestoreBackupBlock(lsn, record, 1, false, false);
		else
=======
		 * order. We must insert the new tuple before replacing the old tuple
		 * with the redirect tuple.
		 */

		/* Install new tuple first so redirect is valid */
		if (xldata->newPage)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		{
			/* AddNode is not used for nulls pages */
			buffer = XLogInitBufferForRedo(record, 1);
			SpGistInitBuffer(buffer, 0);
			action = BLK_NEEDS_REDO;
		}
		else
			action = XLogReadBufferForRedo(record, 1, &buffer);
		if (action == BLK_NEEDS_REDO)
		{
			page = BufferGetPage(buffer);

			addOrReplaceTuple(page, (Item) innerTuple,
							  innerTupleHdr.size, xldata->offnumNew);

			/*
			 * If parent is in this same page, update it now.
			 */
			if (xldata->parentBlk == 1)
			{
				SpGistInnerTuple parentTuple;

				parentTuple = (SpGistInnerTuple) PageGetItem(page,
								  PageGetItemId(page, xldata->offnumParent));

<<<<<<< HEAD
				if (!XLByteLE(lsn, PageGetLSN(page)))
				{
					addOrReplaceTuple(page, (Item) innerTuple,
									  innerTuple->size, xldata->offnumNew);

					/*
					 * If parent is in this same page, don't advance LSN;
					 * doing so would fool us into not applying the parent
					 * downlink update below.  We'll update the LSN when we
					 * fix the parent downlink.
					 */
					if (xldata->blknoParent != xldata->blknoNew)
					{
						PageSetLSN(page, lsn);
					}
					MarkBufferDirty(buffer);
				}
				UnlockReleaseBuffer(buffer);
=======
				spgUpdateNodeLink(parentTuple, xldata->nodeI,
								  blknoNew, xldata->offnumNew);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
			}
			PageSetLSN(page, lsn);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);

		/* Delete old tuple, replacing it with redirect or placeholder tuple */
<<<<<<< HEAD
		if (record->xl_info & XLR_BKP_BLOCK(0))
			(void) RestoreBackupBlock(lsn, record, 0, false, false);
		else
=======
		if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		{
			SpGistDeadTuple dt;

			page = BufferGetPage(buffer);

			if (state.isBuild)
				dt = spgFormDeadTuple(&state, SPGIST_PLACEHOLDER,
									  InvalidBlockNumber,
									  InvalidOffsetNumber);
			else
				dt = spgFormDeadTuple(&state, SPGIST_REDIRECT,
									  blknoNew,
									  xldata->offnumNew);

			PageIndexTupleDelete(page, xldata->offnum);
			if (PageAddItem(page, (Item) dt, dt->size,
							xldata->offnum,
							false, false) != xldata->offnum)
				elog(ERROR, "failed to add item of size %u to SPGiST index page",
					 dt->size);

			if (state.isBuild)
				SpGistPageGetOpaque(page)->nPlaceholder++;
			else
				SpGistPageGetOpaque(page)->nRedirection++;

			/*
			 * If parent is in this same page, update it now.
			 */
			if (xldata->parentBlk == 0)
			{
				SpGistInnerTuple parentTuple;

				parentTuple = (SpGistInnerTuple) PageGetItem(page,
								  PageGetItemId(page, xldata->offnumParent));

<<<<<<< HEAD
					PageIndexTupleDelete(page, xldata->offnum);
					if (PageAddItem(page, (Item) dt, dt->size,
									xldata->offnum,
									false, false) != xldata->offnum)
						elog(ERROR, "failed to add item of size %u to SPGiST index page",
							 dt->size);

					if (state.isBuild)
						SpGistPageGetOpaque(page)->nPlaceholder++;
					else
						SpGistPageGetOpaque(page)->nRedirection++;

					/*
					 * If parent is in this same page, don't advance LSN;
					 * doing so would fool us into not applying the parent
					 * downlink update below.  We'll update the LSN when we
					 * fix the parent downlink.
					 */
					if (xldata->blknoParent != xldata->blkno)
					{
						PageSetLSN(page, lsn);
					}
					MarkBufferDirty(buffer);
				}
				UnlockReleaseBuffer(buffer);
=======
				spgUpdateNodeLink(parentTuple, xldata->nodeI,
								  blknoNew, xldata->offnumNew);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
			}
			PageSetLSN(page, lsn);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);

		/*
		 * Update parent downlink (if we didn't do it as part of the source or
		 * destination page update already).
		 */
<<<<<<< HEAD
		if (xldata->blknoParent == xldata->blkno)
			bbi = 0;
		else if (xldata->blknoParent == xldata->blknoNew)
			bbi = 1;
		else
			bbi = 2;

		if (record->xl_info & XLR_BKP_BLOCK(bbi))
		{
			if (bbi == 2)		/* else we already did it */
				(void) RestoreBackupBlock(lsn, record, bbi, false, false);
		}
		else
=======
		if (xldata->parentBlk == 2)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		{
			if (XLogReadBufferForRedo(record, 2, &buffer) == BLK_NEEDS_REDO)
			{
				SpGistInnerTuple parentTuple;

				page = BufferGetPage(buffer);

				parentTuple = (SpGistInnerTuple) PageGetItem(page,
								  PageGetItemId(page, xldata->offnumParent));

				spgUpdateNodeLink(parentTuple, xldata->nodeI,
								  blknoNew, xldata->offnumNew);

<<<<<<< HEAD
					PageSetLSN(page, lsn);
					MarkBufferDirty(buffer);
				}
				UnlockReleaseBuffer(buffer);
=======
				PageSetLSN(page, lsn);
				MarkBufferDirty(buffer);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
			}
			if (BufferIsValid(buffer))
				UnlockReleaseBuffer(buffer);
		}
	}
}

static void
spgRedoSplitTuple(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	char	   *ptr = XLogRecGetData(record);
	spgxlogSplitTuple *xldata = (spgxlogSplitTuple *) ptr;
	char	   *prefixTuple;
	SpGistInnerTupleData prefixTupleHdr;
	char	   *postfixTuple;
	SpGistInnerTupleData postfixTupleHdr;
	Buffer		buffer;
	Page		page;
	XLogRedoAction action;

	ptr += sizeof(spgxlogSplitTuple);
	prefixTuple = ptr;
	/* the prefix tuple is unaligned, so make a copy to access its header */
	memcpy(&prefixTupleHdr, prefixTuple, sizeof(SpGistInnerTupleData));
	ptr += prefixTupleHdr.size;
	postfixTuple = ptr;
	/* postfix tuple is also unaligned */
	memcpy(&postfixTupleHdr, postfixTuple, sizeof(SpGistInnerTupleData));

	/*
	 * In normal operation we would have both pages locked simultaneously; but
	 * in WAL replay it should be safe to update them one at a time, as long
	 * as we do it in the right order.
	 */

	/*
	 * In normal operation we would have both pages locked simultaneously; but
	 * in WAL replay it should be safe to update them one at a time, as long
	 * as we do it in the right order.
	 */

	/* insert postfix tuple first to avoid dangling link */
<<<<<<< HEAD
	if (record->xl_info & XLR_BKP_BLOCK(1))
		(void) RestoreBackupBlock(lsn, record, 1, false, false);
	else if (xldata->blknoPostfix != xldata->blknoPrefix)
=======
	if (!xldata->postfixBlkSame)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	{
		if (xldata->newPage)
		{
			buffer = XLogInitBufferForRedo(record, 1);
			/* SplitTuple is not used for nulls pages */
			SpGistInitBuffer(buffer, 0);
			action = BLK_NEEDS_REDO;
		}
		else
			action = XLogReadBufferForRedo(record, 1, &buffer);
		if (action == BLK_NEEDS_REDO)
		{
			page = BufferGetPage(buffer);

			addOrReplaceTuple(page, (Item) postfixTuple,
							  postfixTupleHdr.size, xldata->offnumPostfix);

<<<<<<< HEAD
				PageSetLSN(page, lsn);
				MarkBufferDirty(buffer);
			}
=======
			PageSetLSN(page, lsn);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
			UnlockReleaseBuffer(buffer);
	}

	/* now handle the original page */
<<<<<<< HEAD
	if (record->xl_info & XLR_BKP_BLOCK(0))
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
	else
=======
	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	{
		page = BufferGetPage(buffer);

		PageIndexTupleDelete(page, xldata->offnumPrefix);
		if (PageAddItem(page, (Item) prefixTuple, prefixTupleHdr.size,
				 xldata->offnumPrefix, false, false) != xldata->offnumPrefix)
			elog(ERROR, "failed to add item of size %u to SPGiST index page",
				 prefixTupleHdr.size);

		if (xldata->postfixBlkSame)
			addOrReplaceTuple(page, (Item) postfixTuple,
							  postfixTupleHdr.size,
							  xldata->offnumPostfix);

<<<<<<< HEAD
				PageSetLSN(page, lsn);
				MarkBufferDirty(buffer);
			}
			UnlockReleaseBuffer(buffer);
		}
=======
		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

static void
spgRedoPickSplit(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	char	   *ptr = XLogRecGetData(record);
	spgxlogPickSplit *xldata = (spgxlogPickSplit *) ptr;
	char	   *innerTuple;
	SpGistInnerTupleData innerTupleHdr;
	SpGistState state;
	OffsetNumber *toDelete;
	OffsetNumber *toInsert;
	uint8	   *leafPageSelect;
	Buffer		srcBuffer;
	Buffer		destBuffer;
<<<<<<< HEAD
=======
	Buffer		innerBuffer;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	Page		srcPage;
	Page		destPage;
	Page		page;
	int			i;
	BlockNumber blknoInner;
	XLogRedoAction action;

	XLogRecGetBlockTag(record, 2, NULL, NULL, &blknoInner);

	fillFakeState(&state, xldata->stateSrc);

	ptr += SizeOfSpgxlogPickSplit;
	toDelete = (OffsetNumber *) ptr;
	ptr += sizeof(OffsetNumber) * xldata->nDelete;
	toInsert = (OffsetNumber *) ptr;
	ptr += sizeof(OffsetNumber) * xldata->nInsert;
	leafPageSelect = (uint8 *) ptr;
	ptr += sizeof(uint8) * xldata->nInsert;

	innerTuple = ptr;
	/* the inner tuple is unaligned, so make a copy to access its header */
	memcpy(&innerTupleHdr, innerTuple, sizeof(SpGistInnerTupleData));
	ptr += innerTupleHdr.size;

	/* now ptr points to the list of leaf tuples */

	if (xldata->isRootSplit)
	{
		/* when splitting root, we touch it only in the guise of new inner */
		srcBuffer = InvalidBuffer;
		srcPage = NULL;
	}
	else if (xldata->initSrc)
	{
		/* just re-init the source page */
<<<<<<< HEAD
		srcBuffer = XLogReadBuffer(xldata->node, xldata->blknoSrc, true);
		Assert(BufferIsValid(srcBuffer));
=======
		srcBuffer = XLogInitBufferForRedo(record, 0);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		srcPage = (Page) BufferGetPage(srcBuffer);

		SpGistInitBuffer(srcBuffer,
					 SPGIST_LEAF | (xldata->storesNulls ? SPGIST_NULLS : 0));
		/* don't update LSN etc till we're done with it */
	}
	else
	{
		/*
		 * Delete the specified tuples from source page.  (In case we're in
		 * Hot Standby, we need to hold lock on the page till we're done
		 * inserting leaf tuples and the new inner tuple, else the added
		 * redirect tuple will be a dangling link.)
		 */
<<<<<<< HEAD
		if (record->xl_info & XLR_BKP_BLOCK(bbi))
		{
			srcBuffer = RestoreBackupBlock(lsn, record, bbi, false, true);
			srcPage = NULL;		/* don't need to do any page updates */
		}
		else
		{
			srcBuffer = XLogReadBuffer(xldata->node, xldata->blknoSrc, false);
			if (BufferIsValid(srcBuffer))
			{
				srcPage = BufferGetPage(srcBuffer);
				if (!XLByteLE(lsn, PageGetLSN(srcPage)))
				{
					/*
					 * We have it a bit easier here than in doPickSplit(),
					 * because we know the inner tuple's location already, so
					 * we can inject the correct redirection tuple now.
					 */
					if (!state.isBuild)
						spgPageIndexMultiDelete(&state, srcPage,
												toDelete, xldata->nDelete,
												SPGIST_REDIRECT,
												SPGIST_PLACEHOLDER,
												xldata->blknoInner,
												xldata->offnumInner);
					else
						spgPageIndexMultiDelete(&state, srcPage,
												toDelete, xldata->nDelete,
												SPGIST_PLACEHOLDER,
												SPGIST_PLACEHOLDER,
												InvalidBlockNumber,
												InvalidOffsetNumber);

					/* don't update LSN etc till we're done with it */
				}
				else
					srcPage = NULL;		/* don't do any page updates */
			}
			else
				srcPage = NULL;
		}
		bbi++;
=======
		srcPage = NULL;
		if (XLogReadBufferForRedo(record, 0, &srcBuffer) == BLK_NEEDS_REDO)
		{
			srcPage = BufferGetPage(srcBuffer);

			/*
			 * We have it a bit easier here than in doPickSplit(), because we
			 * know the inner tuple's location already, so we can inject the
			 * correct redirection tuple now.
			 */
			if (!state.isBuild)
				spgPageIndexMultiDelete(&state, srcPage,
										toDelete, xldata->nDelete,
										SPGIST_REDIRECT,
										SPGIST_PLACEHOLDER,
										blknoInner,
										xldata->offnumInner);
			else
				spgPageIndexMultiDelete(&state, srcPage,
										toDelete, xldata->nDelete,
										SPGIST_PLACEHOLDER,
										SPGIST_PLACEHOLDER,
										InvalidBlockNumber,
										InvalidOffsetNumber);

			/* don't update LSN etc till we're done with it */
		}
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	}

	/* try to access dest page if any */
	if (!XLogRecHasBlockRef(record, 1))
	{
		destBuffer = InvalidBuffer;
		destPage = NULL;
	}
	else if (xldata->initDest)
	{
		/* just re-init the dest page */
<<<<<<< HEAD
		destBuffer = XLogReadBuffer(xldata->node, xldata->blknoDest, true);
		Assert(BufferIsValid(destBuffer));
=======
		destBuffer = XLogInitBufferForRedo(record, 1);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		destPage = (Page) BufferGetPage(destBuffer);

		SpGistInitBuffer(destBuffer,
					 SPGIST_LEAF | (xldata->storesNulls ? SPGIST_NULLS : 0));
		/* don't update LSN etc till we're done with it */
	}
	else
	{
		/*
		 * We could probably release the page lock immediately in the
		 * full-page-image case, but for safety let's hold it till later.
		 */
<<<<<<< HEAD
		if (record->xl_info & XLR_BKP_BLOCK(bbi))
		{
			destBuffer = RestoreBackupBlock(lsn, record, bbi, false, true);
			destPage = NULL;	/* don't need to do any page updates */
		}
		else
		{
			destBuffer = XLogReadBuffer(xldata->node, xldata->blknoDest, false);
			if (BufferIsValid(destBuffer))
			{
				destPage = (Page) BufferGetPage(destBuffer);
				if (XLByteLE(lsn, PageGetLSN(destPage)))
					destPage = NULL;	/* don't do any page updates */
			}
			else
				destPage = NULL;
		}
		bbi++;
=======
		if (XLogReadBufferForRedo(record, 1, &destBuffer) == BLK_NEEDS_REDO)
			destPage = (Page) BufferGetPage(destBuffer);
		else
			destPage = NULL;	/* don't do any page updates */
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	}

	/* restore leaf tuples to src and/or dest page */
	for (i = 0; i < xldata->nInsert; i++)
	{
<<<<<<< HEAD
		SpGistLeafTuple lt = (SpGistLeafTuple) ptr;
=======
		char	   *leafTuple;
		SpGistLeafTupleData leafTupleHdr;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

		/* the tuples are not aligned, so must copy to access the size field. */
		leafTuple = ptr;
		memcpy(&leafTupleHdr, leafTuple, sizeof(SpGistLeafTupleData));
		ptr += leafTupleHdr.size;

		page = leafPageSelect[i] ? destPage : srcPage;
		if (page == NULL)
			continue;			/* no need to touch this page */

<<<<<<< HEAD
		addOrReplaceTuple(page, (Item) lt, lt->size, toInsert[i]);
=======
		addOrReplaceTuple(page, (Item) leafTuple, leafTupleHdr.size,
						  toInsert[i]);
	}

	/* Now update src and dest page LSNs if needed */
	if (srcPage != NULL)
	{
		PageSetLSN(srcPage, lsn);
		MarkBufferDirty(srcBuffer);
	}
	if (destPage != NULL)
	{
		PageSetLSN(destPage, lsn);
		MarkBufferDirty(destBuffer);
	}

	/* restore new inner tuple */
	if (xldata->initInner)
	{
		innerBuffer = XLogInitBufferForRedo(record, 2);
		SpGistInitBuffer(innerBuffer, (xldata->storesNulls ? SPGIST_NULLS : 0));
		action = BLK_NEEDS_REDO;
	}
	else
		action = XLogReadBufferForRedo(record, 2, &innerBuffer);

	if (action == BLK_NEEDS_REDO)
	{
		page = BufferGetPage(innerBuffer);

		addOrReplaceTuple(page, (Item) innerTuple, innerTupleHdr.size,
						  xldata->offnumInner);

		/* if inner is also parent, update link while we're here */
		if (xldata->innerIsParent)
		{
			SpGistInnerTuple parent;

			parent = (SpGistInnerTuple) PageGetItem(page,
								  PageGetItemId(page, xldata->offnumParent));
			spgUpdateNodeLink(parent, xldata->nodeI,
							  blknoInner, xldata->offnumInner);
		}

		PageSetLSN(page, lsn);
		MarkBufferDirty(innerBuffer);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	}
	if (BufferIsValid(innerBuffer))
		UnlockReleaseBuffer(innerBuffer);

<<<<<<< HEAD
	/* Now update src and dest page LSNs if needed */
	if (srcPage != NULL)
	{
		PageSetLSN(srcPage, lsn);
		MarkBufferDirty(srcBuffer);
	}
	if (destPage != NULL)
	{
		PageSetLSN(destPage, lsn);
		MarkBufferDirty(destBuffer);
	}

	/* restore new inner tuple */
	if (record->xl_info & XLR_BKP_BLOCK(bbi))
		(void) RestoreBackupBlock(lsn, record, bbi, false, false);
	else
	{
		Buffer		buffer = XLogReadBuffer(xldata->node, xldata->blknoInner,
											xldata->initInner);

		if (BufferIsValid(buffer))
		{
			page = BufferGetPage(buffer);

			if (xldata->initInner)
				SpGistInitBuffer(buffer,
								 (xldata->storesNulls ? SPGIST_NULLS : 0));

			if (!XLByteLE(lsn, PageGetLSN(page)))
			{
				addOrReplaceTuple(page, (Item) innerTuple, innerTuple->size,
								  xldata->offnumInner);

				/* if inner is also parent, update link while we're here */
				if (xldata->blknoInner == xldata->blknoParent)
				{
					SpGistInnerTuple parent;

					parent = (SpGistInnerTuple) PageGetItem(page,
								  PageGetItemId(page, xldata->offnumParent));
					spgUpdateNodeLink(parent, xldata->nodeI,
									xldata->blknoInner, xldata->offnumInner);
				}

				PageSetLSN(page, lsn);
				MarkBufferDirty(buffer);
			}
			UnlockReleaseBuffer(buffer);
		}
	}
	bbi++;
=======
	/*
	 * Now we can release the leaf-page locks.  It's okay to do this before
	 * updating the parent downlink.
	 */
	if (BufferIsValid(srcBuffer))
		UnlockReleaseBuffer(srcBuffer);
	if (BufferIsValid(destBuffer))
		UnlockReleaseBuffer(destBuffer);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	/*
	 * Now we can release the leaf-page locks.	It's okay to do this before
	 * updating the parent downlink.
	 */
	if (BufferIsValid(srcBuffer))
		UnlockReleaseBuffer(srcBuffer);
	if (BufferIsValid(destBuffer))
		UnlockReleaseBuffer(destBuffer);

	/* update parent downlink, unless we did it above */
	if (XLogRecHasBlockRef(record, 3))
	{
<<<<<<< HEAD
		/* no parent cause we split the root */
		Assert(SpGistBlockIsRoot(xldata->blknoInner));
	}
	else if (xldata->blknoInner != xldata->blknoParent)
	{
		if (record->xl_info & XLR_BKP_BLOCK(bbi))
			(void) RestoreBackupBlock(lsn, record, bbi, false, false);
		else
=======
		Buffer		parentBuffer;

		if (XLogReadBufferForRedo(record, 3, &parentBuffer) == BLK_NEEDS_REDO)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		{
			SpGistInnerTuple parent;

			page = BufferGetPage(parentBuffer);

			parent = (SpGistInnerTuple) PageGetItem(page,
								  PageGetItemId(page, xldata->offnumParent));
			spgUpdateNodeLink(parent, xldata->nodeI,
							  blknoInner, xldata->offnumInner);

<<<<<<< HEAD
					PageSetLSN(page, lsn);
					MarkBufferDirty(buffer);
				}
				UnlockReleaseBuffer(buffer);
			}
=======
			PageSetLSN(page, lsn);
			MarkBufferDirty(parentBuffer);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		}
		if (BufferIsValid(parentBuffer))
			UnlockReleaseBuffer(parentBuffer);
	}
	else
		Assert(xldata->innerIsParent || xldata->isRootSplit);
}

static void
spgRedoVacuumLeaf(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	char	   *ptr = XLogRecGetData(record);
	spgxlogVacuumLeaf *xldata = (spgxlogVacuumLeaf *) ptr;
	OffsetNumber *toDead;
	OffsetNumber *toPlaceholder;
	OffsetNumber *moveSrc;
	OffsetNumber *moveDest;
	OffsetNumber *chainSrc;
	OffsetNumber *chainDest;
	SpGistState state;
	Buffer		buffer;
	Page		page;
	int			i;

	fillFakeState(&state, xldata->stateSrc);

	ptr += SizeOfSpgxlogVacuumLeaf;
	toDead = (OffsetNumber *) ptr;
	ptr += sizeof(OffsetNumber) * xldata->nDead;
	toPlaceholder = (OffsetNumber *) ptr;
	ptr += sizeof(OffsetNumber) * xldata->nPlaceholder;
	moveSrc = (OffsetNumber *) ptr;
	ptr += sizeof(OffsetNumber) * xldata->nMove;
	moveDest = (OffsetNumber *) ptr;
	ptr += sizeof(OffsetNumber) * xldata->nMove;
	chainSrc = (OffsetNumber *) ptr;
	ptr += sizeof(OffsetNumber) * xldata->nChain;
	chainDest = (OffsetNumber *) ptr;

<<<<<<< HEAD
	if (record->xl_info & XLR_BKP_BLOCK(0))
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
	else
=======
	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	{
		page = BufferGetPage(buffer);

		spgPageIndexMultiDelete(&state, page,
								toDead, xldata->nDead,
								SPGIST_DEAD, SPGIST_DEAD,
								InvalidBlockNumber,
								InvalidOffsetNumber);

		spgPageIndexMultiDelete(&state, page,
								toPlaceholder, xldata->nPlaceholder,
								SPGIST_PLACEHOLDER, SPGIST_PLACEHOLDER,
								InvalidBlockNumber,
								InvalidOffsetNumber);

		/* see comments in vacuumLeafPage() */
		for (i = 0; i < xldata->nMove; i++)
		{
			ItemId		idSrc = PageGetItemId(page, moveSrc[i]);
			ItemId		idDest = PageGetItemId(page, moveDest[i]);
			ItemIdData	tmp;

			tmp = *idSrc;
			*idSrc = *idDest;
			*idDest = tmp;
		}

		spgPageIndexMultiDelete(&state, page,
								moveSrc, xldata->nMove,
								SPGIST_PLACEHOLDER, SPGIST_PLACEHOLDER,
								InvalidBlockNumber,
								InvalidOffsetNumber);

		for (i = 0; i < xldata->nChain; i++)
		{
			SpGistLeafTuple lt;

			lt = (SpGistLeafTuple) PageGetItem(page,
										   PageGetItemId(page, chainSrc[i]));
<<<<<<< HEAD
					Assert(lt->tupstate == SPGIST_LIVE);
					lt->nextOffset = chainDest[i];
				}

				PageSetLSN(page, lsn);
				MarkBufferDirty(buffer);
			}
			UnlockReleaseBuffer(buffer);
=======
			Assert(lt->tupstate == SPGIST_LIVE);
			lt->nextOffset = chainDest[i];
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		}

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

static void
spgRedoVacuumRoot(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	char	   *ptr = XLogRecGetData(record);
	spgxlogVacuumRoot *xldata = (spgxlogVacuumRoot *) ptr;
	OffsetNumber *toDelete;
	Buffer		buffer;
	Page		page;

	toDelete = xldata->offsets;

<<<<<<< HEAD
	if (record->xl_info & XLR_BKP_BLOCK(0))
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
	else
=======
	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	{
		page = BufferGetPage(buffer);

<<<<<<< HEAD
				PageSetLSN(page, lsn);
				MarkBufferDirty(buffer);
			}
			UnlockReleaseBuffer(buffer);
		}
=======
		/* The tuple numbers are in order */
		PageIndexMultiDelete(page, toDelete, xldata->nDelete);

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

static void
spgRedoVacuumRedirect(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	char	   *ptr = XLogRecGetData(record);
	spgxlogVacuumRedirect *xldata = (spgxlogVacuumRedirect *) ptr;
	OffsetNumber *itemToPlaceholder;
	Buffer		buffer;

	itemToPlaceholder = xldata->offsets;

	/*
	 * If any redirection tuples are being removed, make sure there are no
	 * live Hot Standby transactions that might need to see them.
	 */
	if (InHotStandby)
<<<<<<< HEAD
	{
		if (TransactionIdIsValid(xldata->newestRedirectXid))
			ResolveRecoveryConflictWithSnapshot(xldata->newestRedirectXid,
												xldata->node);
	}

	if (record->xl_info & XLR_BKP_BLOCK(0))
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
	else
=======
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	{
		if (TransactionIdIsValid(xldata->newestRedirectXid))
		{
			RelFileNode node;

			XLogRecGetBlockTag(record, 0, &node, NULL, NULL);
			ResolveRecoveryConflictWithSnapshot(xldata->newestRedirectXid,
												node);
		}
	}

	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		Page		page = BufferGetPage(buffer);
		SpGistPageOpaque opaque = SpGistPageGetOpaque(page);
		int			i;

		/* Convert redirect pointers to plain placeholders */
		for (i = 0; i < xldata->nToPlaceholder; i++)
		{
			SpGistDeadTuple dt;

			dt = (SpGistDeadTuple) PageGetItem(page,
								  PageGetItemId(page, itemToPlaceholder[i]));
			Assert(dt->tupstate == SPGIST_REDIRECT);
			dt->tupstate = SPGIST_PLACEHOLDER;
			ItemPointerSetInvalid(&dt->pointer);
		}

		Assert(opaque->nRedirection >= xldata->nToPlaceholder);
		opaque->nRedirection -= xldata->nToPlaceholder;
		opaque->nPlaceholder += xldata->nToPlaceholder;

		/* Remove placeholder tuples at end of page */
		if (xldata->firstPlaceholder != InvalidOffsetNumber)
		{
			int			max = PageGetMaxOffsetNumber(page);
			OffsetNumber *toDelete;

			toDelete = palloc(sizeof(OffsetNumber) * max);

			for (i = xldata->firstPlaceholder; i <= max; i++)
				toDelete[i - xldata->firstPlaceholder] = i;

			i = max - xldata->firstPlaceholder + 1;
			Assert(opaque->nPlaceholder >= i);
			opaque->nPlaceholder -= i;

<<<<<<< HEAD
				PageSetLSN(page, lsn);
				MarkBufferDirty(buffer);
			}
=======
			/* The array is sorted, so can use PageIndexMultiDelete */
			PageIndexMultiDelete(page, toDelete, i);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

			pfree(toDelete);
		}

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

/*
 * GPDB: rm_redo and rm_desc of RmgrTable have different signature from upstream
 * because xact_redo need different parameter
 */
void
<<<<<<< HEAD
spg_redo(XLogRecPtr beginLoc, XLogRecPtr lsn, XLogRecord *record)
=======
spg_redo(XLogReaderState *record)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	MemoryContext oldCxt;

	oldCxt = MemoryContextSwitchTo(opCtx);
	switch (info)
	{
		case XLOG_SPGIST_CREATE_INDEX:
			spgRedoCreateIndex(record);
			break;
		case XLOG_SPGIST_ADD_LEAF:
			spgRedoAddLeaf(record);
			break;
		case XLOG_SPGIST_MOVE_LEAFS:
			spgRedoMoveLeafs(record);
			break;
		case XLOG_SPGIST_ADD_NODE:
			spgRedoAddNode(record);
			break;
		case XLOG_SPGIST_SPLIT_TUPLE:
			spgRedoSplitTuple(record);
			break;
		case XLOG_SPGIST_PICKSPLIT:
			spgRedoPickSplit(record);
			break;
		case XLOG_SPGIST_VACUUM_LEAF:
			spgRedoVacuumLeaf(record);
			break;
		case XLOG_SPGIST_VACUUM_ROOT:
			spgRedoVacuumRoot(record);
			break;
		case XLOG_SPGIST_VACUUM_REDIRECT:
			spgRedoVacuumRedirect(record);
			break;
		default:
			elog(PANIC, "spg_redo: unknown op code %u", info);
	}

	MemoryContextSwitchTo(oldCxt);
	MemoryContextReset(opCtx);
}

<<<<<<< HEAD
static void
out_target(StringInfo buf, RelFileNode node)
{
	appendStringInfo(buf, "rel %u/%u/%u ",
					 node.spcNode, node.dbNode, node.relNode);
}

void
spg_desc(StringInfo buf, XLogRecPtr beginLoc, XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;
	char		*rec = XLogRecGetData(record);

	switch (info)
	{
		case XLOG_SPGIST_CREATE_INDEX:
			appendStringInfo(buf, "create_index: rel %u/%u/%u",
							 ((RelFileNode *) rec)->spcNode,
							 ((RelFileNode *) rec)->dbNode,
							 ((RelFileNode *) rec)->relNode);
			break;
		case XLOG_SPGIST_ADD_LEAF:
			out_target(buf, ((spgxlogAddLeaf *) rec)->node);
			appendStringInfo(buf, "add leaf to page: %u",
							 ((spgxlogAddLeaf *) rec)->blknoLeaf);
			break;
		case XLOG_SPGIST_MOVE_LEAFS:
			out_target(buf, ((spgxlogMoveLeafs *) rec)->node);
			appendStringInfo(buf, "move %u leafs from page %u to page %u",
							 ((spgxlogMoveLeafs *) rec)->nMoves,
							 ((spgxlogMoveLeafs *) rec)->blknoSrc,
							 ((spgxlogMoveLeafs *) rec)->blknoDst);
			break;
		case XLOG_SPGIST_ADD_NODE:
			out_target(buf, ((spgxlogAddNode *) rec)->node);
			appendStringInfo(buf, "add node to %u:%u",
							 ((spgxlogAddNode *) rec)->blkno,
							 ((spgxlogAddNode *) rec)->offnum);
			break;
		case XLOG_SPGIST_SPLIT_TUPLE:
			out_target(buf, ((spgxlogSplitTuple *) rec)->node);
			appendStringInfo(buf, "split node %u:%u to %u:%u",
							 ((spgxlogSplitTuple *) rec)->blknoPrefix,
							 ((spgxlogSplitTuple *) rec)->offnumPrefix,
							 ((spgxlogSplitTuple *) rec)->blknoPostfix,
							 ((spgxlogSplitTuple *) rec)->offnumPostfix);
			break;
		case XLOG_SPGIST_PICKSPLIT:
			out_target(buf, ((spgxlogPickSplit *) rec)->node);
			appendStringInfo(buf, "split leaf page");
			break;
		case XLOG_SPGIST_VACUUM_LEAF:
			out_target(buf, ((spgxlogVacuumLeaf *) rec)->node);
			appendStringInfo(buf, "vacuum leaf tuples on page %u",
							 ((spgxlogVacuumLeaf *) rec)->blkno);
			break;
		case XLOG_SPGIST_VACUUM_ROOT:
			out_target(buf, ((spgxlogVacuumRoot *) rec)->node);
			appendStringInfo(buf, "vacuum leaf tuples on root page %u",
							 ((spgxlogVacuumRoot *) rec)->blkno);
			break;
		case XLOG_SPGIST_VACUUM_REDIRECT:
			out_target(buf, ((spgxlogVacuumRedirect *) rec)->node);
			appendStringInfo(buf, "vacuum redirect tuples on page %u, newest XID %u",
							 ((spgxlogVacuumRedirect *) rec)->blkno,
							 ((spgxlogVacuumRedirect *) rec)->newestRedirectXid);
			break;
		default:
			appendStringInfo(buf, "unknown spgist op code %u", info);
			break;
	}
}

=======
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
void
spg_xlog_startup(void)
{
	opCtx = AllocSetContextCreate(CurrentMemoryContext,
								  "SP-GiST temporary context",
								  ALLOCSET_DEFAULT_MINSIZE,
								  ALLOCSET_DEFAULT_INITSIZE,
								  ALLOCSET_DEFAULT_MAXSIZE);
}

void
spg_xlog_cleanup(void)
{
	MemoryContextDelete(opCtx);
	opCtx = NULL;
}
