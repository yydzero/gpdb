/*-------------------------------------------------------------------------
 *
 * ginxlog.c
 *	  WAL replay logic for inverted index.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			 src/backend/access/gin/ginxlog.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/gin.h"
#include "access/gin_private.h"
#include "access/bufmask.h"
#include "access/xlogutils.h"
#include "utils/memutils.h"

static MemoryContext opCtx;		/* working memory for operations */

static void
ginRedoClearIncompleteSplit(XLogReaderState *record, uint8 block_id)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	Buffer		buffer;
	Page		page;

	if (XLogReadBufferForRedo(record, block_id, &buffer) == BLK_NEEDS_REDO)
	{
		page = (Page) BufferGetPage(buffer);
		GinPageGetOpaque(page)->flags &= ~GIN_INCOMPLETE_SPLIT;

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

static void
ginRedoCreateIndex(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	Buffer		RootBuffer,
				MetaBuffer;
	Page		page;

<<<<<<< HEAD
	/* Backup blocks are not used in create_index records */
	Assert(!(record->xl_info & XLR_BKP_BLOCK_MASK));

	MetaBuffer = XLogReadBuffer(*node, GIN_METAPAGE_BLKNO, true);
	Assert(BufferIsValid(MetaBuffer));
=======
	MetaBuffer = XLogInitBufferForRedo(record, 0);
	Assert(BufferGetBlockNumber(MetaBuffer) == GIN_METAPAGE_BLKNO);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	page = (Page) BufferGetPage(MetaBuffer);

	GinInitMetabuffer(MetaBuffer);

	PageSetLSN(page, lsn);
	MarkBufferDirty(MetaBuffer);

	RootBuffer = XLogInitBufferForRedo(record, 1);
	Assert(BufferGetBlockNumber(RootBuffer) == GIN_ROOT_BLKNO);
	page = (Page) BufferGetPage(RootBuffer);

	GinInitBuffer(RootBuffer, GIN_LEAF);

	PageSetLSN(page, lsn);
	MarkBufferDirty(RootBuffer);

	UnlockReleaseBuffer(RootBuffer);
	UnlockReleaseBuffer(MetaBuffer);
}

static void
ginRedoCreatePTree(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	ginxlogCreatePostingTree *data = (ginxlogCreatePostingTree *) XLogRecGetData(record);
	char	   *ptr;
	Buffer		buffer;
	Page		page;

<<<<<<< HEAD
	/* Backup blocks are not used in create_ptree records */
	Assert(!(record->xl_info & XLR_BKP_BLOCK_MASK));

	buffer = XLogReadBuffer(data->node, data->blkno, true);
	Assert(BufferIsValid(buffer));
=======
	buffer = XLogInitBufferForRedo(record, 0);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	page = (Page) BufferGetPage(buffer);

	GinInitBuffer(buffer, GIN_DATA | GIN_LEAF | GIN_COMPRESSED);

	ptr = XLogRecGetData(record) + sizeof(ginxlogCreatePostingTree);

	/* Place page data */
	memcpy(GinDataLeafPageGetPostingList(page), ptr, data->size);

	GinDataPageSetDataSize(page, data->size);

	PageSetLSN(page, lsn);

	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
}

static void
ginRedoInsertEntry(Buffer buffer, bool isLeaf, BlockNumber rightblkno, void *rdata)
{
	Page		page = BufferGetPage(buffer);
	ginxlogInsertEntry *data = (ginxlogInsertEntry *) rdata;
	OffsetNumber offset = data->offset;
	IndexTuple	itup;

	if (rightblkno != InvalidBlockNumber)
	{
		/* update link to right page after split */
		Assert(!GinPageIsLeaf(page));
		Assert(offset >= FirstOffsetNumber && offset <= PageGetMaxOffsetNumber(page));
		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offset));
		GinSetDownlink(itup, rightblkno);
	}

	if (data->isDelete)
	{
		Assert(GinPageIsLeaf(page));
		Assert(offset >= FirstOffsetNumber && offset <= PageGetMaxOffsetNumber(page));
		PageIndexTupleDelete(page, offset);
	}

	itup = &data->tuple;

	if (PageAddItem(page, (Item) itup, IndexTupleSize(itup), offset, false, false) == InvalidOffsetNumber)
	{
		RelFileNode node;
		ForkNumber	forknum;
		BlockNumber blknum;

		BufferGetTag(buffer, &node, &forknum, &blknum);
		elog(ERROR, "failed to add item to index page in %u/%u/%u",
			 node.spcNode, node.dbNode, node.relNode);
	}
}

static void
ginRedoRecompress(Page page, ginxlogRecompressDataLeaf *data)
{
	int			actionno;
	int			segno;
	GinPostingList *oldseg;
	Pointer		segmentend;
	char	   *walbuf;
	int			totalsize;

	/*
	 * If the page is in pre-9.4 format, convert to new format first.
	 */
	if (!GinPageIsCompressed(page))
	{
		ItemPointer uncompressed = (ItemPointer) GinDataPageGetData(page);
		int			nuncompressed = GinPageGetOpaque(page)->maxoff;
		int			npacked;
		GinPostingList *plist;

		plist = ginCompressPostingList(uncompressed, nuncompressed,
									   BLCKSZ, &npacked);
		Assert(npacked == nuncompressed);

		totalsize = SizeOfGinPostingList(plist);

		memcpy(GinDataLeafPageGetPostingList(page), plist, totalsize);
		GinDataPageSetDataSize(page, totalsize);
		GinPageSetCompressed(page);
		GinPageGetOpaque(page)->maxoff = InvalidOffsetNumber;
	}

	oldseg = GinDataLeafPageGetPostingList(page);
	segmentend = (Pointer) oldseg + GinDataLeafPageGetPostingListSize(page);
	segno = 0;

	walbuf = ((char *) data) + sizeof(ginxlogRecompressDataLeaf);
	for (actionno = 0; actionno < data->nactions; actionno++)
	{
		uint8		a_segno = *((uint8 *) (walbuf++));
		uint8		a_action = *((uint8 *) (walbuf++));
		GinPostingList *newseg = NULL;
		int			newsegsize = 0;
		ItemPointerData *items = NULL;
		uint16		nitems = 0;
		ItemPointerData *olditems;
		int			nolditems;
		ItemPointerData *newitems;
		int			nnewitems;
		int			segsize;
		Pointer		segptr;
		int			szleft;

		/* Extract all the information we need from the WAL record */
		if (a_action == GIN_SEGMENT_INSERT ||
			a_action == GIN_SEGMENT_REPLACE)
		{
			newseg = (GinPostingList *) walbuf;
			newsegsize = SizeOfGinPostingList(newseg);
			walbuf += SHORTALIGN(newsegsize);
		}

		if (a_action == GIN_SEGMENT_ADDITEMS)
		{
			memcpy(&nitems, walbuf, sizeof(uint16));
			walbuf += sizeof(uint16);
			items = (ItemPointerData *) walbuf;
			walbuf += nitems * sizeof(ItemPointerData);
		}

		/* Skip to the segment that this action concerns */
		Assert(segno <= a_segno);
		while (segno < a_segno)
		{
			oldseg = GinNextPostingListSegment(oldseg);
			segno++;
		}

		/*
		 * ADDITEMS action is handled like REPLACE, but the new segment to
		 * replace the old one is reconstructed using the old segment from
		 * disk and the new items from the WAL record.
		 */
		if (a_action == GIN_SEGMENT_ADDITEMS)
		{
			int			npacked;

			olditems = ginPostingListDecode(oldseg, &nolditems);

			newitems = ginMergeItemPointers(items, nitems,
											olditems, nolditems,
											&nnewitems);
			Assert(nnewitems == nolditems + nitems);

			newseg = ginCompressPostingList(newitems, nnewitems,
											BLCKSZ, &npacked);
			Assert(npacked == nnewitems);

			newsegsize = SizeOfGinPostingList(newseg);
			a_action = GIN_SEGMENT_REPLACE;
		}

		segptr = (Pointer) oldseg;
		if (segptr != segmentend)
			segsize = SizeOfGinPostingList(oldseg);
		else
		{
			/*
			 * Positioned after the last existing segment. Only INSERTs
			 * expected here.
			 */
			Assert(a_action == GIN_SEGMENT_INSERT);
			segsize = 0;
		}
		szleft = segmentend - segptr;

		switch (a_action)
		{
			case GIN_SEGMENT_DELETE:
				memmove(segptr, segptr + segsize, szleft - segsize);
				segmentend -= segsize;

				segno++;
				break;

			case GIN_SEGMENT_INSERT:
				/* make room for the new segment */
				memmove(segptr + newsegsize, segptr, szleft);
				/* copy the new segment in place */
				memcpy(segptr, newseg, newsegsize);
				segmentend += newsegsize;
				segptr += newsegsize;
				break;

			case GIN_SEGMENT_REPLACE:
				/* shift the segments that follow */
				memmove(segptr + newsegsize,
						segptr + segsize,
						szleft - segsize);
				/* copy the replacement segment in place */
				memcpy(segptr, newseg, newsegsize);
				segmentend -= segsize;
				segmentend += newsegsize;
				segptr += newsegsize;
				segno++;
				break;

			default:
				elog(ERROR, "unexpected GIN leaf action: %u", a_action);
		}
		oldseg = (GinPostingList *) segptr;
	}

	totalsize = segmentend - (Pointer) GinDataLeafPageGetPostingList(page);
	GinDataPageSetDataSize(page, totalsize);
}

static void
ginRedoInsertData(Buffer buffer, bool isLeaf, BlockNumber rightblkno, void *rdata)
{
	Page		page = BufferGetPage(buffer);

	if (isLeaf)
	{
		ginxlogRecompressDataLeaf *data = (ginxlogRecompressDataLeaf *) rdata;

		Assert(GinPageIsLeaf(page));

		ginRedoRecompress(page, data);
	}
	else
	{
		ginxlogInsertDataInternal *data = (ginxlogInsertDataInternal *) rdata;
		PostingItem *oldpitem;

		Assert(!GinPageIsLeaf(page));

		/* update link to right page after split */
		oldpitem = GinDataPageGetPostingItem(page, data->offset);
		PostingItemSetBlockNumber(oldpitem, rightblkno);

		GinDataPageAddPostingItem(page, &data->newitem, data->offset);
	}
}

static void
ginRedoInsert(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	ginxlogInsert *data = (ginxlogInsert *) XLogRecGetData(record);
	Buffer		buffer;
#ifdef NOT_USED
	BlockNumber leftChildBlkno = InvalidBlockNumber;
#endif
	BlockNumber rightChildBlkno = InvalidBlockNumber;
	bool		isLeaf = (data->flags & GIN_INSERT_ISLEAF) != 0;

	/*
	 * First clear incomplete-split flag on child page if this finishes a
	 * split.
	 */
	if (!isLeaf)
	{
		char	   *payload = XLogRecGetData(record) + sizeof(ginxlogInsert);

#ifdef NOT_USED
		leftChildBlkno = BlockIdGetBlockNumber((BlockId) payload);
#endif
		payload += sizeof(BlockIdData);
		rightChildBlkno = BlockIdGetBlockNumber((BlockId) payload);
		payload += sizeof(BlockIdData);

		ginRedoClearIncompleteSplit(record, 1);
	}

<<<<<<< HEAD
	/* If we have a full-page image, restore it and we're done */
	if (IsBkpBlockApplied(record, 0))
	{
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
		return;
	}

	buffer = XLogReadBuffer(data->node, data->blkno, false);
	if (!BufferIsValid(buffer))
		return;					/* page was deleted, nothing to do */
	page = (Page) BufferGetPage(buffer);

	if (!XLByteLE(lsn, PageGetLSN(page)))
=======
	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	{
		Page		page = BufferGetPage(buffer);
		Size		len;
		char	   *payload = XLogRecGetBlockData(record, 0, &len);

		/* How to insert the payload is tree-type specific */
		if (data->flags & GIN_INSERT_ISDATA)
		{
			Assert(GinPageIsData(page));
			ginRedoInsertData(buffer, isLeaf, rightChildBlkno, payload);
		}
		else
		{
			Assert(!GinPageIsData(page));
<<<<<<< HEAD

			if (data->updateBlkno != InvalidBlockNumber)
			{
				/* update link to right page after split */
				Assert(!GinPageIsLeaf(page));
				Assert(data->offset >= FirstOffsetNumber && data->offset <= PageGetMaxOffsetNumber(page));
				itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, data->offset));
				GinSetDownlink(itup, data->updateBlkno);
			}

			if (data->isDelete)
			{
				Assert(GinPageIsLeaf(page));
				Assert(data->offset >= FirstOffsetNumber && data->offset <= PageGetMaxOffsetNumber(page));
				PageIndexTupleDelete(page, data->offset);
			}

			itup = (IndexTuple) (XLogRecGetData(record) + sizeof(ginxlogInsert));

			if (PageAddItem(page, (Item) itup, IndexTupleSize(itup), data->offset, false, false) == InvalidOffsetNumber)
				elog(ERROR, "failed to add item to index page in %u/%u/%u",
					 data->node.spcNode, data->node.dbNode, data->node.relNode);
		}

		PageSetLSN(page, lsn);

=======
			ginRedoInsertEntry(buffer, isLeaf, rightChildBlkno, payload);
		}

		PageSetLSN(page, lsn);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

static void
ginRedoSplit(XLogReaderState *record)
{
	ginxlogSplit *data = (ginxlogSplit *) XLogRecGetData(record);
	Buffer		lbuffer,
				rbuffer,
				rootbuf;
	bool		isLeaf = (data->flags & GIN_INSERT_ISLEAF) != 0;
	bool		isRoot = (data->flags & GIN_SPLIT_ROOT) != 0;

	/*
	 * First clear incomplete-split flag on child page if this finishes a
	 * split
	 */
	if (!isLeaf)
		ginRedoClearIncompleteSplit(record, 3);

<<<<<<< HEAD
	/* Backup blocks are not used in split records */
	Assert(!(record->xl_info & XLR_BKP_BLOCK_MASK));

	lbuffer = XLogReadBuffer(data->node, data->lblkno, true);
	Assert(BufferIsValid(lbuffer));
	lpage = (Page) BufferGetPage(lbuffer);
	GinInitBuffer(lbuffer, flags);
=======
	if (XLogReadBufferForRedo(record, 0, &lbuffer) != BLK_RESTORED)
		elog(ERROR, "GIN split record did not contain a full-page image of left page");
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	if (XLogReadBufferForRedo(record, 1, &rbuffer) != BLK_RESTORED)
		elog(ERROR, "GIN split record did not contain a full-page image of right page");

	if (isRoot)
	{
<<<<<<< HEAD
		IndexTuple	itup = (IndexTuple) (XLogRecGetData(record) + sizeof(ginxlogSplit));
		OffsetNumber i;

		for (i = 0; i < data->separator; i++)
		{
			if (PageAddItem(lpage, (Item) itup, IndexTupleSize(itup), InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
				elog(ERROR, "failed to add item to index page in %u/%u/%u",
				  data->node.spcNode, data->node.dbNode, data->node.relNode);
			itup = (IndexTuple) (((char *) itup) + MAXALIGN(IndexTupleSize(itup)));
		}

		for (i = data->separator; i < data->nitem; i++)
		{
			if (PageAddItem(rpage, (Item) itup, IndexTupleSize(itup), InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
				elog(ERROR, "failed to add item to index page in %u/%u/%u",
				  data->node.spcNode, data->node.dbNode, data->node.relNode);
			itup = (IndexTuple) (((char *) itup) + MAXALIGN(IndexTupleSize(itup)));
		}
	}

	PageSetLSN(rpage, lsn);
	MarkBufferDirty(rbuffer);

	PageSetLSN(lpage, lsn);
	MarkBufferDirty(lbuffer);

	if (!data->isLeaf && data->updateBlkno != InvalidBlockNumber)
		forgetIncompleteSplit(data->node, data->leftChildBlkno, data->updateBlkno);

	if (data->isRootSplit)
	{
		Buffer		rootBuf = XLogReadBuffer(data->node, data->rootBlkno, true);
		Page		rootPage = BufferGetPage(rootBuf);

		GinInitBuffer(rootBuf, flags & ~GIN_LEAF);

		if (data->isData)
		{
			Assert(data->rootBlkno != GIN_ROOT_BLKNO);
			ginDataFillRoot(NULL, rootBuf, lbuffer, rbuffer);
		}
		else
		{
			Assert(data->rootBlkno == GIN_ROOT_BLKNO);
			ginEntryFillRoot(NULL, rootBuf, lbuffer, rbuffer);
		}

		PageSetLSN(rootPage, lsn);

		MarkBufferDirty(rootBuf);
		UnlockReleaseBuffer(rootBuf);
=======
		if (XLogReadBufferForRedo(record, 2, &rootbuf) != BLK_RESTORED)
			elog(ERROR, "GIN split record did not contain a full-page image of root page");
		UnlockReleaseBuffer(rootbuf);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	}

	UnlockReleaseBuffer(rbuffer);
	UnlockReleaseBuffer(lbuffer);
}

/*
 * VACUUM_PAGE record contains simply a full image of the page, similar to
 * an XLOG_FPI record.
 */
static void
ginRedoVacuumPage(XLogReaderState *record)
{
	Buffer		buffer;

<<<<<<< HEAD
	if (IsBkpBlockApplied(record, 0))
	{
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
		return;
	}
=======
	if (XLogReadBufferForRedo(record, 0, &buffer) != BLK_RESTORED)
	{
		elog(ERROR, "replay of gin entry tree page vacuum did not restore the page");
	}
	UnlockReleaseBuffer(buffer);
}
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

static void
ginRedoVacuumDataLeafPage(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	Buffer		buffer;

	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		Page		page = BufferGetPage(buffer);
		Size		len;
		ginxlogVacuumDataLeafPage *xlrec;

		xlrec = (ginxlogVacuumDataLeafPage *) XLogRecGetBlockData(record, 0, &len);

		Assert(GinPageIsLeaf(page));
		Assert(GinPageIsData(page));

		ginRedoRecompress(page, &xlrec->data);
		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

static void
ginRedoDeletePage(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	ginxlogDeletePage *data = (ginxlogDeletePage *) XLogRecGetData(record);
	Buffer		dbuffer;
	Buffer		pbuffer;
	Buffer		lbuffer;
	Page		page;

<<<<<<< HEAD
	if (IsBkpBlockApplied(record, 0))
		dbuffer = RestoreBackupBlock(lsn, record, 0, false, true);
	else
	{
		dbuffer = XLogReadBuffer(data->node, data->blkno, false);
		if (BufferIsValid(dbuffer))
		{
			page = BufferGetPage(dbuffer);
			if (!XLByteLE(lsn, PageGetLSN(page)))
			{
				Assert(GinPageIsData(page));
				GinPageGetOpaque(page)->flags = GIN_DELETED;
				PageSetLSN(page, lsn);
				MarkBufferDirty(dbuffer);
			}
		}
	}

	if (IsBkpBlockApplied(record, 1))
		pbuffer = RestoreBackupBlock(lsn, record, 1, false, true);
	else
	{
		pbuffer = XLogReadBuffer(data->node, data->parentBlkno, false);
		if (BufferIsValid(pbuffer))
		{
			page = BufferGetPage(pbuffer);
			if (!XLByteLE(lsn, PageGetLSN(page)))
			{
				Assert(GinPageIsData(page));
				Assert(!GinPageIsLeaf(page));
				GinPageDeletePostingItem(page, data->parentOffset);
				PageSetLSN(page, lsn);
				MarkBufferDirty(pbuffer);
			}
		}
	}

	if (IsBkpBlockApplied(record, 2))
		(void) RestoreBackupBlock(lsn, record, 2, false, false);
	else if (data->leftBlkno != InvalidBlockNumber)
	{
		lbuffer = XLogReadBuffer(data->node, data->leftBlkno, false);
		if (BufferIsValid(lbuffer))
		{
			page = BufferGetPage(lbuffer);
			if (!XLByteLE(lsn, PageGetLSN(page)))
			{
				Assert(GinPageIsData(page));
				GinPageGetOpaque(page)->rightlink = data->rightLink;
				PageSetLSN(page, lsn);
				MarkBufferDirty(lbuffer);
			}
			UnlockReleaseBuffer(lbuffer);
		}
	}

=======
	if (XLogReadBufferForRedo(record, 0, &dbuffer) == BLK_NEEDS_REDO)
	{
		page = BufferGetPage(dbuffer);
		Assert(GinPageIsData(page));
		GinPageGetOpaque(page)->flags = GIN_DELETED;
		PageSetLSN(page, lsn);
		MarkBufferDirty(dbuffer);
	}

	if (XLogReadBufferForRedo(record, 1, &pbuffer) == BLK_NEEDS_REDO)
	{
		page = BufferGetPage(pbuffer);
		Assert(GinPageIsData(page));
		Assert(!GinPageIsLeaf(page));
		GinPageDeletePostingItem(page, data->parentOffset);
		PageSetLSN(page, lsn);
		MarkBufferDirty(pbuffer);
	}

	if (XLogReadBufferForRedo(record, 2, &lbuffer) == BLK_NEEDS_REDO)
	{
		page = BufferGetPage(lbuffer);
		Assert(GinPageIsData(page));
		GinPageGetOpaque(page)->rightlink = data->rightLink;
		PageSetLSN(page, lsn);
		MarkBufferDirty(lbuffer);
	}

	if (BufferIsValid(lbuffer))
		UnlockReleaseBuffer(lbuffer);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	if (BufferIsValid(pbuffer))
		UnlockReleaseBuffer(pbuffer);
	if (BufferIsValid(dbuffer))
		UnlockReleaseBuffer(dbuffer);
}

static void
ginRedoUpdateMetapage(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	ginxlogUpdateMeta *data = (ginxlogUpdateMeta *) XLogRecGetData(record);
	Buffer		metabuffer;
	Page		metapage;
	Buffer		buffer;

	/*
	 * Restore the metapage. This is essentially the same as a full-page
	 * image, so restore the metapage unconditionally without looking at the
	 * LSN, to avoid torn page hazards.
	 */
	metabuffer = XLogInitBufferForRedo(record, 0);
	Assert(BufferGetBlockNumber(metabuffer) == GIN_METAPAGE_BLKNO);
	metapage = BufferGetPage(metabuffer);

<<<<<<< HEAD
	if (!XLByteLE(lsn, PageGetLSN(metapage)))
	{
		memcpy(GinPageGetMeta(metapage), &data->metadata, sizeof(GinMetaPageData));
		PageSetLSN(metapage, lsn);
		MarkBufferDirty(metabuffer);
	}
=======
	GinInitPage(metapage, GIN_META, BufferGetPageSize(metabuffer));
	memcpy(GinPageGetMeta(metapage), &data->metadata, sizeof(GinMetaPageData));
	PageSetLSN(metapage, lsn);
	MarkBufferDirty(metabuffer);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

	if (data->ntuples > 0)
	{
		/*
		 * insert into tail page
		 */
<<<<<<< HEAD
		if (record->xl_info & XLR_BKP_BLOCK(0))
			(void) RestoreBackupBlock(lsn, record, 0, false, false);
		else
=======
		if (XLogReadBufferForRedo(record, 1, &buffer) == BLK_NEEDS_REDO)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		{
			Page		page = BufferGetPage(buffer);
			OffsetNumber off;
			int			i;
			Size		tupsize;
			char	   *payload;
			IndexTuple	tuples;
			Size		totaltupsize;

			payload = XLogRecGetBlockData(record, 1, &totaltupsize);
			tuples = (IndexTuple) payload;

			if (PageIsEmpty(page))
				off = FirstOffsetNumber;
			else
				off = OffsetNumberNext(PageGetMaxOffsetNumber(page));

			for (i = 0; i < data->ntuples; i++)
			{
				tupsize = IndexTupleSize(tuples);

				if (PageAddItem(page, (Item) tuples, tupsize, off,
								false, false) == InvalidOffsetNumber)
					elog(ERROR, "failed to add item to index page");

				tuples = (IndexTuple) (((char *) tuples) + tupsize);

<<<<<<< HEAD
						l = PageAddItem(page, (Item) tuples, tupsize, off, false, false);

						if (l == InvalidOffsetNumber)
							elog(ERROR, "failed to add item to index page");

						tuples = (IndexTuple) (((char *) tuples) + tupsize);

						off++;
					}

					/*
					 * Increase counter of heap tuples
					 */
					GinPageGetOpaque(page)->maxoff++;

					PageSetLSN(page, lsn);
					MarkBufferDirty(buffer);
				}
				UnlockReleaseBuffer(buffer);
=======
				off++;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
			}
			Assert(payload + totaltupsize == (char *) tuples);

			/*
			 * Increase counter of heap tuples
			 */
			GinPageGetOpaque(page)->maxoff++;

			PageSetLSN(page, lsn);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);
	}
	else if (data->prevTail != InvalidBlockNumber)
	{
		/*
		 * New tail
		 */
<<<<<<< HEAD
		if (record->xl_info & XLR_BKP_BLOCK(0))
			(void) RestoreBackupBlock(lsn, record, 0, false, false);
		else
		{
			buffer = XLogReadBuffer(data->node, data->prevTail, false);
			if (BufferIsValid(buffer))
			{
				Page		page = BufferGetPage(buffer);

				if (!XLByteLE(lsn, PageGetLSN(page)))
				{
					GinPageGetOpaque(page)->rightlink = data->newRightlink;

					PageSetLSN(page, lsn);
					MarkBufferDirty(buffer);
				}
				UnlockReleaseBuffer(buffer);
			}
=======
		if (XLogReadBufferForRedo(record, 1, &buffer) == BLK_NEEDS_REDO)
		{
			Page		page = BufferGetPage(buffer);

			GinPageGetOpaque(page)->rightlink = data->newRightlink;

			PageSetLSN(page, lsn);
			MarkBufferDirty(buffer);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);
	}

	UnlockReleaseBuffer(metabuffer);
}

static void
ginRedoInsertListPage(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	ginxlogInsertListPage *data = (ginxlogInsertListPage *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;
	OffsetNumber l,
				off = FirstOffsetNumber;
	int			i,
				tupsize;
	char	   *payload;
	IndexTuple	tuples;
	Size		totaltupsize;

<<<<<<< HEAD
	/* If we have a full-page image, restore it and we're done */
	if (record->xl_info & XLR_BKP_BLOCK(0))
	{
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
		return;
	}

	buffer = XLogReadBuffer(data->node, data->blkno, true);
	Assert(BufferIsValid(buffer));
=======
	/* We always re-initialize the page. */
	buffer = XLogInitBufferForRedo(record, 0);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	page = BufferGetPage(buffer);

	GinInitBuffer(buffer, GIN_LIST);
	GinPageGetOpaque(page)->rightlink = data->rightlink;
	if (data->rightlink == InvalidBlockNumber)
	{
		/* tail of sublist */
		GinPageSetFullRow(page);
		GinPageGetOpaque(page)->maxoff = 1;
	}
	else
	{
		GinPageGetOpaque(page)->maxoff = 0;
	}

	payload = XLogRecGetBlockData(record, 0, &totaltupsize);

	tuples = (IndexTuple) payload;
	for (i = 0; i < data->ntuples; i++)
	{
		tupsize = IndexTupleSize(tuples);

		l = PageAddItem(page, (Item) tuples, tupsize, off, false, false);

		if (l == InvalidOffsetNumber)
			elog(ERROR, "failed to add item to index page");

		tuples = (IndexTuple) (((char *) tuples) + tupsize);
		off++;
	}
	Assert((char *) tuples == payload + totaltupsize);

	PageSetLSN(page, lsn);
	MarkBufferDirty(buffer);

	UnlockReleaseBuffer(buffer);
}

static void
ginRedoDeleteListPages(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	ginxlogDeleteListPages *data = (ginxlogDeleteListPages *) XLogRecGetData(record);
	Buffer		metabuffer;
	Page		metapage;
	int			i;

<<<<<<< HEAD
	/* Backup blocks are not used in delete_listpage records */
	Assert(!(record->xl_info & XLR_BKP_BLOCK_MASK));

	metabuffer = XLogReadBuffer(data->node, GIN_METAPAGE_BLKNO, false);
	if (!BufferIsValid(metabuffer))
		return;					/* assume index was deleted, nothing to do */
	metapage = BufferGetPage(metabuffer);

	if (!XLByteLE(lsn, PageGetLSN(metapage)))
	{
		memcpy(GinPageGetMeta(metapage), &data->metadata, sizeof(GinMetaPageData));
		PageSetLSN(metapage, lsn);
		MarkBufferDirty(metabuffer);
	}

	/*
	 * In normal operation, shiftList() takes exclusive lock on all the
	 * pages-to-be-deleted simultaneously.	During replay, however, it should
=======
	metabuffer = XLogInitBufferForRedo(record, 0);
	Assert(BufferGetBlockNumber(metabuffer) == GIN_METAPAGE_BLKNO);
	metapage = BufferGetPage(metabuffer);

	GinInitPage(metapage, GIN_META, BufferGetPageSize(metabuffer));

	memcpy(GinPageGetMeta(metapage), &data->metadata, sizeof(GinMetaPageData));
	PageSetLSN(metapage, lsn);
	MarkBufferDirty(metabuffer);

	/*
	 * In normal operation, shiftList() takes exclusive lock on all the
	 * pages-to-be-deleted simultaneously.  During replay, however, it should
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	 * be all right to lock them one at a time.  This is dependent on the fact
	 * that we are deleting pages from the head of the list, and that readers
	 * share-lock the next page before releasing the one they are on. So we
	 * cannot get past a reader that is on, or due to visit, any page we are
	 * going to delete.  New incoming readers will block behind our metapage
	 * lock and then see a fully updated page list.
<<<<<<< HEAD
=======
	 *
	 * No full-page images are taken of the deleted pages. Instead, they are
	 * re-initialized as empty, deleted pages. Their right-links don't need to
	 * be preserved, because no new readers can see the pages, as explained
	 * above.
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	 */
	for (i = 0; i < data->ndeleted; i++)
	{
		Buffer		buffer;
		Page		page;

		buffer = XLogInitBufferForRedo(record, i + 1);
		page = BufferGetPage(buffer);
		GinInitBuffer(buffer, GIN_DELETED);

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);

<<<<<<< HEAD
				PageSetLSN(page, lsn);
				MarkBufferDirty(buffer);
			}

			UnlockReleaseBuffer(buffer);
		}
=======
		UnlockReleaseBuffer(buffer);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	}
	UnlockReleaseBuffer(metabuffer);
}

void
<<<<<<< HEAD
gin_redo(XLogRecPtr beginLoc, XLogRecPtr lsn, XLogRecord *record)
=======
gin_redo(XLogReaderState *record)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	MemoryContext oldCtx;

	/*
	 * GIN indexes do not require any conflict processing. NB: If we ever
	 * implement a similar optimization as we have in b-tree, and remove
	 * killed tuples outside VACUUM, we'll need to handle that here.
	 */

	oldCtx = MemoryContextSwitchTo(opCtx);
	switch (info)
	{
		case XLOG_GIN_CREATE_INDEX:
			ginRedoCreateIndex(record);
			break;
		case XLOG_GIN_CREATE_PTREE:
			ginRedoCreatePTree(record);
			break;
		case XLOG_GIN_INSERT:
			ginRedoInsert(record);
			break;
		case XLOG_GIN_SPLIT:
			ginRedoSplit(record);
			break;
		case XLOG_GIN_VACUUM_PAGE:
			ginRedoVacuumPage(record);
			break;
		case XLOG_GIN_VACUUM_DATA_LEAF_PAGE:
			ginRedoVacuumDataLeafPage(record);
			break;
		case XLOG_GIN_DELETE_PAGE:
			ginRedoDeletePage(record);
			break;
		case XLOG_GIN_UPDATE_META_PAGE:
			ginRedoUpdateMetapage(record);
			break;
		case XLOG_GIN_INSERT_LISTPAGE:
			ginRedoInsertListPage(record);
			break;
		case XLOG_GIN_DELETE_LISTPAGE:
			ginRedoDeleteListPages(record);
			break;
		default:
			elog(PANIC, "gin_redo: unknown op code %u", info);
	}
	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(opCtx);
}

<<<<<<< HEAD
static void
desc_node(StringInfo buf, RelFileNode node, BlockNumber blkno)
{
	appendStringInfo(buf, "node: %u/%u/%u blkno: %u",
					 node.spcNode, node.dbNode, node.relNode, blkno);
}

void
gin_desc(StringInfo buf, XLogRecPtr beginLoc, XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;
	char		*rec = XLogRecGetData(record);

	switch (info)
	{
		case XLOG_GIN_CREATE_INDEX:
			appendStringInfo(buf, "Create index, ");
			desc_node(buf, *(RelFileNode *) rec, GIN_ROOT_BLKNO);
			break;
		case XLOG_GIN_CREATE_PTREE:
			appendStringInfo(buf, "Create posting tree, ");
			desc_node(buf, ((ginxlogCreatePostingTree *) rec)->node, ((ginxlogCreatePostingTree *) rec)->blkno);
			break;
		case XLOG_GIN_INSERT:
			appendStringInfo(buf, "Insert item, ");
			desc_node(buf, ((ginxlogInsert *) rec)->node, ((ginxlogInsert *) rec)->blkno);
			appendStringInfo(buf, " offset: %u nitem: %u isdata: %c isleaf %c isdelete %c updateBlkno:%u",
							 ((ginxlogInsert *) rec)->offset,
							 ((ginxlogInsert *) rec)->nitem,
							 (((ginxlogInsert *) rec)->isData) ? 'T' : 'F',
							 (((ginxlogInsert *) rec)->isLeaf) ? 'T' : 'F',
							 (((ginxlogInsert *) rec)->isDelete) ? 'T' : 'F',
							 ((ginxlogInsert *) rec)->updateBlkno);
			break;
		case XLOG_GIN_SPLIT:
			appendStringInfo(buf, "Page split, ");
			desc_node(buf, ((ginxlogSplit *) rec)->node, ((ginxlogSplit *) rec)->lblkno);
			appendStringInfo(buf, " isrootsplit: %c", (((ginxlogSplit *) rec)->isRootSplit) ? 'T' : 'F');
			break;
		case XLOG_GIN_VACUUM_PAGE:
			appendStringInfo(buf, "Vacuum page, ");
			desc_node(buf, ((ginxlogVacuumPage *) rec)->node, ((ginxlogVacuumPage *) rec)->blkno);
			break;
		case XLOG_GIN_DELETE_PAGE:
			appendStringInfo(buf, "Delete page, ");
			desc_node(buf, ((ginxlogDeletePage *) rec)->node, ((ginxlogDeletePage *) rec)->blkno);
			break;
		case XLOG_GIN_UPDATE_META_PAGE:
			appendStringInfo(buf, "Update metapage, ");
			desc_node(buf, ((ginxlogUpdateMeta *) rec)->node, GIN_METAPAGE_BLKNO);
			break;
		case XLOG_GIN_INSERT_LISTPAGE:
			appendStringInfo(buf, "Insert new list page, ");
			desc_node(buf, ((ginxlogInsertListPage *) rec)->node, ((ginxlogInsertListPage *) rec)->blkno);
			break;
		case XLOG_GIN_DELETE_LISTPAGE:
			appendStringInfo(buf, "Delete list pages (%d), ", ((ginxlogDeleteListPages *) rec)->ndeleted);
			desc_node(buf, ((ginxlogDeleteListPages *) rec)->node, GIN_METAPAGE_BLKNO);
			break;
		default:
			elog(PANIC, "gin_desc: unknown op code %u", info);
	}
}

=======
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
void
gin_xlog_startup(void)
{
	opCtx = AllocSetContextCreate(CurrentMemoryContext,
								  "GIN recovery temporary context",
								  ALLOCSET_DEFAULT_MINSIZE,
								  ALLOCSET_DEFAULT_INITSIZE,
								  ALLOCSET_DEFAULT_MAXSIZE);
}

<<<<<<< HEAD
static void
ginContinueSplit(ginIncompleteSplit *split)
{
	GinBtreeData btree;
	GinState	ginstate;
	Relation	reln;
	Buffer		buffer;
	GinBtreeStack stack;

	/*
	 * elog(NOTICE,"ginContinueSplit root:%u l:%u r:%u",  split->rootBlkno,
	 * split->leftBlkno, split->rightBlkno);
	 */

	buffer = XLogReadBuffer(split->node, split->leftBlkno, false);

	/*
	 * Failure should be impossible here, because we wrote the page earlier.
	 */
	if (!BufferIsValid(buffer))
		elog(PANIC, "ginContinueSplit: left block %u not found",
			 split->leftBlkno);

	reln = CreateFakeRelcacheEntry(split->node);

	/*
	 * Failure should be impossible here, because we wrote the page earlier.
	 */
	if (!BufferIsValid(buffer))
		elog(PANIC, "ginContinueSplit: left block %u not found",
			 split->leftBlkno);

	if (split->rootBlkno == GIN_ROOT_BLKNO)
	{
		MemSet(&ginstate, 0, sizeof(ginstate));
		ginstate.index = reln;

		ginPrepareEntryScan(&btree,
							InvalidOffsetNumber, (Datum) 0, GIN_CAT_NULL_KEY,
							&ginstate);
		btree.entry = ginPageGetLinkItup(buffer);
	}
	else
	{
		Page		page = BufferGetPage(buffer);

		ginPrepareDataScan(&btree, reln);

		PostingItemSetBlockNumber(&(btree.pitem), split->leftBlkno);
		if (GinPageIsLeaf(page))
			btree.pitem.key = *(ItemPointerData *) GinDataPageGetItem(page,
											 GinPageGetOpaque(page)->maxoff);
		else
			btree.pitem.key = ((PostingItem *) GinDataPageGetItem(page,
									   GinPageGetOpaque(page)->maxoff))->key;
	}

	btree.rightblkno = split->rightBlkno;

	stack.blkno = split->leftBlkno;
	stack.buffer = buffer;
	stack.off = InvalidOffsetNumber;
	stack.parent = NULL;

	ginFindParents(&btree, &stack, split->rootBlkno);
	ginInsertValue(&btree, stack.parent, NULL);

	FreeFakeRelcacheEntry(reln);

	UnlockReleaseBuffer(buffer);
}

=======
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
void
gin_xlog_cleanup(void)
{
	MemoryContextDelete(opCtx);
}

/*
 * Mask a GIN page before running consistency checks on it.
 */
void
gin_mask(char *pagedata, BlockNumber blkno)
{
	Page		page = (Page) pagedata;
	GinPageOpaque opaque;

	mask_page_lsn_and_checksum(page);
	opaque = GinPageGetOpaque(page);

	mask_page_hint_bits(page);

	/*
	 * GIN metapage doesn't use pd_lower/pd_upper. Other page types do. Hence,
	 * we need to apply masking for those pages.
	 */
	if (opaque->flags != GIN_META)
	{
		/*
		 * For GIN_DELETED page, the page is initialized to empty. Hence, mask
		 * the page content.
		 */
		if (opaque->flags & GIN_DELETED)
			mask_page_content(page);
		else
			mask_unused_space(page);
	}
}
