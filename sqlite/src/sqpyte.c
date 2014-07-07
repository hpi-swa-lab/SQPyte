#include <assert.h>

i64 lastRowid;
unsigned nVmStep = 0;      /* Number of virtual machine steps */

/* Opcode: Transaction P1 P2 P3 P4 P5
**
** Begin a transaction on database P1 if a transaction is not already
** active.
** If P2 is non-zero, then a write-transaction is started, or if a 
** read-transaction is already active, it is upgraded to a write-transaction.
** If P2 is zero, then a read-transaction is started.
**
** P1 is the index of the database file on which the transaction is
** started.  Index 0 is the main database file and index 1 is the
** file used for temporary tables.  Indices of 2 or more are used for
** attached databases.
**
** If a write-transaction is started and the Vdbe.usesStmtJournal flag is
** true (this flag is set if the Vdbe may modify more than one row and may
** throw an ABORT exception), a statement transaction may also be opened.
** More specifically, a statement transaction is opened iff the database
** connection is currently not in autocommit mode, or if there are other
** active statements. A statement transaction allows the changes made by this
** VDBE to be rolled back after an error without having to roll back the
** entire transaction. If no error is encountered, the statement transaction
** will automatically commit when the VDBE halts.
**
** If P5!=0 then this opcode also checks the schema cookie against P3
** and the schema generation counter against P4.
** The cookie changes its value whenever the database schema changes.
** This operation is used to detect when that the cookie has changed
** and that the current process needs to reread the schema.  If the schema
** cookie in P3 differs from the schema cookie in the database header or
** if the schema generation counter in P4 differs from the current
** generation counter, then an SQLITE_SCHEMA error is raised and execution
** halts.  The sqlite3_step() wrapper function might then reprepare the
** statement and rerun it from the beginning.
*/
int impl_OP_Transaction(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
  int rc;
  Btree *pBt;
  int iMeta;
  int iGen;

  assert( p->bIsReader );
  assert( p->readOnly==0 || pOp->p2==0 );
  assert( pOp->p1>=0 && pOp->p1<db->nDb );
  assert( (p->btreeMask & (((yDbMask)1)<<pOp->p1))!=0 );
  if( pOp->p2 && (db->flags & SQLITE_QueryOnly)!=0 ){
    rc = SQLITE_READONLY;
    // goto abort_due_to_error;
    printf("In impl_OP_Transaction(): abort_due_to_error 1.\n");
    assert(0);
  }
  pBt = db->aDb[pOp->p1].pBt;

  if( pBt ){
    rc = sqlite3BtreeBeginTrans(pBt, pOp->p2);
    if( rc==SQLITE_BUSY ){
      p->pc = pc;
      p->rc = rc = SQLITE_BUSY;
      // Translated: goto vdbe_return;
      db->lastRowid = lastRowid;
      testcase( nVmStep>0 );
      p->aCounter[SQLITE_STMTSTATUS_VM_STEP] += (int)nVmStep;
      sqlite3VdbeLeave(p);
      return rc;
    }
    if( rc!=SQLITE_OK ){
      // goto abort_due_to_error;
      printf("In impl_OP_Transaction(): abort_due_to_error 2.\n");
      assert(0);
    }

    if( pOp->p2 && p->usesStmtJournal 
     && (db->autoCommit==0 || db->nVdbeRead>1) 
    ){
      assert( sqlite3BtreeIsInTrans(pBt) );
      if( p->iStatement==0 ){
        assert( db->nStatement>=0 && db->nSavepoint>=0 );
        db->nStatement++; 
        p->iStatement = db->nSavepoint + db->nStatement;
      }

      rc = sqlite3VtabSavepoint(db, SAVEPOINT_BEGIN, p->iStatement-1);
      if( rc==SQLITE_OK ){
        rc = sqlite3BtreeBeginStmt(pBt, p->iStatement);
      }

      /* Store the current value of the database handles deferred constraint
      ** counter. If the statement transaction needs to be rolled back,
      ** the value of this counter needs to be restored too.  */
      p->nStmtDefCons = db->nDeferredCons;
      p->nStmtDefImmCons = db->nDeferredImmCons;
    }

    /* Gather the schema version number for checking */
    sqlite3BtreeGetMeta(pBt, BTREE_SCHEMA_VERSION, (u32 *)&iMeta);
    iGen = db->aDb[pOp->p1].pSchema->iGeneration;
  }else{
    iGen = iMeta = 0;
  }
  assert( pOp->p5==0 || pOp->p4type==P4_INT32 );
  if( pOp->p5 && (iMeta!=pOp->p3 || iGen!=pOp->p4.i) ){
    sqlite3DbFree(db, p->zErrMsg);
    p->zErrMsg = sqlite3DbStrDup(db, "database schema has changed");
    /* If the schema-cookie from the database file matches the cookie 
    ** stored with the in-memory representation of the schema, do
    ** not reload the schema from the database file.
    **
    ** If virtual-tables are in use, this is not just an optimization.
    ** Often, v-tables store their data in other SQLite tables, which
    ** are queried from within xNext() and other v-table methods using
    ** prepared queries. If such a query is out-of-date, we do not want to
    ** discard the database schema, as the user code implementing the
    ** v-table would have to be ready for the sqlite3_vtab structure itself
    ** to be invalidated whenever sqlite3_step() is called from within 
    ** a v-table method.
    */
    if( db->aDb[pOp->p1].pSchema->schema_cookie!=iMeta ){
      sqlite3ResetOneSchema(db, pOp->p1);
    }
    p->expired = 1;
    rc = SQLITE_SCHEMA;
  }
  return rc;
}

/* Opcode: TableLock P1 P2 P3 P4 *
** Synopsis: iDb=P1 root=P2 write=P3
**
** Obtain a lock on a particular table. This instruction is only used when
** the shared-cache feature is enabled. 
**
** P1 is the index of the database in sqlite3.aDb[] of the database
** on which the lock is acquired.  A readlock is obtained if P3==0 or
** a write lock if P3==1.
**
** P2 contains the root-page of the table to lock.
**
** P4 contains a pointer to the name of the table being locked. This is only
** used to generate an error message if the lock cannot be obtained.
*/
int impl_OP_TableLock(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
  int rc;

  u8 isWriteLock = (u8)pOp->p3;
  if( isWriteLock || 0==(db->flags&SQLITE_ReadUncommitted) ){
    int p1 = pOp->p1; 
    assert( p1>=0 && p1<db->nDb );
    assert( (p->btreeMask & (((yDbMask)1)<<p1))!=0 );
    assert( isWriteLock==0 || isWriteLock==1 );
    rc = sqlite3BtreeLockTable(db->aDb[p1].pBt, pOp->p2, isWriteLock);
    if( (rc&0xFF)==SQLITE_LOCKED ){
      const char *z = pOp->p4.z;
      sqlite3SetString(&p->zErrMsg, db, "database table is locked: %s", z);
    }
  }
  return rc;
}

/* Opcode:  Goto * P2 * * *
**
** An unconditional jump to address P2.
** The next instruction executed will be 
** the one at index P2 from the beginning of
** the program.
**
** The P1 parameter is not actually used by this opcode.  However, it
** is sometimes set to 1 instead of 0 as a hint to the command-line shell
** that this Goto is the bottom of a loop and that the lines from P2 down
** to the current line should be indented for EXPLAIN output.
*/
int impl_OP_Goto(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
  pc = pOp->p2 - 1;

  /* Opcodes that are used as the bottom of a loop (OP_Next, OP_Prev,
  ** OP_VNext, OP_RowSetNext, or OP_SorterNext) all jump here upon
  ** completion.  Check to see if sqlite3_interrupt() has been called
  ** or if the progress callback needs to be invoked. 
  **
  ** This code uses unstructured "goto" statements and does not look clean.
  ** But that is not due to sloppy coding habits. The code is written this
  ** way for performance, to avoid having to run the interrupt and progress
  ** checks on every opcode.  This helps sqlite3_step() to run about 1.5%
  ** faster according to "valgrind --tool=cachegrind" */

// check_for_interrupt:
//   if( db->u1.isInterrupted ) goto abort_due_to_interrupt;

// #ifndef SQLITE_OMIT_PROGRESS_CALLBACK
  /* Call the progress callback if it is configured and the required number
  ** of VDBE ops have been executed (either since this invocation of
  ** sqlite3VdbeExec() or since last time the progress callback was called).
  ** If the progress callback returns non-zero, exit the virtual machine with
  ** a return code SQLITE_ABORT.
  */
  // if( db->xProgress!=0 && nVmStep>=nProgressLimit ){
  //   assert( db->nProgressOps!=0 );
  //   nProgressLimit = nVmStep + db->nProgressOps - (nVmStep%db->nProgressOps);
  //   if( db->xProgress(db->pProgressArg) ){
  //     rc = SQLITE_INTERRUPT;
  //     goto vdbe_error_halt;
  //   }
  // }
// #endif
  return pc;
}

/* Opcode: OpenRead P1 P2 P3 P4 P5
** Synopsis: root=P2 iDb=P3
**
** Open a read-only cursor for the database table whose root page is
** P2 in a database file.  The database file is determined by P3. 
** P3==0 means the main database, P3==1 means the database used for 
** temporary tables, and P3>1 means used the corresponding attached
** database.  Give the new cursor an identifier of P1.  The P1
** values need not be contiguous but all P1 values should be small integers.
** It is an error for P1 to be negative.
**
** If P5!=0 then use the content of register P2 as the root page, not
** the value of P2 itself.
**
** There will be a read lock on the database whenever there is an
** open cursor.  If the database was unlocked prior to this instruction
** then a read lock is acquired as part of this instruction.  A read
** lock allows other processes to read the database but prohibits
** any other process from modifying the database.  The read lock is
** released when all cursors are closed.  If this instruction attempts
** to get a read lock but fails, the script terminates with an
** SQLITE_BUSY error code.
**
** The P4 value may be either an integer (P4_INT32) or a pointer to
** a KeyInfo structure (P4_KEYINFO). If it is a pointer to a KeyInfo 
** structure, then said structure defines the content and collating 
** sequence of the index being opened. Otherwise, if P4 is an integer 
** value, it is set to the number of columns in the table.
**
** See also OpenWrite.
*/
/* Opcode: OpenWrite P1 P2 P3 P4 P5
** Synopsis: root=P2 iDb=P3
**
** Open a read/write cursor named P1 on the table or index whose root
** page is P2.  Or if P5!=0 use the content of register P2 to find the
** root page.
**
** The P4 value may be either an integer (P4_INT32) or a pointer to
** a KeyInfo structure (P4_KEYINFO). If it is a pointer to a KeyInfo 
** structure, then said structure defines the content and collating 
** sequence of the index being opened. Otherwise, if P4 is an integer 
** value, it is set to the number of columns in the table, or to the
** largest index of any column of the table that is actually used.
**
** This instruction works just like OpenRead except that it opens the cursor
** in read/write mode.  For a given table, there can be one or more read-only
** cursors or a single read/write cursor but not both.
**
** See also OpenRead.
*/
void impl_OP_OpenWrite(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
  int nField;
  KeyInfo *pKeyInfo;
  int p2;
  int iDb;
  int wrFlag;
  Btree *pX;
  VdbeCursor *pCur;
  Db *pDb;
  int rc;
  Mem *pIn2;

  Mem *aMem = p->aMem;

  assert( (pOp->p5&(OPFLAG_P2ISREG|OPFLAG_BULKCSR))==pOp->p5 );
  assert( pOp->opcode==OP_OpenWrite || pOp->p5==0 );
  assert( p->bIsReader );
  assert( pOp->opcode==OP_OpenRead || p->readOnly==0 );

  if( p->expired ){
    rc = SQLITE_ABORT;
    // Translated: break;
    return;
  }

  nField = 0;
  pKeyInfo = 0;
  p2 = pOp->p2;
  iDb = pOp->p3;
  assert( iDb>=0 && iDb<db->nDb );
  assert( (p->btreeMask & (((yDbMask)1)<<iDb))!=0 );
  pDb = &db->aDb[iDb];
  pX = pDb->pBt;
  assert( pX!=0 );
  if( pOp->opcode==OP_OpenWrite ){
    wrFlag = 1;
    assert( sqlite3SchemaMutexHeld(db, iDb, 0) );
    if( pDb->pSchema->file_format < p->minWriteFileFormat ){
      p->minWriteFileFormat = pDb->pSchema->file_format;
    }
  }else{
    wrFlag = 0;
  }
  if( pOp->p5 & OPFLAG_P2ISREG ){
    assert( p2>0 );
    assert( p2<=(p->nMem-p->nCursor) );
    pIn2 = &aMem[p2];
    assert( memIsValid(pIn2) );
    assert( (pIn2->flags & MEM_Int)!=0 );
    sqlite3VdbeMemIntegerify(pIn2);
    p2 = (int)pIn2->u.i;
    /* The p2 value always comes from a prior OP_CreateTable opcode and
    ** that opcode will always set the p2 value to 2 or more or else fail.
    ** If there were a failure, the prepared statement would have halted
    ** before reaching this instruction. */
    if( NEVER(p2<2) ) {
      rc = SQLITE_CORRUPT_BKPT;
      // goto abort_due_to_error;
      printf("In impl_OP_OpenWrite(): abort_due_to_error.\n");
      assert(0);
    }
  }
  if( pOp->p4type==P4_KEYINFO ){
    pKeyInfo = pOp->p4.pKeyInfo;
    assert( pKeyInfo->enc==ENC(db) );
    assert( pKeyInfo->db==db );
    nField = pKeyInfo->nField+pKeyInfo->nXField;
  }else if( pOp->p4type==P4_INT32 ){
    nField = pOp->p4.i;
  }
  assert( pOp->p1>=0 );
  assert( nField>=0 );
  testcase( nField==0 );  /* Table with INTEGER PRIMARY KEY and nothing else */
  pCur = allocateCursor(p, pOp->p1, nField, iDb, 1);
  if( pCur==0 ) {
    // goto no_mem;
      printf("In impl_OP_OpenWrite(): no_mem.\n");
      assert(0);
  }
  pCur->nullRow = 1;
  pCur->isOrdered = 1;
  rc = sqlite3BtreeCursor(pX, p2, wrFlag, pKeyInfo, pCur->pCursor);
  pCur->pKeyInfo = pKeyInfo;
  assert( OPFLAG_BULKCSR==BTREE_BULKLOAD );
  sqlite3BtreeCursorHints(pCur->pCursor, (pOp->p5 & OPFLAG_BULKCSR));

  /* Since it performs no memory allocation or IO, the only value that
  ** sqlite3BtreeCursor() may return is SQLITE_OK. */
  assert( rc==SQLITE_OK );

  /* Set the VdbeCursor.isTable variable. Previous versions of
  ** SQLite used to check if the root-page flags were sane at this point
  ** and report database corruption if they were not, but this check has
  ** since moved into the btree layer.  */  
  pCur->isTable = pOp->p4type!=P4_KEYINFO;
}

void impl_OP_OpenRead(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
  impl_OP_OpenWrite(p, db, pc, pOp);
}

/* Opcode: Rewind P1 P2 * * *
**
** The next use of the Rowid or Column or Next instruction for P1 
** will refer to the first entry in the database table or index.
** If the table or index is empty and P2>0, then jump immediately to P2.
** If P2 is 0 or if the table or index is not empty, fall through
** to the following instruction.
*/
int impl_OP_Rewind(Vdbe *p, sqlite3 *db, int *pc, Op *pOp) {
  int rc;
  VdbeCursor *pC;
  BtCursor *pCrsr;
  int res;

  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  assert( isSorter(pC)==(pOp->opcode==OP_SorterSort) );
  res = 1;
  if( isSorter(pC) ){
    rc = sqlite3VdbeSorterRewind(db, pC, &res);
  }else{
    pCrsr = pC->pCursor;
    assert( pCrsr );
    rc = sqlite3BtreeFirst(pCrsr, &res);
    pC->deferredMoveto = 0;
    pC->cacheStatus = CACHE_STALE;
    pC->rowidIsValid = 0;
  }
  pC->nullRow = (u8)res;
  assert( pOp->p2>0 && pOp->p2<p->nOp );
  VdbeBranchTaken(res!=0,2);
  if( res ){
    *pc = pOp->p2 - 1;
  }
  return rc;
}

/* Opcode: Column P1 P2 P3 P4 P5
** Synopsis:  r[P3]=PX
**
** Interpret the data that cursor P1 points to as a structure built using
** the MakeRecord instruction.  (See the MakeRecord opcode for additional
** information about the format of the data.)  Extract the P2-th column
** from this record.  If there are less that (P2+1) 
** values in the record, extract a NULL.
**
** The value extracted is stored in register P3.
**
** If the column contains fewer than P2 fields, then extract a NULL.  Or,
** if the P4 argument is a P4_MEM use the value of the P4 argument as
** the result.
**
** If the OPFLAG_CLEARCACHE bit is set on P5 and P1 is a pseudo-table cursor,
** then the cache of the cursor is reset prior to extracting the column.
** The first OP_Column against a pseudo-table after the value of the content
** register has changed should have this bit set.
**
** If the OPFLAG_LENGTHARG and OPFLAG_TYPEOFARG bits are set on P5 when
** the result is guaranteed to only be used as the argument of a length()
** or typeof() function, respectively.  The loading of large blobs can be
** skipped for length() and all content loading can be skipped for typeof().
*/
int impl_OP_Column(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
  i64 payloadSize64; /* Number of bytes in the record */
  int p2;            /* column number to retrieve */
  VdbeCursor *pC;    /* The VDBE cursor */
  BtCursor *pCrsr;   /* The BTree cursor */
  u32 *aType;        /* aType[i] holds the numeric type of the i-th column */
  u32 *aOffset;      /* aOffset[i] is offset to start of data for i-th column */
  int len;           /* The length of the serialized data for the column */
  int i;             /* Loop counter */
  Mem *pDest;        /* Where to write the extracted value */
  Mem sMem;          /* For storing the record being decoded */
  const u8 *zData;   /* Part of the record being decoded */
  const u8 *zHdr;    /* Next unparsed byte of the header */
  const u8 *zEndHdr; /* Pointer to first byte after the header */
  u32 offset;        /* Offset into the data */
  u32 szField;       /* Number of bytes in the content of a field */
  u32 avail;         /* Number of bytes of available data */
  u32 t;             /* A type code from the record header */
  Mem *pReg;         /* PseudoTable input register */
  int rc;
  Mem *aMem = p->aMem;
  u8 encoding = ENC(db);     /* The database encoding */

  p2 = pOp->p2;
  assert( pOp->p3>0 && pOp->p3<=(p->nMem-p->nCursor) );
  pDest = &aMem[pOp->p3];
  memAboutToChange(p, pDest);
  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  assert( p2<pC->nField );
  aType = pC->aType;
  aOffset = aType + pC->nField;
#ifndef SQLITE_OMIT_VIRTUALTABLE
  assert( pC->pVtabCursor==0 ); /* OP_Column never called on virtual table */
#endif
  pCrsr = pC->pCursor;
  assert( pCrsr!=0 || pC->pseudoTableReg>0 ); /* pCrsr NULL on PseudoTables */
  assert( pCrsr!=0 || pC->nullRow );          /* pC->nullRow on PseudoTables */

  /* If the cursor cache is stale, bring it up-to-date */
  rc = sqlite3VdbeCursorMoveto(pC);
  if( rc ) {
    // goto abort_due_to_error;
      printf("In impl_OP_Column(): abort_due_to_error.\n");
      assert(0);
  }
  if( pC->cacheStatus!=p->cacheCtr || (pOp->p5&OPFLAG_CLEARCACHE)!=0 ){
    if( pC->nullRow ){
      if( pCrsr==0 ){
        assert( pC->pseudoTableReg>0 );
        pReg = &aMem[pC->pseudoTableReg];
        assert( pReg->flags & MEM_Blob );
        assert( memIsValid(pReg) );
        pC->payloadSize = pC->szRow = avail = pReg->n;
        pC->aRow = (u8*)pReg->z;
      }else{
        MemSetTypeFlag(pDest, MEM_Null);
        goto op_column_out;
      }
    }else{
      assert( pCrsr );
      if( pC->isTable==0 ){
        assert( sqlite3BtreeCursorIsValid(pCrsr) );
        VVA_ONLY(rc =) sqlite3BtreeKeySize(pCrsr, &payloadSize64);
        assert( rc==SQLITE_OK ); /* True because of CursorMoveto() call above */
        /* sqlite3BtreeParseCellPtr() uses getVarint32() to extract the
        ** payload size, so it is impossible for payloadSize64 to be
        ** larger than 32 bits. */
        assert( (payloadSize64 & SQLITE_MAX_U32)==(u64)payloadSize64 );
        pC->aRow = sqlite3BtreeKeyFetch(pCrsr, &avail);
        pC->payloadSize = (u32)payloadSize64;
      }else{
        assert( sqlite3BtreeCursorIsValid(pCrsr) );
        VVA_ONLY(rc =) sqlite3BtreeDataSize(pCrsr, &pC->payloadSize);
        assert( rc==SQLITE_OK );   /* DataSize() cannot fail */
        pC->aRow = sqlite3BtreeDataFetch(pCrsr, &avail);
      }
      assert( avail<=65536 );  /* Maximum page size is 64KiB */
      if( pC->payloadSize <= (u32)avail ){
        pC->szRow = pC->payloadSize;
      }else{
        pC->szRow = avail;
      }
      if( pC->payloadSize > (u32)db->aLimit[SQLITE_LIMIT_LENGTH] ){
        // goto too_big;
        printf("In impl_OP_Column(): too_big.\n");
        assert(0);
      }
    }
    pC->cacheStatus = p->cacheCtr;
    pC->iHdrOffset = getVarint32(pC->aRow, offset);
    pC->nHdrParsed = 0;
    aOffset[0] = offset;
    if( avail<offset ){
      /* pC->aRow does not have to hold the entire row, but it does at least
      ** need to cover the header of the record.  If pC->aRow does not contain
      ** the complete header, then set it to zero, forcing the header to be
      ** dynamically allocated. */
      pC->aRow = 0;
      pC->szRow = 0;
    }

    /* Make sure a corrupt database has not given us an oversize header.
    ** Do this now to avoid an oversize memory allocation.
    **
    ** Type entries can be between 1 and 5 bytes each.  But 4 and 5 byte
    ** types use so much data space that there can only be 4096 and 32 of
    ** them, respectively.  So the maximum header length results from a
    ** 3-byte type for each of the maximum of 32768 columns plus three
    ** extra bytes for the header length itself.  32768*3 + 3 = 98307.
    */
    if( offset > 98307 || offset > pC->payloadSize ){
      rc = SQLITE_CORRUPT_BKPT;
      goto op_column_error;
    }
  }

  /* Make sure at least the first p2+1 entries of the header have been
  ** parsed and valid information is in aOffset[] and aType[].
  */
  if( pC->nHdrParsed<=p2 ){
    /* If there is more header available for parsing in the record, try
    ** to extract additional fields up through the p2+1-th field 
    */
    if( pC->iHdrOffset<aOffset[0] ){
      /* Make sure zData points to enough of the record to cover the header. */
      if( pC->aRow==0 ){
        memset(&sMem, 0, sizeof(sMem));
        rc = sqlite3VdbeMemFromBtree(pCrsr, 0, aOffset[0], 
                                     !pC->isTable, &sMem);
        if( rc!=SQLITE_OK ){
          goto op_column_error;
        }
        zData = (u8*)sMem.z;
      }else{
        zData = pC->aRow;
      }
  
      /* Fill in aType[i] and aOffset[i] values through the p2-th field. */
      i = pC->nHdrParsed;
      offset = aOffset[i];
      zHdr = zData + pC->iHdrOffset;
      zEndHdr = zData + aOffset[0];
      assert( i<=p2 && zHdr<zEndHdr );
      do{
        if( zHdr[0]<0x80 ){
          t = zHdr[0];
          zHdr++;
        }else{
          zHdr += sqlite3GetVarint32(zHdr, &t);
        }
        aType[i] = t;
        szField = sqlite3VdbeSerialTypeLen(t);
        offset += szField;
        if( offset<szField ){  /* True if offset overflows */
          zHdr = &zEndHdr[1];  /* Forces SQLITE_CORRUPT return below */
          break;
        }
        i++;
        aOffset[i] = offset;
      }while( i<=p2 && zHdr<zEndHdr );
      pC->nHdrParsed = i;
      pC->iHdrOffset = (u32)(zHdr - zData);
      if( pC->aRow==0 ){
        sqlite3VdbeMemRelease(&sMem);
        sMem.flags = MEM_Null;
      }
  
      /* If we have read more header data than was contained in the header,
      ** or if the end of the last field appears to be past the end of the
      ** record, or if the end of the last field appears to be before the end
      ** of the record (when all fields present), then we must be dealing 
      ** with a corrupt database.
      */
      if( (zHdr > zEndHdr)
       || (offset > pC->payloadSize)
       || (zHdr==zEndHdr && offset!=pC->payloadSize)
      ){
        rc = SQLITE_CORRUPT_BKPT;
        goto op_column_error;
      }
    }

    /* If after trying to extra new entries from the header, nHdrParsed is
    ** still not up to p2, that means that the record has fewer than p2
    ** columns.  So the result will be either the default value or a NULL.
    */
    if( pC->nHdrParsed<=p2 ){
      if( pOp->p4type==P4_MEM ){
        sqlite3VdbeMemShallowCopy(pDest, pOp->p4.pMem, MEM_Static);
      }else{
        MemSetTypeFlag(pDest, MEM_Null);
      }
      goto op_column_out;
    }
  }

  /* Extract the content for the p2+1-th column.  Control can only
  ** reach this point if aOffset[p2], aOffset[p2+1], and aType[p2] are
  ** all valid.
  */
  assert( p2<pC->nHdrParsed );
  assert( rc==SQLITE_OK );
  assert( sqlite3VdbeCheckMemInvariants(pDest) );
  if( pC->szRow>=aOffset[p2+1] ){
    /* This is the common case where the desired content fits on the original
    ** page - where the content is not on an overflow page */
    VdbeMemRelease(pDest);
    sqlite3VdbeSerialGet(pC->aRow+aOffset[p2], aType[p2], pDest);
  }else{
    /* This branch happens only when content is on overflow pages */
    t = aType[p2];
    if( ((pOp->p5 & (OPFLAG_LENGTHARG|OPFLAG_TYPEOFARG))!=0
          && ((t>=12 && (t&1)==0) || (pOp->p5 & OPFLAG_TYPEOFARG)!=0))
     || (len = sqlite3VdbeSerialTypeLen(t))==0
    ){
      /* Content is irrelevant for the typeof() function and for
      ** the length(X) function if X is a blob.  So we might as well use
      ** bogus content rather than reading content from disk.  NULL works
      ** for text and blob and whatever is in the payloadSize64 variable
      ** will work for everything else.  Content is also irrelevant if
      ** the content length is 0. */
      zData = t<=13 ? (u8*)&payloadSize64 : 0;
      sMem.zMalloc = 0;
    }else{
      memset(&sMem, 0, sizeof(sMem));
      sqlite3VdbeMemMove(&sMem, pDest);
      rc = sqlite3VdbeMemFromBtree(pCrsr, aOffset[p2], len, !pC->isTable,
                                   &sMem);
      if( rc!=SQLITE_OK ){
        goto op_column_error;
      }
      zData = (u8*)sMem.z;
    }
    sqlite3VdbeSerialGet(zData, t, pDest);
    /* If we dynamically allocated space to hold the data (in the
    ** sqlite3VdbeMemFromBtree() call above) then transfer control of that
    ** dynamically allocated space over to the pDest structure.
    ** This prevents a memory copy. */
    if( sMem.zMalloc ){
      assert( sMem.z==sMem.zMalloc );
      assert( VdbeMemDynamic(pDest)==0 );
      assert( (pDest->flags & (MEM_Blob|MEM_Str))==0 || pDest->z==sMem.z );
      pDest->flags &= ~(MEM_Ephem|MEM_Static);
      pDest->flags |= MEM_Term;
      pDest->z = sMem.z;
      pDest->zMalloc = sMem.zMalloc;
    }
  }
  pDest->enc = encoding;

op_column_out:
  // Translated Deephemeralize(pDest);
  if( ((pDest)->flags&MEM_Ephem)!=0 && sqlite3VdbeMemMakeWriteable(pDest) ) {
    // goto no_mem;
    printf("In impl_OP_Column(): no_mem.\n");
    assert(0);
  }

op_column_error:
  UPDATE_MAX_BLOBSIZE(pDest);
  REGISTER_TRACE(pOp->p3, pDest);

  return rc;
}

/* Opcode: ResultRow P1 P2 * * *
** Synopsis:  output=r[P1@P2]
**
** The registers P1 through P1+P2-1 contain a single row of
** results. This opcode causes the sqlite3_step() call to terminate
** with an SQLITE_ROW return code and it sets up the sqlite3_stmt
** structure to provide access to the r(P1)..r(P1+P2-1) values as
** the result row.
*/
int impl_OP_ResultRow(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
  Mem *pMem;
  int i;
  int rc;

  Mem *aMem = p->aMem;

  assert( p->nResColumn==pOp->p2 );
  assert( pOp->p1>0 );
  assert( pOp->p1+pOp->p2<=(p->nMem-p->nCursor)+1 );

// #ifndef SQLITE_OMIT_PROGRESS_CALLBACK
  /* Run the progress counter just before returning.
  */
  // if( db->xProgress!=0
  //  && nVmStep>=nProgressLimit
  //  && db->xProgress(db->pProgressArg)!=0
  // ){
  //   rc = SQLITE_INTERRUPT;
  //   goto vdbe_error_halt;
  // }
// #endif

  /* If this statement has violated immediate foreign key constraints, do
  ** not return the number of rows modified. And do not RELEASE the statement
  ** transaction. It needs to be rolled back.  */
  if( SQLITE_OK!=(rc = sqlite3VdbeCheckFk(p, 0)) ){
    assert( db->flags&SQLITE_CountRows );
    assert( p->usesStmtJournal );
    // Translated: break;
    return rc;
  }

  /* If the SQLITE_CountRows flag is set in sqlite3.flags mask, then 
  ** DML statements invoke this opcode to return the number of rows 
  ** modified to the user. This is the only way that a VM that
  ** opens a statement transaction may invoke this opcode.
  **
  ** In case this is such a statement, close any statement transaction
  ** opened by this VM before returning control to the user. This is to
  ** ensure that statement-transactions are always nested, not overlapping.
  ** If the open statement-transaction is not closed here, then the user
  ** may step another VM that opens its own statement transaction. This
  ** may lead to overlapping statement transactions.
  **
  ** The statement transaction is never a top-level transaction.  Hence
  ** the RELEASE call below can never fail.
  */
  assert( p->iStatement==0 || db->flags&SQLITE_CountRows );
  rc = sqlite3VdbeCloseStatement(p, SAVEPOINT_RELEASE);
  if( NEVER(rc!=SQLITE_OK) ){
    // Translated: break;
    return rc;
  }

  /* Invalidate all ephemeral cursor row caches */
  p->cacheCtr = (p->cacheCtr + 2)|1;

  /* Make sure the results of the current row are \000 terminated
  ** and have an assigned type.  The results are de-ephemeralized as
  ** a side effect.
  */
  pMem = p->pResultSet = &aMem[pOp->p1];
  for(i=0; i<pOp->p2; i++){
    assert( memIsValid(&pMem[i]) );

    // Translated Deephemeralize(&pMem[i]);
    if( ((&pMem[i])->flags&MEM_Ephem)!=0 && sqlite3VdbeMemMakeWriteable(&pMem[i]) ) {
      // goto no_mem;
      printf("In impl_OP_ResultRow(): no_mem.\n");
      assert(0);
    }


    assert( (pMem[i].flags & MEM_Ephem)==0
            || (pMem[i].flags & (MEM_Str|MEM_Blob))==0 );
    sqlite3VdbeMemNulTerminate(&pMem[i]);
    REGISTER_TRACE(pOp->p1+i, &pMem[i]);
  }
  if( db->mallocFailed ) {
    // goto no_mem;
    printf("In impl_OP_ResultRow(): no_mem.\n");
    assert(0);
  }

  /* Return SQLITE_ROW
  */
  p->pc = pc + 1;
  rc = SQLITE_ROW;
  // Translated: goto vdbe_return;
  db->lastRowid = lastRowid;
  testcase( nVmStep>0 );
  p->aCounter[SQLITE_STMTSTATUS_VM_STEP] += (int)nVmStep;
  sqlite3VdbeLeave(p);
  return rc;
}

/* Opcode: Next P1 P2 P3 P4 P5
**
** Advance cursor P1 so that it points to the next key/data pair in its
** table or index.  If there are no more key/value pairs then fall through
** to the following instruction.  But if the cursor advance was successful,
** jump immediately to P2.
**
** The P1 cursor must be for a real table, not a pseudo-table.  P1 must have
** been opened prior to this opcode or the program will segfault.
**
** The P3 value is a hint to the btree implementation. If P3==1, that
** means P1 is an SQL index and that this instruction could have been
** omitted if that index had been unique.  P3 is usually 0.  P3 is
** always either 0 or 1.
**
** P4 is always of type P4_ADVANCE. The function pointer points to
** sqlite3BtreeNext().
**
** If P5 is positive and the jump is taken, then event counter
** number P5-1 in the prepared statement is incremented.
**
** See also: Prev, NextIfOpen
*/
/* Opcode: NextIfOpen P1 P2 P3 P4 P5
**
** This opcode works just like OP_Next except that if cursor P1 is not
** open it behaves a no-op.
*/
int impl_OP_Next(Vdbe *p, sqlite3 *db, int *pc, Op *pOp) {
  VdbeCursor *pC;
  int res;
  int rc;

  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  assert( pOp->p5<ArraySize(p->aCounter) );
  pC = p->apCsr[pOp->p1];
  res = pOp->p3;
  assert( pC!=0 );
  assert( pC->deferredMoveto==0 );
  assert( pC->pCursor );
  assert( res==0 || (res==1 && pC->isTable==0) );
  testcase( res==1 );
  assert( pOp->opcode!=OP_Next || pOp->p4.xAdvance==sqlite3BtreeNext );
  assert( pOp->opcode!=OP_Prev || pOp->p4.xAdvance==sqlite3BtreePrevious );
  assert( pOp->opcode!=OP_NextIfOpen || pOp->p4.xAdvance==sqlite3BtreeNext );
  assert( pOp->opcode!=OP_PrevIfOpen || pOp->p4.xAdvance==sqlite3BtreePrevious);
  rc = pOp->p4.xAdvance(pC->pCursor, &res);
next_tail:
  pC->cacheStatus = CACHE_STALE;
  VdbeBranchTaken(res==0,2);
  if( res==0 ){
    pC->nullRow = 0;
    *pc = pOp->p2 - 1;
    p->aCounter[pOp->p5]++;
#ifdef SQLITE_TEST
    sqlite3_search_count++;
#endif
  }else{
    pC->nullRow = 1;
  }
  pC->rowidIsValid = 0;
  // goto check_for_interrupt;
  return rc;
}

/* Opcode: Close P1 * * * *
**
** Close a cursor previously opened as P1.  If P1 is not
** currently open, this instruction is a no-op.
*/
void impl_OP_Close(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  sqlite3VdbeFreeCursor(p, p->apCsr[pOp->p1]);
  p->apCsr[pOp->p1] = 0;
}

/* Opcode:  Halt P1 P2 * P4 P5
**
** Exit immediately.  All open cursors, etc are closed
** automatically.
**
** P1 is the result code returned by sqlite3_exec(), sqlite3_reset(),
** or sqlite3_finalize().  For a normal halt, this should be SQLITE_OK (0).
** For errors, it can be some other value.  If P1!=0 then P2 will determine
** whether or not to rollback the current transaction.  Do not rollback
** if P2==OE_Fail. Do the rollback if P2==OE_Rollback.  If P2==OE_Abort,
** then back out all changes that have occurred during this execution of the
** VDBE, but do not rollback the transaction. 
**
** If P4 is not null then it is an error message string.
**
** P5 is a value between 0 and 4, inclusive, that modifies the P4 string.
**
**    0:  (no change)
**    1:  NOT NULL contraint failed: P4
**    2:  UNIQUE constraint failed: P4
**    3:  CHECK constraint failed: P4
**    4:  FOREIGN KEY constraint failed: P4
**
** If P5 is not zero and P4 is NULL, then everything after the ":" is
** omitted.
**
** There is an implied "Halt 0 0 0" instruction inserted at the very end of
** every program.  So a jump past the last instruction of the program
** is the same as executing Halt.
*/
int impl_OP_Halt(Vdbe *p, sqlite3 *db, int *pc, Op *pOp) {
  const char *zType;
  const char *zLogFmt;
  int rc;
  Op *aOp;
  Mem *aMem = p->aMem;

  if( pOp->p1==SQLITE_OK && p->pFrame ){
    /* Halt the sub-program. Return control to the parent frame. */
    VdbeFrame *pFrame = p->pFrame;
    p->pFrame = pFrame->pParent;
    p->nFrame--;
    sqlite3VdbeSetChanges(db, p->nChange);
    *pc = sqlite3VdbeFrameRestore(pFrame);
    lastRowid = db->lastRowid;
    if( pOp->p2==OE_Ignore ){
      /* Instruction pc is the OP_Program that invoked the sub-program 
      ** currently being halted. If the p2 instruction of this OP_Halt
      ** instruction is set to OE_Ignore, then the sub-program is throwing
      ** an IGNORE exception. In this case jump to the address specified
      ** as the p2 of the calling OP_Program.  */
      *pc = p->aOp[*pc].p2-1;
    }
    aOp = p->aOp;
    aMem = p->aMem;
    // Translated: break;
    return rc;
  }
  p->rc = pOp->p1;
  p->errorAction = (u8)pOp->p2;
  p->pc = *pc;
  if( p->rc ){
    if( pOp->p5 ){
      static const char * const azType[] = { "NOT NULL", "UNIQUE", "CHECK",
                                             "FOREIGN KEY" };
      assert( pOp->p5>=1 && pOp->p5<=4 );
      testcase( pOp->p5==1 );
      testcase( pOp->p5==2 );
      testcase( pOp->p5==3 );
      testcase( pOp->p5==4 );
      zType = azType[pOp->p5-1];
    }else{
      zType = 0;
    }
    assert( zType!=0 || pOp->p4.z!=0 );
    zLogFmt = "abort at %d in [%s]: %s";
    if( zType && pOp->p4.z ){
      sqlite3SetString(&p->zErrMsg, db, "%s constraint failed: %s", 
                       zType, pOp->p4.z);
    }else if( pOp->p4.z ){
      sqlite3SetString(&p->zErrMsg, db, "%s", pOp->p4.z);
    }else{
      sqlite3SetString(&p->zErrMsg, db, "%s constraint failed", zType);
    }
    sqlite3_log(pOp->p1, zLogFmt, *pc, p->zSql, p->zErrMsg);
  }
  rc = sqlite3VdbeHalt(p);
  assert( rc==SQLITE_BUSY || rc==SQLITE_OK || rc==SQLITE_ERROR );
  if( rc==SQLITE_BUSY ){
    p->rc = rc = SQLITE_BUSY;
  }else{
    assert( rc==SQLITE_OK || (p->rc&0xff)==SQLITE_CONSTRAINT );
    assert( rc==SQLITE_OK || db->nDeferredCons>0 || db->nDeferredImmCons>0 );
    rc = p->rc ? SQLITE_ERROR : SQLITE_DONE;
  }
  // Translated: goto vdbe_return;
  db->lastRowid = lastRowid;
  testcase( nVmStep>0 );
  p->aCounter[SQLITE_STMTSTATUS_VM_STEP] += (int)nVmStep;
  sqlite3VdbeLeave(p);
  return rc;
}

/* Opcode: Lt P1 P2 P3 P4 P5
** Synopsis: if r[P1]<r[P3] goto P2
**
** Compare the values in register P1 and P3.  If reg(P3)<reg(P1) then
** jump to address P2.  
**
** If the SQLITE_JUMPIFNULL bit of P5 is set and either reg(P1) or
** reg(P3) is NULL then take the jump.  If the SQLITE_JUMPIFNULL 
** bit is clear then fall through if either operand is NULL.
**
** The SQLITE_AFF_MASK portion of P5 must be an affinity character -
** SQLITE_AFF_TEXT, SQLITE_AFF_INTEGER, and so forth. An attempt is made 
** to coerce both inputs according to this affinity before the
** comparison is made. If the SQLITE_AFF_MASK is 0x00, then numeric
** affinity is used. Note that the affinity conversions are stored
** back into the input registers P1 and P3.  So this opcode can cause
** persistent changes to registers P1 and P3.
**
** Once any conversions have taken place, and neither value is NULL, 
** the values are compared. If both values are blobs then memcmp() is
** used to determine the results of the comparison.  If both values
** are text, then the appropriate collating function specified in
** P4 is  used to do the comparison.  If P4 is not specified then
** memcmp() is used to compare text string.  If both values are
** numeric, then a numeric comparison is used. If the two values
** are of different types, then numbers are considered less than
** strings and strings are considered less than blobs.
**
** If the SQLITE_STOREP2 bit of P5 is set, then do not jump.  Instead,
** store a boolean result (either 0, or 1, or NULL) in register P2.
**
** If the SQLITE_NULLEQ bit is set in P5, then NULL values are considered
** equal to one another, provided that they do not have their MEM_Cleared
** bit set.
*/
/* Opcode: Ne P1 P2 P3 P4 P5
** Synopsis: if r[P1]!=r[P3] goto P2
**
** This works just like the Lt opcode except that the jump is taken if
** the operands in registers P1 and P3 are not equal.  See the Lt opcode for
** additional information.
**
** If SQLITE_NULLEQ is set in P5 then the result of comparison is always either
** true or false and is never NULL.  If both operands are NULL then the result
** of comparison is false.  If either operand is NULL then the result is true.
** If neither operand is NULL the result is the same as it would be if
** the SQLITE_NULLEQ flag were omitted from P5.
*/
/* Opcode: Eq P1 P2 P3 P4 P5
** Synopsis: if r[P1]==r[P3] goto P2
**
** This works just like the Lt opcode except that the jump is taken if
** the operands in registers P1 and P3 are equal.
** See the Lt opcode for additional information.
**
** If SQLITE_NULLEQ is set in P5 then the result of comparison is always either
** true or false and is never NULL.  If both operands are NULL then the result
** of comparison is true.  If either operand is NULL then the result is false.
** If neither operand is NULL the result is the same as it would be if
** the SQLITE_NULLEQ flag were omitted from P5.
*/
/* Opcode: Le P1 P2 P3 P4 P5
** Synopsis: if r[P1]<=r[P3] goto P2
**
** This works just like the Lt opcode except that the jump is taken if
** the content of register P3 is less than or equal to the content of
** register P1.  See the Lt opcode for additional information.
*/
/* Opcode: Gt P1 P2 P3 P4 P5
** Synopsis: if r[P1]>r[P3] goto P2
**
** This works just like the Lt opcode except that the jump is taken if
** the content of register P3 is greater than the content of
** register P1.  See the Lt opcode for additional information.
*/
/* Opcode: Ge P1 P2 P3 P4 P5
** Synopsis: if r[P1]>=r[P3] goto P2
**
** This works just like the Lt opcode except that the jump is taken if
** the content of register P3 is greater than or equal to the content of
** register P1.  See the Lt opcode for additional information.
*/
int impl_OP_Ne_Eq_Gt_Le_Lt_Ge(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_Eq:               /* same as TK_EQ, jump, in1, in3 */
// case OP_Ne:               /* same as TK_NE, jump, in1, in3 */
// case OP_Lt:               /* same as TK_LT, jump, in1, in3 */
// case OP_Le:               /* same as TK_LE, jump, in1, in3 */
// case OP_Gt:               /* same as TK_GT, jump, in1, in3 */
// case OP_Ge: {             /* same as TK_GE, jump, in1, in3 */
  int res;            /* Result of the comparison of pIn1 against pIn3 */
  char affinity;      /* Affinity to use for comparison */
  u16 flags1;         /* Copy of initial value of pIn1->flags */
  u16 flags3;         /* Copy of initial value of pIn3->flags */

  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pIn1;                 /* 1st input operand */
  Mem *pIn3;                 /* 3rd input operand */
  Mem *pOut;                 /* Output operand */
  u8 encoding = ENC(db);

  pIn1 = &aMem[pOp->p1];
  pIn3 = &aMem[pOp->p3];
  flags1 = pIn1->flags;
  flags3 = pIn3->flags;

  if( (flags1 | flags3)&MEM_Null ){
    /* One or both operands are NULL */
    if( pOp->p5 & SQLITE_NULLEQ ){
      /* If SQLITE_NULLEQ is set (which will only happen if the operator is
      ** OP_Eq or OP_Ne) then take the jump or not depending on whether
      ** or not both operands are null.
      */
      assert( pOp->opcode==OP_Eq || pOp->opcode==OP_Ne );
      assert( (flags1 & MEM_Cleared)==0 );
      assert( (pOp->p5 & SQLITE_JUMPIFNULL)==0 );
      if( (flags1&MEM_Null)!=0
       && (flags3&MEM_Null)!=0
       && (flags3&MEM_Cleared)==0
      ){
        res = 0;  /* Results are equal */
      }else{
        res = 1;  /* Results are not equal */
      }
    }else{
      /* SQLITE_NULLEQ is clear and at least one operand is NULL,
      ** then the result is always NULL.
      ** The jump is taken if the SQLITE_JUMPIFNULL bit is set.
      */
      if( pOp->p5 & SQLITE_STOREP2 ){
        pOut = &aMem[pOp->p2];
        MemSetTypeFlag(pOut, MEM_Null);
        REGISTER_TRACE(pOp->p2, pOut);
      }else{
        VdbeBranchTaken(2,3);
        if( pOp->p5 & SQLITE_JUMPIFNULL ){
          pc = pOp->p2-1;
        }
      }
      // Translated: break;
      return pc;
    }
  }else{
    /* Neither operand is NULL.  Do a comparison. */
    affinity = pOp->p5 & SQLITE_AFF_MASK;
    if( affinity ){
      applyAffinity(pIn1, affinity, encoding);
      applyAffinity(pIn3, affinity, encoding);
      if( db->mallocFailed ) {
        // goto no_mem;
        printf("In impl_OP_Compare(): no_mem.\n");
        assert(0);
      }
    }

    assert( pOp->p4type==P4_COLLSEQ || pOp->p4.pColl==0 );
    ExpandBlob(pIn1);
    ExpandBlob(pIn3);
    res = sqlite3MemCompare(pIn3, pIn1, pOp->p4.pColl);
  }
  switch( pOp->opcode ){
    case OP_Eq:    res = res==0;     break;
    case OP_Ne:    res = res!=0;     break;
    case OP_Lt:    res = res<0;      break;
    case OP_Le:    res = res<=0;     break;
    case OP_Gt:    res = res>0;      break;
    default:       res = res>=0;     break;
  }

  if( pOp->p5 & SQLITE_STOREP2 ){
    pOut = &aMem[pOp->p2];
    memAboutToChange(p, pOut);
    MemSetTypeFlag(pOut, MEM_Int);
    pOut->u.i = res;
    REGISTER_TRACE(pOp->p2, pOut);
  }else{
    VdbeBranchTaken(res!=0, (pOp->p5 & SQLITE_NULLEQ)?2:3);
    if( res ){
      pc = pOp->p2-1;
    }
  }
  /* Undo any changes made by applyAffinity() to the input registers. */
  pIn1->flags = (pIn1->flags&~MEM_TypeMask) | (flags1&MEM_TypeMask);
  pIn3->flags = (pIn3->flags&~MEM_TypeMask) | (flags3&MEM_TypeMask);
  // Translated: break;
  return pc;
}

/* Opcode: Integer P1 P2 * * *
** Synopsis: r[P2]=P1
**
** The 32-bit integer value P1 is written into register P2.
*/
void impl_OP_Integer(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_Integer: {         /* out2-prerelease */
  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pOut;                 /* Output operand */
  pOut = &aMem[pOp->p2];
  pOut->u.i = pOp->p1;
  pOut->flags = MEM_Int;
  // break;
}

/* Opcode: Null P1 P2 P3 * *
** Synopsis:  r[P2..P3]=NULL
**
** Write a NULL into registers P2.  If P3 greater than P2, then also write
** NULL into register P3 and every register in between P2 and P3.  If P3
** is less than P2 (typically P3 is zero) then only register P2 is
** set to NULL.
**
** If the P1 value is non-zero, then also set the MEM_Cleared flag so that
** NULL values will not compare equal even if SQLITE_NULLEQ is set on
** OP_Ne or OP_Eq.
*/
void impl_OP_Null(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_Null: {           /* out2-prerelease */
  int cnt;
  u16 nullFlag;
  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pOut;                 /* Output operand */

  cnt = pOp->p3-pOp->p2;
  assert( pOp->p3<=(p->nMem-p->nCursor) );
  pOut = &aMem[pOp->p2];
  pOut->flags = nullFlag = pOp->p1 ? (MEM_Null|MEM_Cleared) : MEM_Null;
  while( cnt>0 ){
    pOut++;
    memAboutToChange(p, pOut);
    VdbeMemRelease(pOut);
    pOut->flags = nullFlag;
    cnt--;
  }
  // break;
}

/* Opcode: AggStep * P2 P3 P4 P5
** Synopsis: accum=r[P3] step(r[P2@P5])
**
** Execute the step function for an aggregate.  The
** function has P5 arguments.   P4 is a pointer to the FuncDef
** structure that specifies the function.  Use register
** P3 as the accumulator.
**
** The P5 arguments are taken from register P2 and its
** successors.
*/
int impl_OP_AggStep(Vdbe *p, sqlite3 *db, int rcIn, Op *pOp) {
// case OP_AggStep: {
  int n;
  int i;
  Mem *pMem;
  Mem *pRec;
  sqlite3_context ctx;
  sqlite3_value **apVal;
  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  int rc = rcIn;

  n = pOp->p5;
  assert( n>=0 );
  pRec = &aMem[pOp->p2];
  apVal = p->apArg;
  assert( apVal || n==0 );
  for(i=0; i<n; i++, pRec++){
    assert( memIsValid(pRec) );
    apVal[i] = pRec;
    memAboutToChange(p, pRec);
  }
  ctx.pFunc = pOp->p4.pFunc;
  assert( pOp->p3>0 && pOp->p3<=(p->nMem-p->nCursor) );
  ctx.pMem = pMem = &aMem[pOp->p3];
  pMem->n++;
  ctx.s.flags = MEM_Null;
  ctx.s.z = 0;
  ctx.s.zMalloc = 0;
  ctx.s.xDel = 0;
  ctx.s.db = db;
  ctx.isError = 0;
  ctx.pColl = 0;
  ctx.skipFlag = 0;
  if( ctx.pFunc->funcFlags & SQLITE_FUNC_NEEDCOLL ){
    assert( pOp>p->aOp );
    assert( pOp[-1].p4type==P4_COLLSEQ );
    assert( pOp[-1].opcode==OP_CollSeq );
    ctx.pColl = pOp[-1].p4.pColl;
  }
  (ctx.pFunc->xStep)(&ctx, n, apVal); /* IMP: R-24505-23230 */
  if( ctx.isError ){
    sqlite3SetString(&p->zErrMsg, db, "%s", sqlite3_value_text(&ctx.s));
    rc = ctx.isError;
  }
  if( ctx.skipFlag ){
    assert( pOp[-1].opcode==OP_CollSeq );
    i = pOp[-1].p1;
    if( i ) sqlite3VdbeMemSetInt64(&aMem[i], 1);
  }

  sqlite3VdbeMemRelease(&ctx.s);

  // break;
  return rc;
}

/* Opcode: AggFinal P1 P2 * P4 *
** Synopsis: accum=r[P1] N=P2
**
** Execute the finalizer function for an aggregate.  P1 is
** the memory location that is the accumulator for the aggregate.
**
** P2 is the number of arguments that the step function takes and
** P4 is a pointer to the FuncDef for this function.  The P2
** argument is not used by this opcode.  It is only there to disambiguate
** functions that can take varying numbers of arguments.  The
** P4 argument is only needed for the degenerate case where
** the step function was not previously called.
*/
void impl_OP_AggFinal(Vdbe *p, sqlite3 *db, int rc, Op *pOp) {
// case OP_AggFinal: {
  Mem *pMem;
  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  u8 encoding = ENC(db);     /* The database encoding */

  assert( pOp->p1>0 && pOp->p1<=(p->nMem-p->nCursor) );
  pMem = &aMem[pOp->p1];
  assert( (pMem->flags & ~(MEM_Null|MEM_Agg))==0 );
  rc = sqlite3VdbeMemFinalize(pMem, pOp->p4.pFunc);
  if( rc ){
    sqlite3SetString(&p->zErrMsg, db, "%s", sqlite3_value_text(pMem));
  }
  sqlite3VdbeChangeEncoding(pMem, encoding);
  UPDATE_MAX_BLOBSIZE(pMem);
  if( sqlite3VdbeMemTooBig(pMem) ){
    // goto too_big;
    printf("In impl_OP_AggFinal(): too_big.\n");
    assert(0);

  }
  // break;
}

/* Opcode: Copy P1 P2 P3 * *
** Synopsis: r[P2@P3+1]=r[P1@P3+1]
**
** Make a copy of registers P1..P1+P3 into registers P2..P2+P3.
**
** This instruction makes a deep copy of the value.  A duplicate
** is made of any string or blob constant.  See also OP_SCopy.
*/
void impl_OP_Copy(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_Copy: {
  int n;
  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pIn1;                 /* 1st input operand */
  Mem *pOut;                 /* Output operand */

  n = pOp->p3;
  pIn1 = &aMem[pOp->p1];
  pOut = &aMem[pOp->p2];
  assert( pOut!=pIn1 );
  while( 1 ){
    sqlite3VdbeMemShallowCopy(pOut, pIn1, MEM_Ephem);
    // Translated Deephemeralize(pOut);
    if( ((pOut)->flags&MEM_Ephem)!=0 && sqlite3VdbeMemMakeWriteable(pOut) ) {
      // goto no_mem;
      printf("In impl_OP_Copy(): no_mem.\n");
      assert(0);
    }

#ifdef SQLITE_DEBUG
    pOut->pScopyFrom = 0;
#endif
    REGISTER_TRACE(pOp->p2+pOp->p3-n, pOut);
    if( (n--)==0 ) break;
    pOut++;
    pIn1++;
  }
  // break;
}

/* Opcode: MustBeInt P1 P2 * * *
** 
** Force the value in register P1 to be an integer.  If the value
** in P1 is not an integer and cannot be converted into an integer
** without data loss, then jump immediately to P2, or if P2==0
** raise an SQLITE_MISMATCH exception.
*/
int impl_OP_MustBeInt(Vdbe *p, sqlite3 *db, int pcIn, Op *pOp) {
// case OP_MustBeInt: {            /* jump, in1 */
  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pIn1;                 /* 1st input operand */
  u8 encoding = ENC(db);     /* The database encoding */
  int rc;
  int pc = pcIn;

  pIn1 = &aMem[pOp->p1];
  if( (pIn1->flags & MEM_Int)==0 ){
    applyAffinity(pIn1, SQLITE_AFF_NUMERIC, encoding);
    VdbeBranchTaken((pIn1->flags&MEM_Int)==0, 2);
    if( (pIn1->flags & MEM_Int)==0 ){
      if( pOp->p2==0 ){
        rc = SQLITE_MISMATCH;
        /* DK: Once the below goto is properly handled,
               perhaps will need to return rc as well. */
        // goto abort_due_to_error;
        printf("In impl_OP_MustBeInt(): abort_due_to_error.\n");
        assert(0);
      }else{
        pc = pOp->p2 - 1;
        // break;
        return pc;
      }
    }
  }
  MemSetTypeFlag(pIn1, MEM_Int);
  // break;
  return pc;
}

/* Opcode: NotExists P1 P2 P3 * *
** Synopsis: intkey=r[P3]
**
** P1 is the index of a cursor open on an SQL table btree (with integer
** keys).  P3 is an integer rowid.  If P1 does not contain a record with
** rowid P3 then jump immediately to P2.  If P1 does contain a record
** with rowid P3 then leave the cursor pointing at that record and fall
** through to the next instruction.
**
** The OP_NotFound opcode performs the same operation on index btrees
** (with arbitrary multi-value keys).
**
** See also: Found, NotFound, NoConflict
*/
int impl_OP_NotExists(Vdbe *p, sqlite3 *db, int *pc, Op *pOp) {
// case OP_NotExists: {        /* jump, in3 */
  VdbeCursor *pC;
  BtCursor *pCrsr;
  int res;
  u64 iKey;

  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pIn3;                 /* 3rd input operand */
  int rc;

  pIn3 = &aMem[pOp->p3];
  assert( pIn3->flags & MEM_Int );
  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  assert( pC->isTable );
  assert( pC->pseudoTableReg==0 );
  pCrsr = pC->pCursor;
  assert( pCrsr!=0 );
  res = 0;
  iKey = pIn3->u.i;
  rc = sqlite3BtreeMovetoUnpacked(pCrsr, 0, iKey, 0, &res);
  pC->lastRowid = pIn3->u.i;
  pC->rowidIsValid = res==0 ?1:0;
  pC->nullRow = 0;
  pC->cacheStatus = CACHE_STALE;
  pC->deferredMoveto = 0;
  VdbeBranchTaken(res!=0,2);
  if( res!=0 ){
    *pc = pOp->p2 - 1;
    assert( pC->rowidIsValid==0 );
  }
  pC->seekResult = res;
  // break;
  return rc;
}

/* Opcode: String P1 P2 * P4 *
** Synopsis: r[P2]='P4' (len=P1)
**
** The string value P4 of length P1 (bytes) is stored in register P2.
*/
void impl_OP_String(Vdbe *p, sqlite3 *db, int rc, Op *pOp) {
// case OP_String: {          /* out2-prerelease */
  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pOut;                 /* Output operand */
  u8 encoding = ENC(db);     /* The database encoding */

  pOut = &aMem[pOp->p2];

  assert( pOp->p4.z!=0 );
  pOut->flags = MEM_Str|MEM_Static|MEM_Term;
  pOut->z = pOp->p4.z;
  pOut->n = pOp->p1;
  pOut->enc = encoding;
  UPDATE_MAX_BLOBSIZE(pOut);
  // break;
}

/* Opcode: String8 * P2 * P4 *
** Synopsis: r[P2]='P4'
**
** P4 points to a nul terminated UTF-8 string. This opcode is transformed 
** into an OP_String before it is executed for the first time.  During
** this transformation, the length of string P4 is computed and stored
** as the P1 parameter.
*/
int impl_OP_String8(Vdbe *p, sqlite3 *db, int rcIn, Op *pOp) {
// case OP_String8: {         /* same as TK_STRING, out2-prerelease */
  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pOut;                 /* Output operand */
  u8 encoding = ENC(db);     /* The database encoding */
  int rc = rcIn;
  pOut = &aMem[pOp->p2];

  assert( pOp->p4.z!=0 );
  pOp->opcode = OP_String;
  pOp->p1 = sqlite3Strlen30(pOp->p4.z);

#ifndef SQLITE_OMIT_UTF16
  if( encoding!=SQLITE_UTF8 ){
    rc = sqlite3VdbeMemSetStr(pOut, pOp->p4.z, -1, SQLITE_UTF8, SQLITE_STATIC);
    if( rc==SQLITE_TOOBIG ) {
      // goto too_big;
      printf("In impl_OP_String8():1: too_big.\n");
      assert(0);
    }
    if( SQLITE_OK!=sqlite3VdbeChangeEncoding(pOut, encoding) ) {
      // goto no_mem;
      printf("In impl_OP_String8(): no_mem.\n");
      assert(0);
    }
    assert( pOut->zMalloc==pOut->z );
    assert( VdbeMemDynamic(pOut)==0 );
    pOut->zMalloc = 0;
    pOut->flags |= MEM_Static;
    if( pOp->p4type==P4_DYNAMIC ){
      sqlite3DbFree(db, pOp->p4.z);
    }
    pOp->p4type = P4_DYNAMIC;
    pOp->p4.z = pOut->z;
    pOp->p1 = pOut->n;
  }
#endif
  if( pOp->p1>db->aLimit[SQLITE_LIMIT_LENGTH] ){
    // goto too_big;
    printf("In impl_OP_String8():2: too_big.\n");
    assert(0);
  }
  /* Fall through to the next case, OP_String */
  impl_OP_String(p, db, rc, pOp);

  return rc;
}

/* Opcode: Function P1 P2 P3 P4 P5
** Synopsis: r[P3]=func(r[P2@P5])
**
** Invoke a user function (P4 is a pointer to a Function structure that
** defines the function) with P5 arguments taken from register P2 and
** successors.  The result of the function is stored in register P3.
** Register P3 must not be one of the function inputs.
**
** P1 is a 32-bit bitmask indicating whether or not each argument to the 
** function was determined to be constant at compile time. If the first
** argument was constant then bit 0 of P1 is set. This is used to determine
** whether meta data associated with a user function argument using the
** sqlite3_set_auxdata() API may be safely retained until the next
** invocation of this opcode.
**
** See also: AggStep and AggFinal
*/
int impl_OP_Function(Vdbe *p, sqlite3 *db, int pc, int rc, Op *pOp) {
// case OP_Function: {
  int i;
  Mem *pArg;
  sqlite3_context ctx;
  sqlite3_value **apVal;
  int n;

  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pOut;                 /* Output operand */
  u8 encoding = ENC(db);     /* The database encoding */

  n = pOp->p5;
  apVal = p->apArg;
  assert( apVal || n==0 );
  assert( pOp->p3>0 && pOp->p3<=(p->nMem-p->nCursor) );
  pOut = &aMem[pOp->p3];
  memAboutToChange(p, pOut);

  assert( n==0 || (pOp->p2>0 && pOp->p2+n<=(p->nMem-p->nCursor)+1) );
  assert( pOp->p3<pOp->p2 || pOp->p3>=pOp->p2+n );
  pArg = &aMem[pOp->p2];
  for(i=0; i<n; i++, pArg++){
    assert( memIsValid(pArg) );
    apVal[i] = pArg;

    // Translated Deephemeralize(pArg);
    if( ((pArg)->flags&MEM_Ephem)!=0 && sqlite3VdbeMemMakeWriteable(pArg) ) {
      // goto no_mem;
      printf("In impl_OP_Function(): no_mem.\n");
      assert(0);
    }

    REGISTER_TRACE(pOp->p2+i, pArg);
  }

  assert( pOp->p4type==P4_FUNCDEF );
  ctx.pFunc = pOp->p4.pFunc;
  ctx.iOp = pc;
  ctx.pVdbe = p;

  /* The output cell may already have a buffer allocated. Move
  ** the pointer to ctx.s so in case the user-function can use
  ** the already allocated buffer instead of allocating a new one.
  */
  memcpy(&ctx.s, pOut, sizeof(Mem));
  pOut->flags = MEM_Null;
  pOut->xDel = 0;
  pOut->zMalloc = 0;
  MemSetTypeFlag(&ctx.s, MEM_Null);

  ctx.fErrorOrAux = 0;
  if( ctx.pFunc->funcFlags & SQLITE_FUNC_NEEDCOLL ){
    assert( pOp>aOp );
    assert( pOp[-1].p4type==P4_COLLSEQ );
    assert( pOp[-1].opcode==OP_CollSeq );
    ctx.pColl = pOp[-1].p4.pColl;
  }
  db->lastRowid = lastRowid;
  (*ctx.pFunc->xFunc)(&ctx, n, apVal); /* IMP: R-24505-23230 */
  lastRowid = db->lastRowid;

  if( db->mallocFailed ){
    /* Even though a malloc() has failed, the implementation of the
    ** user function may have called an sqlite3_result_XXX() function
    ** to return a value. The following call releases any resources
    ** associated with such a value.
    */
    sqlite3VdbeMemRelease(&ctx.s);
    // goto no_mem;
    printf("In impl_OP_Function(): no_mem.\n");
    assert(0);
  }

  /* If the function returned an error, throw an exception */
  if( ctx.fErrorOrAux ){
    if( ctx.isError ){
      sqlite3SetString(&p->zErrMsg, db, "%s", sqlite3_value_text(&ctx.s));
      rc = ctx.isError;
    }
    sqlite3VdbeDeleteAuxData(p, pc, pOp->p1);
  }

  /* Copy the result of the function into register P3 */
  sqlite3VdbeChangeEncoding(&ctx.s, encoding);
  assert( pOut->flags==MEM_Null );
  memcpy(pOut, &ctx.s, sizeof(Mem));
  if( sqlite3VdbeMemTooBig(pOut) ){
    // goto too_big;
    printf("In impl_OP_Function(): too_big.\n");
    assert(0);
  }

#if 0
  /* The app-defined function has done something that as caused this
  ** statement to expire.  (Perhaps the function called sqlite3_exec()
  ** with a CREATE TABLE statement.)
  */
  if( p->expired ) rc = SQLITE_ABORT;
#endif

  REGISTER_TRACE(pOp->p3, pOut);
  UPDATE_MAX_BLOBSIZE(pOut);
  // break;
  return rc;
}

/* Opcode: Real * P2 * P4 *
** Synopsis: r[P2]=P4
**
** P4 is a pointer to a 64-bit floating point value.
** Write that value into register P2.
*/
void impl_OP_Real(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_Real: {            /* same as TK_FLOAT, out2-prerelease */
  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pOut;                 /* Output operand */

  pOut = &aMem[pOp->p2];
  pOut->flags = MEM_Real;
  assert( !sqlite3IsNaN(*pOp->p4.pReal) );
  pOut->r = *pOp->p4.pReal;
  // break;
}

/* Opcode: RealAffinity P1 * * * *
**
** If register P1 holds an integer convert it to a real value.
**
** This opcode is used when extracting information from a column that
** has REAL affinity.  Such column values may still be stored as
** integers, for space efficiency, but after extraction we want them
** to have only a real value.
*/
void impl_OP_RealAffinity(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_RealAffinity: {                  /* in1 */
  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pIn1;                 /* 1st input operand */

  pIn1 = &aMem[pOp->p1];
  if( pIn1->flags & MEM_Int ){
    sqlite3VdbeMemRealify(pIn1);
  }
  // break;
}

/* Opcode: Add P1 P2 P3 * *
** Synopsis:  r[P3]=r[P1]+r[P2]
**
** Add the value in register P1 to the value in register P2
** and store the result in register P3.
** If either input is NULL, the result is NULL.
*/
/* Opcode: Multiply P1 P2 P3 * *
** Synopsis:  r[P3]=r[P1]*r[P2]
**
**
** Multiply the value in register P1 by the value in register P2
** and store the result in register P3.
** If either input is NULL, the result is NULL.
*/
/* Opcode: Subtract P1 P2 P3 * *
** Synopsis:  r[P3]=r[P2]-r[P1]
**
** Subtract the value in register P1 from the value in register P2
** and store the result in register P3.
** If either input is NULL, the result is NULL.
*/
/* Opcode: Divide P1 P2 P3 * *
** Synopsis:  r[P3]=r[P2]/r[P1]
**
** Divide the value in register P1 by the value in register P2
** and store the result in register P3 (P3=P2/P1). If the value in 
** register P1 is zero, then the result is NULL. If either input is 
** NULL, the result is NULL.
*/
/* Opcode: Remainder P1 P2 P3 * *
** Synopsis:  r[P3]=r[P2]%r[P1]
**
** Compute the remainder after integer register P2 is divided by 
** register P1 and store the result in register P3. 
** If the value in register P1 is zero the result is NULL.
** If either operand is NULL, the result is NULL.
*/
void impl_OP_Add_Subtract_Multiply_Divide_Remainder(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_Add:                   /* same as TK_PLUS, in1, in2, out3 */
// case OP_Subtract:              /* same as TK_MINUS, in1, in2, out3 */
// case OP_Multiply:              /* same as TK_STAR, in1, in2, out3 */
// case OP_Divide:                /* same as TK_SLASH, in1, in2, out3 */
// case OP_Remainder: {           /* same as TK_REM, in1, in2, out3 */
  char bIntint;   /* Started out as two integer operands */
  u16 flags;      /* Combined MEM_* flags from both inputs */
  u16 type1;      /* Numeric type of left operand */
  u16 type2;      /* Numeric type of right operand */
  i64 iA;         /* Integer value of left operand */
  i64 iB;         /* Integer value of right operand */
  double rA;      /* Real value of left operand */
  double rB;      /* Real value of right operand */

  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pIn1;                 /* 1st input operand */
  Mem *pIn2;
  Mem *pOut;                 /* Output operand */

  pIn1 = &aMem[pOp->p1];
  type1 = numericType(pIn1);
  pIn2 = &aMem[pOp->p2];
  type2 = numericType(pIn2);
  pOut = &aMem[pOp->p3];
  flags = pIn1->flags | pIn2->flags;
  if( (flags & MEM_Null)!=0 ) goto arithmetic_result_is_null;
  if( (type1 & type2 & MEM_Int)!=0 ){
    iA = pIn1->u.i;
    iB = pIn2->u.i;
    bIntint = 1;
    switch( pOp->opcode ){
      case OP_Add:       if( sqlite3AddInt64(&iB,iA) ) goto fp_math;  break;
      case OP_Subtract:  if( sqlite3SubInt64(&iB,iA) ) goto fp_math;  break;
      case OP_Multiply:  if( sqlite3MulInt64(&iB,iA) ) goto fp_math;  break;
      case OP_Divide: {
        if( iA==0 ) goto arithmetic_result_is_null;
        if( iA==-1 && iB==SMALLEST_INT64 ) goto fp_math;
        iB /= iA;
        break;
      }
      default: {
        if( iA==0 ) goto arithmetic_result_is_null;
        if( iA==-1 ) iA = 1;
        iB %= iA;
        break;
      }
    }
    pOut->u.i = iB;
    MemSetTypeFlag(pOut, MEM_Int);
  }else{
    bIntint = 0;
fp_math:
    rA = sqlite3VdbeRealValue(pIn1);
    rB = sqlite3VdbeRealValue(pIn2);
    switch( pOp->opcode ){
      case OP_Add:         rB += rA;       break;
      case OP_Subtract:    rB -= rA;       break;
      case OP_Multiply:    rB *= rA;       break;
      case OP_Divide: {
        /* (double)0 In case of SQLITE_OMIT_FLOATING_POINT... */
        if( rA==(double)0 ) goto arithmetic_result_is_null;
        rB /= rA;
        break;
      }
      default: {
        iA = (i64)rA;
        iB = (i64)rB;
        if( iA==0 ) goto arithmetic_result_is_null;
        if( iA==-1 ) iA = 1;
        rB = (double)(iB % iA);
        break;
      }
    }
#ifdef SQLITE_OMIT_FLOATING_POINT
    pOut->u.i = rB;
    MemSetTypeFlag(pOut, MEM_Int);
#else
    if( sqlite3IsNaN(rB) ){
      goto arithmetic_result_is_null;
    }
    pOut->r = rB;
    MemSetTypeFlag(pOut, MEM_Real);
    if( ((type1|type2)&MEM_Real)==0 && !bIntint ){
      sqlite3VdbeIntegerAffinity(pOut);
    }
#endif
  }
  return;
  // break;

arithmetic_result_is_null:
  sqlite3VdbeMemSetNull(pOut);
  // break;
  return;
}

/* Opcode: If P1 P2 P3 * *
**
** Jump to P2 if the value in register P1 is true.  The value
** is considered true if it is numeric and non-zero.  If the value
** in P1 is NULL then take the jump if P3 is non-zero.
*/
/* Opcode: IfNot P1 P2 P3 * *
**
** Jump to P2 if the value in register P1 is False.  The value
** is considered false if it has a numeric value of zero.  If the value
** in P1 is NULL then take the jump if P3 is zero.
*/
int impl_OP_If_IfNot(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_If:                 /* jump, in1 */
// case OP_IfNot: {            /* jump, in1 */
  int c;

  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pIn1;                 /* 1st input operand */

  pIn1 = &aMem[pOp->p1];
  if( pIn1->flags & MEM_Null ){
    c = pOp->p3;
  }else{
#ifdef SQLITE_OMIT_FLOATING_POINT
    c = sqlite3VdbeIntValue(pIn1)!=0;
#else
    c = sqlite3VdbeRealValue(pIn1)!=0.0;
#endif
    if( pOp->opcode==OP_IfNot ) c = !c;
  }
  VdbeBranchTaken(c!=0, 2);
  if( c ){
    pc = pOp->p2-1;
  }
  // break;
  return pc;
}

/* Opcode: Rowid P1 P2 * * *
** Synopsis: r[P2]=rowid
**
** Store in register P2 an integer which is the key of the table entry that
** P1 is currently point to.
**
** P1 can be either an ordinary table or a virtual table.  There used to
** be a separate OP_VRowid opcode for use with virtual tables, but this
** one opcode now works for both table types.
*/
int impl_OP_Rowid(Vdbe *p, sqlite3 *db, int rc, Op *pOp) {
// case OP_Rowid: {                 /* out2-prerelease */
  VdbeCursor *pC;
  i64 v;
  sqlite3_vtab *pVtab;
  const sqlite3_module *pModule;

  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pOut;                 /* Output operand */
  pOut = &aMem[pOp->p2];

  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  assert( pC->pseudoTableReg==0 || pC->nullRow );
  if( pC->nullRow ){
    pOut->flags = MEM_Null;
    // break;
    return rc;
  }else if( pC->deferredMoveto ){
    v = pC->movetoTarget;
#ifndef SQLITE_OMIT_VIRTUALTABLE
  }else if( pC->pVtabCursor ){
    pVtab = pC->pVtabCursor->pVtab;
    pModule = pVtab->pModule;
    assert( pModule->xRowid );
    rc = pModule->xRowid(pC->pVtabCursor, &v);
    sqlite3VtabImportErrmsg(p, pVtab);
#endif /* SQLITE_OMIT_VIRTUALTABLE */
  }else{
    assert( pC->pCursor!=0 );
    rc = sqlite3VdbeCursorMoveto(pC);
    if( rc ) {
      // goto abort_due_to_error;
      printf("In impl_OP_Rowid(): abort_due_to_error.\n");
      assert(0);
    }
    if( pC->rowidIsValid ){
      v = pC->lastRowid;
    }else{
      rc = sqlite3BtreeKeySize(pC->pCursor, &v);
      assert( rc==SQLITE_OK );  /* Always so because of CursorMoveto() above */
    }
  }
  pOut->u.i = v;

  // break;
  return rc;
}

/* Opcode: IsNull P1 P2 * * *
** Synopsis:  if r[P1]==NULL goto P2
**
** Jump to P2 if the value in register P1 is NULL.
*/
int impl_OP_IsNull(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_IsNull: {            /* same as TK_ISNULL, jump, in1 */
  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pIn1;                 /* 1st input operand */

  pIn1 = &aMem[pOp->p1];
  VdbeBranchTaken( (pIn1->flags & MEM_Null)!=0, 2);
  if( (pIn1->flags & MEM_Null)!=0 ){
    pc = pOp->p2 - 1;
  }
  // break;
  return pc;
}

/* Opcode: SeekGe P1 P2 P3 P4 *
** Synopsis: key=r[P3@P4]
**
** If cursor P1 refers to an SQL table (B-Tree that uses integer keys), 
** use the value in register P3 as the key.  If cursor P1 refers 
** to an SQL index, then P3 is the first in an array of P4 registers 
** that are used as an unpacked index key. 
**
** Reposition cursor P1 so that  it points to the smallest entry that 
** is greater than or equal to the key value. If there are no records 
** greater than or equal to the key and P2 is not zero, then jump to P2.
**
** See also: Found, NotFound, Distinct, SeekLt, SeekGt, SeekLe
*/
/* Opcode: SeekGt P1 P2 P3 P4 *
** Synopsis: key=r[P3@P4]
**
** If cursor P1 refers to an SQL table (B-Tree that uses integer keys), 
** use the value in register P3 as a key. If cursor P1 refers 
** to an SQL index, then P3 is the first in an array of P4 registers 
** that are used as an unpacked index key. 
**
** Reposition cursor P1 so that  it points to the smallest entry that 
** is greater than the key value. If there are no records greater than 
** the key and P2 is not zero, then jump to P2.
**
** See also: Found, NotFound, Distinct, SeekLt, SeekGe, SeekLe
*/
/* Opcode: SeekLt P1 P2 P3 P4 * 
** Synopsis: key=r[P3@P4]
**
** If cursor P1 refers to an SQL table (B-Tree that uses integer keys), 
** use the value in register P3 as a key. If cursor P1 refers 
** to an SQL index, then P3 is the first in an array of P4 registers 
** that are used as an unpacked index key. 
**
** Reposition cursor P1 so that  it points to the largest entry that 
** is less than the key value. If there are no records less than 
** the key and P2 is not zero, then jump to P2.
**
** See also: Found, NotFound, Distinct, SeekGt, SeekGe, SeekLe
*/
/* Opcode: SeekLe P1 P2 P3 P4 *
** Synopsis: key=r[P3@P4]
**
** If cursor P1 refers to an SQL table (B-Tree that uses integer keys), 
** use the value in register P3 as a key. If cursor P1 refers 
** to an SQL index, then P3 is the first in an array of P4 registers 
** that are used as an unpacked index key. 
**
** Reposition cursor P1 so that it points to the largest entry that 
** is less than or equal to the key value. If there are no records 
** less than or equal to the key and P2 is not zero, then jump to P2.
**
** See also: Found, NotFound, Distinct, SeekGt, SeekGe, SeekLt
*/
int impl_OP_SeekLT_SeekLE_SeekGE_SeekGT(Vdbe *p, sqlite3 *db, int *pc, int rc, Op *pOp) {
// case OP_SeekLT:         /* jump, in3 */
// case OP_SeekLE:         /* jump, in3 */
// case OP_SeekGE:         /* jump, in3 */
// case OP_SeekGT: {       /* jump, in3 */
  int res;
  int oc;
  VdbeCursor *pC;
  UnpackedRecord r;
  int nField;
  i64 iKey;      /* The rowid we are to seek to */

  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pIn3;                 /* 3rd input operand */

  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  assert( pOp->p2!=0 );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  assert( pC->pseudoTableReg==0 );
  assert( OP_SeekLE == OP_SeekLT+1 );
  assert( OP_SeekGE == OP_SeekLT+2 );
  assert( OP_SeekGT == OP_SeekLT+3 );
  assert( pC->isOrdered );
  assert( pC->pCursor!=0 );
  oc = pOp->opcode;
  pC->nullRow = 0;
  if( pC->isTable ){
    /* The input value in P3 might be of any type: integer, real, string,
    ** blob, or NULL.  But it needs to be an integer before we can do
    ** the seek, so covert it. */
    pIn3 = &aMem[pOp->p3];
    applyNumericAffinity(pIn3);
    iKey = sqlite3VdbeIntValue(pIn3);
    pC->rowidIsValid = 0;

    /* If the P3 value could not be converted into an integer without
    ** loss of information, then special processing is required... */
    if( (pIn3->flags & MEM_Int)==0 ){
      if( (pIn3->flags & MEM_Real)==0 ){
        /* If the P3 value cannot be converted into any kind of a number,
        ** then the seek is not possible, so jump to P2 */
        *pc = pOp->p2 - 1;  VdbeBranchTaken(1,2);
        // break;
        return rc;
      }

      /* If the approximation iKey is larger than the actual real search
      ** term, substitute >= for > and < for <=. e.g. if the search term
      ** is 4.9 and the integer approximation 5:
      **
      **        (x >  4.9)    ->     (x >= 5)
      **        (x <= 4.9)    ->     (x <  5)
      */
      if( pIn3->r<(double)iKey ){
        assert( OP_SeekGE==(OP_SeekGT-1) );
        assert( OP_SeekLT==(OP_SeekLE-1) );
        assert( (OP_SeekLE & 0x0001)==(OP_SeekGT & 0x0001) );
        if( (oc & 0x0001)==(OP_SeekGT & 0x0001) ) oc--;
      }

      /* If the approximation iKey is smaller than the actual real search
      ** term, substitute <= for < and > for >=.  */
      else if( pIn3->r>(double)iKey ){
        assert( OP_SeekLE==(OP_SeekLT+1) );
        assert( OP_SeekGT==(OP_SeekGE+1) );
        assert( (OP_SeekLT & 0x0001)==(OP_SeekGE & 0x0001) );
        if( (oc & 0x0001)==(OP_SeekLT & 0x0001) ) oc++;
      }
    } 
    rc = sqlite3BtreeMovetoUnpacked(pC->pCursor, 0, (u64)iKey, 0, &res);
    if( rc!=SQLITE_OK ){
      // goto abort_due_to_error;
      printf("In impl_OP_SeekLT_SeekLE_SeekGE_SeekGT():1: abort_due_to_error.\n");
      assert(0);
    }
    if( res==0 ){
      pC->rowidIsValid = 1;
      pC->lastRowid = iKey;
    }
  }else{
    nField = pOp->p4.i;
    assert( pOp->p4type==P4_INT32 );
    assert( nField>0 );
    r.pKeyInfo = pC->pKeyInfo;
    r.nField = (u16)nField;

    /* The next line of code computes as follows, only faster:
    **   if( oc==OP_SeekGT || oc==OP_SeekLE ){
    **     r.default_rc = -1;
    **   }else{
    **     r.default_rc = +1;
    **   }
    */
    r.default_rc = ((1 & (oc - OP_SeekLT)) ? -1 : +1);
    assert( oc!=OP_SeekGT || r.default_rc==-1 );
    assert( oc!=OP_SeekLE || r.default_rc==-1 );
    assert( oc!=OP_SeekGE || r.default_rc==+1 );
    assert( oc!=OP_SeekLT || r.default_rc==+1 );

    r.aMem = &aMem[pOp->p3];
#ifdef SQLITE_DEBUG
    { int i; for(i=0; i<r.nField; i++) assert( memIsValid(&r.aMem[i]) ); }
#endif
    ExpandBlob(r.aMem);
    rc = sqlite3BtreeMovetoUnpacked(pC->pCursor, &r, 0, 0, &res);
    if( rc!=SQLITE_OK ){
      // goto abort_due_to_error;
      printf("In impl_OP_SeekLT_SeekLE_SeekGE_SeekGT():2: abort_due_to_error.\n");
      assert(0);
    }
    pC->rowidIsValid = 0;
  }
  pC->deferredMoveto = 0;
  pC->cacheStatus = CACHE_STALE;
#ifdef SQLITE_TEST
  sqlite3_search_count++;
#endif
  if( oc>=OP_SeekGE ){  assert( oc==OP_SeekGE || oc==OP_SeekGT );
    if( res<0 || (res==0 && oc==OP_SeekGT) ){
      res = 0;
      rc = sqlite3BtreeNext(pC->pCursor, &res);
      if( rc!=SQLITE_OK ) {
        // goto abort_due_to_error;
        printf("In impl_OP_SeekLT_SeekLE_SeekGE_SeekGT():3: abort_due_to_error.\n");
        assert(0);        
      }
      pC->rowidIsValid = 0;
    }else{
      res = 0;
    }
  }else{
    assert( oc==OP_SeekLT || oc==OP_SeekLE );
    if( res>0 || (res==0 && oc==OP_SeekLT) ){
      res = 0;
      rc = sqlite3BtreePrevious(pC->pCursor, &res);
      if( rc!=SQLITE_OK ) {
        // goto abort_due_to_error;
        printf("In impl_OP_SeekLT_SeekLE_SeekGE_SeekGT():4: abort_due_to_error.\n");
        assert(0);        
      }
      pC->rowidIsValid = 0;
    }else{
      /* res might be negative because the table is empty.  Check to
      ** see if this is the case.
      */
      res = sqlite3BtreeEof(pC->pCursor);
    }
  }
  assert( pOp->p2>0 );
  VdbeBranchTaken(res!=0,2);
  if( res ){
    *pc = pOp->p2 - 1;
  }
  // break;
  return rc;
}

/* Opcode: Move P1 P2 P3 * *
** Synopsis:  r[P2@P3]=r[P1@P3]
**
** Move the P3 values in register P1..P1+P3-1 over into
** registers P2..P2+P3-1.  Registers P1..P1+P3-1 are
** left holding a NULL.  It is an error for register ranges
** P1..P1+P3-1 and P2..P2+P3-1 to overlap.  It is an error
** for P3 to be less than 1.
*/
void impl_OP_Move(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_Move: {
  char *zMalloc;   /* Holding variable for allocated memory */
  int n;           /* Number of registers left to copy */
  int p1;          /* Register to copy from */
  int p2;          /* Register to copy to */

  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pIn1;                 /* 1st input operand */
  Mem *pOut;                 /* Output operand */

  n = pOp->p3;
  p1 = pOp->p1;
  p2 = pOp->p2;
  assert( n>0 && p1>0 && p2>0 );
  assert( p1+n<=p2 || p2+n<=p1 );

  pIn1 = &aMem[p1];
  pOut = &aMem[p2];
  do{
    assert( pOut<=&aMem[(p->nMem-p->nCursor)] );
    assert( pIn1<=&aMem[(p->nMem-p->nCursor)] );
    assert( memIsValid(pIn1) );
    memAboutToChange(p, pOut);
    VdbeMemRelease(pOut);
    zMalloc = pOut->zMalloc;
    memcpy(pOut, pIn1, sizeof(Mem));
#ifdef SQLITE_DEBUG
    if( pOut->pScopyFrom>=&aMem[p1] && pOut->pScopyFrom<&aMem[p1+pOp->p3] ){
      pOut->pScopyFrom += p1 - pOp->p2;
    }
#endif
    pIn1->flags = MEM_Undefined;
    pIn1->xDel = 0;
    pIn1->zMalloc = zMalloc;
    REGISTER_TRACE(p2++, pOut);
    pIn1++;
    pOut++;
  }while( --n );
  // break;
}

/* Opcode: IfZero P1 P2 P3 * *
** Synopsis: r[P1]+=P3, if r[P1]==0 goto P2
**
** The register P1 must contain an integer.  Add literal P3 to the
** value in register P1.  If the result is exactly 0, jump to P2. 
**
** It is illegal to use this instruction on a register that does
** not contain an integer.  An assertion fault will result if you try.
*/
int impl_OP_IfZero(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_IfZero: {        /* jump, in1 */
  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pIn1;                 /* 1st input operand */

  pIn1 = &aMem[pOp->p1];
  assert( pIn1->flags&MEM_Int );
  pIn1->u.i += pOp->p3;
  VdbeBranchTaken(pIn1->u.i==0, 2);
  if( pIn1->u.i==0 ){
     pc = pOp->p2 - 1;
  }
  // break;
  return pc;
}

/* Opcode: IdxRowid P1 P2 * * *
** Synopsis: r[P2]=rowid
**
** Write into register P2 an integer which is the last entry in the record at
** the end of the index key pointed to by cursor P1.  This integer should be
** the rowid of the table entry to which this index entry points.
**
** See also: Rowid, MakeRecord.
*/
int impl_OP_IdxRowid(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_IdxRowid: {              /* out2-prerelease */
  BtCursor *pCrsr;
  VdbeCursor *pC;
  i64 rowid;

  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pOut;                 /* Output operand */
  int rc;

  pOut = &aMem[pOp->p2];
  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  pCrsr = pC->pCursor;
  assert( pCrsr!=0 );
  pOut->flags = MEM_Null;
  rc = sqlite3VdbeCursorMoveto(pC);
  if( NEVER(rc) ) {
    // goto abort_due_to_error;
    printf("In impl_OP_IdxRowid():1: abort_due_to_error.\n");
    assert(0);            
  }
  assert( pC->deferredMoveto==0 );
  assert( pC->isTable==0 );
  if( !pC->nullRow ){
    rowid = 0;  /* Not needed.  Only used to silence a warning. */
    rc = sqlite3VdbeIdxRowid(db, pCrsr, &rowid);
    if( rc!=SQLITE_OK ){
      // goto abort_due_to_error;
      printf("In impl_OP_IdxRowid():2: abort_due_to_error.\n");
      assert(0);            
    }
    pOut->u.i = rowid;
    pOut->flags = MEM_Int;
  }
  // break;
  return rc;
}

/* Opcode: IdxGE P1 P2 P3 P4 P5
** Synopsis: key=r[P3@P4]
**
** The P4 register values beginning with P3 form an unpacked index 
** key that omits the PRIMARY KEY.  Compare this key value against the index 
** that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID 
** fields at the end.
**
** If the P1 index entry is greater than or equal to the key value
** then jump to P2.  Otherwise fall through to the next instruction.
*/
/* Opcode: IdxGT P1 P2 P3 P4 P5
** Synopsis: key=r[P3@P4]
**
** The P4 register values beginning with P3 form an unpacked index 
** key that omits the PRIMARY KEY.  Compare this key value against the index 
** that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID 
** fields at the end.
**
** If the P1 index entry is greater than the key value
** then jump to P2.  Otherwise fall through to the next instruction.
*/
/* Opcode: IdxLT P1 P2 P3 P4 P5
** Synopsis: key=r[P3@P4]
**
** The P4 register values beginning with P3 form an unpacked index 
** key that omits the PRIMARY KEY or ROWID.  Compare this key value against
** the index that P1 is currently pointing to, ignoring the PRIMARY KEY or
** ROWID on the P1 index.
**
** If the P1 index entry is less than the key value then jump to P2.
** Otherwise fall through to the next instruction.
*/
/* Opcode: IdxLE P1 P2 P3 P4 P5
** Synopsis: key=r[P3@P4]
**
** The P4 register values beginning with P3 form an unpacked index 
** key that omits the PRIMARY KEY or ROWID.  Compare this key value against
** the index that P1 is currently pointing to, ignoring the PRIMARY KEY or
** ROWID on the P1 index.
**
** If the P1 index entry is less than or equal to the key value then jump
** to P2. Otherwise fall through to the next instruction.
*/
int impl_OP_IdxLE_IdxGT_IdxLT_IdxGE(Vdbe *p, sqlite3 *db, int *pc, Op *pOp) {
// case OP_IdxLE:          /* jump */
// case OP_IdxGT:          /* jump */
// case OP_IdxLT:          /* jump */
// case OP_IdxGE:  {       /* jump */
  VdbeCursor *pC;
  int res;
  UnpackedRecord r;

  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  int rc;

  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  assert( pC->isOrdered );
  assert( pC->pCursor!=0);
  assert( pC->deferredMoveto==0 );
  assert( pOp->p5==0 || pOp->p5==1 );
  assert( pOp->p4type==P4_INT32 );
  r.pKeyInfo = pC->pKeyInfo;
  r.nField = (u16)pOp->p4.i;
  if( pOp->opcode<OP_IdxLT ){
    assert( pOp->opcode==OP_IdxLE || pOp->opcode==OP_IdxGT );
    r.default_rc = -1;
  }else{
    assert( pOp->opcode==OP_IdxGE || pOp->opcode==OP_IdxLT );
    r.default_rc = 0;
  }
  r.aMem = &aMem[pOp->p3];
#ifdef SQLITE_DEBUG
  { int i; for(i=0; i<r.nField; i++) assert( memIsValid(&r.aMem[i]) ); }
#endif
  res = 0;  /* Not needed.  Only used to silence a warning. */
  rc = sqlite3VdbeIdxKeyCompare(pC, &r, &res);
  assert( (OP_IdxLE&1)==(OP_IdxLT&1) && (OP_IdxGE&1)==(OP_IdxGT&1) );
  if( (pOp->opcode&1)==(OP_IdxLT&1) ){
    assert( pOp->opcode==OP_IdxLE || pOp->opcode==OP_IdxLT );
    res = -res;
  }else{
    assert( pOp->opcode==OP_IdxGE || pOp->opcode==OP_IdxGT );
    res++;
  }
  VdbeBranchTaken(res>0,2);
  if( res>0 ){
    *pc = pOp->p2 - 1 ;
  }
  // break;
  return rc;
}

/* Opcode: Seek P1 P2 * * *
** Synopsis:  intkey=r[P2]
**
** P1 is an open table cursor and P2 is a rowid integer.  Arrange
** for P1 to move so that it points to the rowid given by P2.
**
** This is actually a deferred seek.  Nothing actually happens until
** the cursor is used to read a record.  That way, if no reads
** occur, no unnecessary I/O happens.
*/
void impl_OP_Seek(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_Seek: {    /* in2 */
  VdbeCursor *pC;

  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pIn2;

  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  assert( pC->pCursor!=0 );
  assert( pC->isTable );
  pC->nullRow = 0;
  pIn2 = &aMem[pOp->p2];
  pC->movetoTarget = sqlite3VdbeIntValue(pIn2);
  pC->rowidIsValid = 0;
  pC->deferredMoveto = 1;
  // break;
}

/* Opcode: Once P1 P2 * * *
**
** Check if OP_Once flag P1 is set. If so, jump to instruction P2. Otherwise,
** set the flag and fall through to the next instruction.  In other words,
** this opcode causes all following opcodes up through P2 (but not including
** P2) to run just once and to be skipped on subsequent times through the loop.
*/
int impl_OP_Once(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_Once: {             /* jump */
  assert( pOp->p1<p->nOnceFlag );
  VdbeBranchTaken(p->aOnceFlag[pOp->p1]!=0, 2);
  if( p->aOnceFlag[pOp->p1] ){
    pc = pOp->p2-1;
  }else{
    p->aOnceFlag[pOp->p1] = 1;
  }
  // break;
  return pc;
}

/* Opcode: SCopy P1 P2 * * *
** Synopsis: r[P2]=r[P1]
**
** Make a shallow copy of register P1 into register P2.
**
** This instruction makes a shallow copy of the value.  If the value
** is a string or blob, then the copy is only a pointer to the
** original and hence if the original changes so will the copy.
** Worse, if the original is deallocated, the copy becomes invalid.
** Thus the program must guarantee that the original will not change
** during the lifetime of the copy.  Use OP_Copy to make a complete
** copy.
*/
void impl_OP_SCopy(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_SCopy: {            /* out2 */
  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pIn1;                 /* 1st input operand */
  Mem *pOut;                 /* Output operand */

  pIn1 = &aMem[pOp->p1];
  pOut = &aMem[pOp->p2];
  assert( pOut!=pIn1 );
  sqlite3VdbeMemShallowCopy(pOut, pIn1, MEM_Ephem);
#ifdef SQLITE_DEBUG
  if( pOut->pScopyFrom==0 ) pOut->pScopyFrom = pIn1;
#endif
  // break;
}

/* Opcode: Affinity P1 P2 * P4 *
** Synopsis: affinity(r[P1@P2])
**
** Apply affinities to a range of P2 registers starting with P1.
**
** P4 is a string that is P2 characters long. The nth character of the
** string indicates the column affinity that should be used for the nth
** memory cell in the range.
*/
void impl_OP_Affinity(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_Affinity: {
  const char *zAffinity;   /* The affinity to be applied */
  char cAff;               /* A single character of affinity */

  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pIn1;                 /* 1st input operand */
  u8 encoding = ENC(db);     /* The database encoding */

  zAffinity = pOp->p4.z;
  assert( zAffinity!=0 );
  assert( zAffinity[pOp->p2]==0 );
  pIn1 = &aMem[pOp->p1];
  while( (cAff = *(zAffinity++))!=0 ){
    assert( pIn1 <= &p->aMem[(p->nMem-p->nCursor)] );
    assert( memIsValid(pIn1) );
    applyAffinity(pIn1, cAff, encoding);
    pIn1++;
  }
  // break;
}

/* Opcode: OpenEphemeral P1 P2 * P4 P5
** Synopsis: nColumn=P2
**
** Open a new cursor P1 to a transient table.
** The cursor is always opened read/write even if 
** the main database is read-only.  The ephemeral
** table is deleted automatically when the cursor is closed.
**
** P2 is the number of columns in the ephemeral table.
** The cursor points to a BTree table if P4==0 and to a BTree index
** if P4 is not 0.  If P4 is not NULL, it points to a KeyInfo structure
** that defines the format of keys in the index.
**
** The P5 parameter can be a mask of the BTREE_* flags defined
** in btree.h.  These flags control aspects of the operation of
** the btree.  The BTREE_OMIT_JOURNAL and BTREE_SINGLE flags are
** added automatically.
*/
/* Opcode: OpenAutoindex P1 P2 * P4 *
** Synopsis: nColumn=P2
**
** This opcode works the same as OP_OpenEphemeral.  It has a
** different name to distinguish its use.  Tables created using
** by this opcode will be used for automatically created transient
** indices in joins.
*/
int impl_OP_OpenAutoindex_OpenEphemeral(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_OpenAutoindex: 
// case OP_OpenEphemeral: {
  VdbeCursor *pCx;
  KeyInfo *pKeyInfo;
  int rc;

  static const int vfsFlags = 
      SQLITE_OPEN_READWRITE |
      SQLITE_OPEN_CREATE |
      SQLITE_OPEN_EXCLUSIVE |
      SQLITE_OPEN_DELETEONCLOSE |
      SQLITE_OPEN_TRANSIENT_DB;
  assert( pOp->p1>=0 );
  assert( pOp->p2>=0 );
  pCx = allocateCursor(p, pOp->p1, pOp->p2, -1, 1);
  if( pCx==0 ) {
    // goto no_mem;
    printf("In impl_OP_OpenAutoindex_OpenEphemeral(): no_mem.\n");
    assert(0);
  }
  pCx->nullRow = 1;
  pCx->isEphemeral = 1;
  rc = sqlite3BtreeOpen(db->pVfs, 0, db, &pCx->pBt, 
                        BTREE_OMIT_JOURNAL | BTREE_SINGLE | pOp->p5, vfsFlags);
  if( rc==SQLITE_OK ){
    rc = sqlite3BtreeBeginTrans(pCx->pBt, 1);
  }
  if( rc==SQLITE_OK ){
    /* If a transient index is required, create it by calling
    ** sqlite3BtreeCreateTable() with the BTREE_BLOBKEY flag before
    ** opening it. If a transient table is required, just use the
    ** automatically created table with root-page 1 (an BLOB_INTKEY table).
    */
    if( (pKeyInfo = pOp->p4.pKeyInfo)!=0 ){
      int pgno;
      assert( pOp->p4type==P4_KEYINFO );
      rc = sqlite3BtreeCreateTable(pCx->pBt, &pgno, BTREE_BLOBKEY | pOp->p5); 
      if( rc==SQLITE_OK ){
        assert( pgno==MASTER_ROOT+1 );
        assert( pKeyInfo->db==db );
        assert( pKeyInfo->enc==ENC(db) );
        pCx->pKeyInfo = pKeyInfo;
        rc = sqlite3BtreeCursor(pCx->pBt, pgno, 1, pKeyInfo, pCx->pCursor);
      }
      pCx->isTable = 0;
    }else{
      rc = sqlite3BtreeCursor(pCx->pBt, MASTER_ROOT, 1, 0, pCx->pCursor);
      pCx->isTable = 1;
    }
  }
  pCx->isOrdered = (pOp->p5!=BTREE_UNORDERED);
  // break;
  return rc;
}

/* Opcode: MakeRecord P1 P2 P3 P4 *
** Synopsis: r[P3]=mkrec(r[P1@P2])
**
** Convert P2 registers beginning with P1 into the [record format]
** use as a data record in a database table or as a key
** in an index.  The OP_Column opcode can decode the record later.
**
** P4 may be a string that is P2 characters long.  The nth character of the
** string indicates the column affinity that should be used for the nth
** field of the index key.
**
** The mapping from character to affinity is given by the SQLITE_AFF_
** macros defined in sqliteInt.h.
**
** If P4 is NULL then all index fields have the affinity NONE.
*/
void impl_OP_MakeRecord(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_MakeRecord: {
  u8 *zNewRecord;        /* A buffer to hold the data for the new record */
  Mem *pRec;             /* The new record */
  u64 nData;             /* Number of bytes of data space */
  int nHdr;              /* Number of bytes of header space */
  i64 nByte;             /* Data space required for this record */
  int nZero;             /* Number of zero bytes at the end of the record */
  int nVarint;           /* Number of bytes in a varint */
  u32 serial_type;       /* Type field */
  Mem *pData0;           /* First field to be combined into the record */
  Mem *pLast;            /* Last field of the record */
  int nField;            /* Number of fields in the record */
  char *zAffinity;       /* The affinity string for the record */
  int file_format;       /* File format to use for encoding */
  int i;                 /* Space used in zNewRecord[] header */
  int j;                 /* Space used in zNewRecord[] content */
  int len;               /* Length of a field */

  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pOut;                 /* Output operand */
  u8 encoding = ENC(db);     /* The database encoding */

  /* Assuming the record contains N fields, the record format looks
  ** like this:
  **
  ** ------------------------------------------------------------------------
  ** | hdr-size | type 0 | type 1 | ... | type N-1 | data0 | ... | data N-1 | 
  ** ------------------------------------------------------------------------
  **
  ** Data(0) is taken from register P1.  Data(1) comes from register P1+1
  ** and so froth.
  **
  ** Each type field is a varint representing the serial type of the 
  ** corresponding data element (see sqlite3VdbeSerialType()). The
  ** hdr-size field is also a varint which is the offset from the beginning
  ** of the record to data0.
  */
  nData = 0;         /* Number of bytes of data space */
  nHdr = 0;          /* Number of bytes of header space */
  nZero = 0;         /* Number of zero bytes at the end of the record */
  nField = pOp->p1;
  zAffinity = pOp->p4.z;
  assert( nField>0 && pOp->p2>0 && pOp->p2+nField<=(p->nMem-p->nCursor)+1 );
  pData0 = &aMem[nField];
  nField = pOp->p2;
  pLast = &pData0[nField-1];
  file_format = p->minWriteFileFormat;

  /* Identify the output register */
  assert( pOp->p3<pOp->p1 || pOp->p3>=pOp->p1+pOp->p2 );
  pOut = &aMem[pOp->p3];
  memAboutToChange(p, pOut);

  /* Apply the requested affinity to all inputs
  */
  assert( pData0<=pLast );
  if( zAffinity ){
    pRec = pData0;
    do{
      applyAffinity(pRec++, *(zAffinity++), encoding);
      assert( zAffinity[0]==0 || pRec<=pLast );
    }while( zAffinity[0] );
  }

  /* Loop through the elements that will make up the record to figure
  ** out how much space is required for the new record.
  */
  pRec = pLast;
  do{
    assert( memIsValid(pRec) );
    serial_type = sqlite3VdbeSerialType(pRec, file_format);
    len = sqlite3VdbeSerialTypeLen(serial_type);
    if( pRec->flags & MEM_Zero ){
      if( nData ){
        sqlite3VdbeMemExpandBlob(pRec);
      }else{
        nZero += pRec->u.nZero;
        len -= pRec->u.nZero;
      }
    }
    nData += len;
    testcase( serial_type==127 );
    testcase( serial_type==128 );
    nHdr += serial_type<=127 ? 1 : sqlite3VarintLen(serial_type);
  }while( (--pRec)>=pData0 );

  /* Add the initial header varint and total the size */
  testcase( nHdr==126 );
  testcase( nHdr==127 );
  if( nHdr<=126 ){
    /* The common case */
    nHdr += 1;
  }else{
    /* Rare case of a really large header */
    nVarint = sqlite3VarintLen(nHdr);
    nHdr += nVarint;
    if( nVarint<sqlite3VarintLen(nHdr) ) nHdr++;
  }
  nByte = nHdr+nData;
  if( nByte>db->aLimit[SQLITE_LIMIT_LENGTH] ){
    // goto too_big;
    printf("In impl_OP_MakeRecord(): too_big.\n");
    assert(0);    
  }

  /* Make sure the output register has a buffer large enough to store 
  ** the new record. The output register (pOp->p3) is not allowed to
  ** be one of the input registers (because the following call to
  ** sqlite3VdbeMemGrow() could clobber the value before it is used).
  */
  if( sqlite3VdbeMemGrow(pOut, (int)nByte, 0) ){
    // goto no_mem;
    printf("In impl_OP_MakeRecord(): no_mem.\n");
    assert(0);    
  }
  zNewRecord = (u8 *)pOut->z;

  /* Write the record */
  i = putVarint32(zNewRecord, nHdr);
  j = nHdr;
  assert( pData0<=pLast );
  pRec = pData0;
  do{
    serial_type = sqlite3VdbeSerialType(pRec, file_format);
    i += putVarint32(&zNewRecord[i], serial_type);            /* serial type */
    j += sqlite3VdbeSerialPut(&zNewRecord[j], pRec, serial_type); /* content */
  }while( (++pRec)<=pLast );
  assert( i==nHdr );
  assert( j==nByte );

  assert( pOp->p3>0 && pOp->p3<=(p->nMem-p->nCursor) );
  pOut->n = (int)nByte;
  pOut->flags = MEM_Blob;
  pOut->xDel = 0;
  if( nZero ){
    pOut->u.nZero = nZero;
    pOut->flags |= MEM_Zero;
  }
  pOut->enc = SQLITE_UTF8;  /* In case the blob is ever converted to text */
  REGISTER_TRACE(pOp->p3, pOut);
  UPDATE_MAX_BLOBSIZE(pOut);
  // break;
}

/* Opcode: IdxInsert P1 P2 P3 * P5
** Synopsis: key=r[P2]
**
** Register P2 holds an SQL index key made using the
** MakeRecord instructions.  This opcode writes that key
** into the index P1.  Data for the entry is nil.
**
** P3 is a flag that provides a hint to the b-tree layer that this
** insert is likely to be an append.
**
** If P5 has the OPFLAG_NCHANGE bit set, then the change counter is
** incremented by this instruction.  If the OPFLAG_NCHANGE bit is clear,
** then the change counter is unchanged.
**
** If P5 has the OPFLAG_USESEEKRESULT bit set, then the cursor must have
** just done a seek to the spot where the new entry is to be inserted.
** This flag avoids doing an extra seek.
**
** This instruction only works for indices.  The equivalent instruction
** for tables is OP_Insert.
*/
int impl_OP_SorterInsert_IdxInsert(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_SorterInsert:       /* in2 */
// case OP_IdxInsert: {        /* in2 */
  VdbeCursor *pC;
  BtCursor *pCrsr;
  int nKey;
  const char *zKey;

  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pIn2;
  int rc;

  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  assert( isSorter(pC)==(pOp->opcode==OP_SorterInsert) );
  pIn2 = &aMem[pOp->p2];
  assert( pIn2->flags & MEM_Blob );
  pCrsr = pC->pCursor;
  if( pOp->p5 & OPFLAG_NCHANGE ) p->nChange++;
  assert( pCrsr!=0 );
  assert( pC->isTable==0 );
  rc = ExpandBlob(pIn2);
  if( rc==SQLITE_OK ){
    if( isSorter(pC) ){
      rc = sqlite3VdbeSorterWrite(db, pC, pIn2);
    }else{
      nKey = pIn2->n;
      zKey = pIn2->z;
      rc = sqlite3BtreeInsert(pCrsr, zKey, nKey, "", 0, 0, pOp->p3, 
          ((pOp->p5 & OPFLAG_USESEEKRESULT) ? pC->seekResult : 0)
          );
      assert( pC->deferredMoveto==0 );
      pC->cacheStatus = CACHE_STALE;
    }
  }
  // break;
  return rc;
}

/* Opcode: Found P1 P2 P3 P4 *
** Synopsis: key=r[P3@P4]
**
** If P4==0 then register P3 holds a blob constructed by MakeRecord.  If
** P4>0 then register P3 is the first of P4 registers that form an unpacked
** record.
**
** Cursor P1 is on an index btree.  If the record identified by P3 and P4
** is a prefix of any entry in P1 then a jump is made to P2 and
** P1 is left pointing at the matching entry.
**
** See also: NotFound, NoConflict, NotExists. SeekGe
*/
/* Opcode: NotFound P1 P2 P3 P4 *
** Synopsis: key=r[P3@P4]
**
** If P4==0 then register P3 holds a blob constructed by MakeRecord.  If
** P4>0 then register P3 is the first of P4 registers that form an unpacked
** record.
** 
** Cursor P1 is on an index btree.  If the record identified by P3 and P4
** is not the prefix of any entry in P1 then a jump is made to P2.  If P1 
** does contain an entry whose prefix matches the P3/P4 record then control
** falls through to the next instruction and P1 is left pointing at the
** matching entry.
**
** See also: Found, NotExists, NoConflict
*/
/* Opcode: NoConflict P1 P2 P3 P4 *
** Synopsis: key=r[P3@P4]
**
** If P4==0 then register P3 holds a blob constructed by MakeRecord.  If
** P4>0 then register P3 is the first of P4 registers that form an unpacked
** record.
** 
** Cursor P1 is on an index btree.  If the record identified by P3 and P4
** contains any NULL value, jump immediately to P2.  If all terms of the
** record are not-NULL then a check is done to determine if any row in the
** P1 index btree has a matching key prefix.  If there are no matches, jump
** immediately to P2.  If there is a match, fall through and leave the P1
** cursor pointing to the matching row.
**
** This opcode is similar to OP_NotFound with the exceptions that the
** branch is always taken if any part of the search key input is NULL.
**
** See also: NotFound, Found, NotExists
*/
int impl_OP_NoConflict_NotFound_Found(Vdbe *p, sqlite3 *db, int *pc, Op *pOp) {
// case OP_NoConflict:     /* jump, in3 */
// case OP_NotFound:       /* jump, in3 */
// case OP_Found: {        /* jump, in3 */
  int alreadyExists;
  int ii;
  VdbeCursor *pC;
  int res;
  char *pFree;
  UnpackedRecord *pIdxKey;
  UnpackedRecord r;
  char aTempRec[ROUND8(sizeof(UnpackedRecord)) + sizeof(Mem)*4 + 7];

  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pIn3;                 /* 3rd input operand */
  Mem *pOut;                 /* Output operand */
  int rc;

#ifdef SQLITE_TEST
  if( pOp->opcode!=OP_NoConflict ) sqlite3_found_count++;
#endif

  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  assert( pOp->p4type==P4_INT32 );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  pIn3 = &aMem[pOp->p3];
  assert( pC->pCursor!=0 );
  assert( pC->isTable==0 );
  pFree = 0;  /* Not needed.  Only used to suppress a compiler warning. */
  if( pOp->p4.i>0 ){
    r.pKeyInfo = pC->pKeyInfo;
    r.nField = (u16)pOp->p4.i;
    r.aMem = pIn3;
    for(ii=0; ii<r.nField; ii++){
      assert( memIsValid(&r.aMem[ii]) );
      ExpandBlob(&r.aMem[ii]);
#ifdef SQLITE_DEBUG
      if( ii ) REGISTER_TRACE(pOp->p3+ii, &r.aMem[ii]);
#endif
    }
    pIdxKey = &r;
  }else{
    pIdxKey = sqlite3VdbeAllocUnpackedRecord(
        pC->pKeyInfo, aTempRec, sizeof(aTempRec), &pFree
    ); 
    if( pIdxKey==0 ) {
      // goto no_mem;
      printf("In impl_OP_NoConflict_NotFound_Found(): no_mem.\n");
      assert(0);      
    }
    assert( pIn3->flags & MEM_Blob );
    assert( (pIn3->flags & MEM_Zero)==0 );  /* zeroblobs already expanded */
    sqlite3VdbeRecordUnpack(pC->pKeyInfo, pIn3->n, pIn3->z, pIdxKey);
  }
  pIdxKey->default_rc = 0;
  if( pOp->opcode==OP_NoConflict ){
    /* For the OP_NoConflict opcode, take the jump if any of the
    ** input fields are NULL, since any key with a NULL will not
    ** conflict */
    for(ii=0; ii<r.nField; ii++){
      if( r.aMem[ii].flags & MEM_Null ){
        *pc = pOp->p2 - 1;
        VdbeBranchTaken(1,2);
        break;
      }
    }
  }
  rc = sqlite3BtreeMovetoUnpacked(pC->pCursor, pIdxKey, 0, 0, &res);
  if( pOp->p4.i==0 ){
    sqlite3DbFree(db, pFree);
  }
  if( rc!=SQLITE_OK ){
    // break;
    return rc;
  }
  pC->seekResult = res;
  alreadyExists = (res==0);
  pC->nullRow = 1-alreadyExists;
  pC->deferredMoveto = 0;
  pC->cacheStatus = CACHE_STALE;
  if( pOp->opcode==OP_Found ){
    VdbeBranchTaken(alreadyExists!=0,2);
    if( alreadyExists ) *pc = pOp->p2 - 1;
  }else{
    VdbeBranchTaken(alreadyExists==0,2);
    if( !alreadyExists ) *pc = pOp->p2 - 1;
  }
  // break;
  return rc;
}

/* Opcode: RowSetTest P1 P2 P3 P4
** Synopsis: if r[P3] in rowset(P1) goto P2
**
** Register P3 is assumed to hold a 64-bit integer value. If register P1
** contains a RowSet object and that RowSet object contains
** the value held in P3, jump to register P2. Otherwise, insert the
** integer in P3 into the RowSet and continue on to the
** next opcode.
**
** The RowSet object is optimized for the case where successive sets
** of integers, where each set contains no duplicates. Each set
** of values is identified by a unique P4 value. The first set
** must have P4==0, the final set P4=-1.  P4 must be either -1 or
** non-negative.  For non-negative values of P4 only the lower 4
** bits are significant.
**
** This allows optimizations: (a) when P4==0 there is no need to test
** the rowset object for P3, as it is guaranteed not to contain it,
** (b) when P4==-1 there is no need to insert the value, as it will
** never be tested for, and (c) when a value that is part of set X is
** inserted, there is no need to search to see if the same value was
** previously inserted as part of set X (only if it was previously
** inserted as part of some other set).
*/
int impl_OP_RowSetTest(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_RowSetTest: {                     /* jump, in1, in3 */
  int iSet;
  int exists;

  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pIn1;                 /* 1st input operand */
  Mem *pIn3;                 /* 3rd input operand */

  pIn1 = &aMem[pOp->p1];
  pIn3 = &aMem[pOp->p3];
  iSet = pOp->p4.i;
  assert( pIn3->flags&MEM_Int );

  /* If there is anything other than a rowset object in memory cell P1,
  ** delete it now and initialize P1 with an empty rowset
  */
  if( (pIn1->flags & MEM_RowSet)==0 ){
    sqlite3VdbeMemSetRowSet(pIn1);
    if( (pIn1->flags & MEM_RowSet)==0 ) {
      // goto no_mem;
      printf("In impl_OP_RowSetTest(): no_mem.\n");
      assert(0);      
    }
  }

  assert( pOp->p4type==P4_INT32 );
  assert( iSet==-1 || iSet>=0 );
  if( iSet ){
    exists = sqlite3RowSetTest(pIn1->u.pRowSet, iSet, pIn3->u.i);
    VdbeBranchTaken(exists!=0,2);
    if( exists ){
      pc = pOp->p2 - 1;
      // break;
      return pc;
    }
  }
  if( iSet>=0 ){
    sqlite3RowSetInsert(pIn1->u.pRowSet, pIn3->u.i);
  }
  // break;
  return pc;
}

/* Opcode:  Gosub P1 P2 * * *
**
** Write the current address onto register P1
** and then jump to address P2.
*/
int impl_OP_Gosub(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_Gosub: {            /* jump */
  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pIn1;                 /* 1st input operand */

  assert( pOp->p1>0 && pOp->p1<=(p->nMem-p->nCursor) );
  pIn1 = &aMem[pOp->p1];
  assert( VdbeMemDynamic(pIn1)==0 );
  memAboutToChange(p, pIn1);
  pIn1->flags = MEM_Int;
  pIn1->u.i = pc;
  REGISTER_TRACE(pOp->p1, pIn1);
  pc = pOp->p2 - 1;
  // break;
  return pc;
}

/* Opcode:  Return P1 * * * *
**
** Jump to the next instruction after the address in register P1.  After
** the jump, register P1 becomes undefined.
*/
int impl_OP_Return(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
// case OP_Return: {           /* in1 */
  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pIn1;                 /* 1st input operand */

  pIn1 = &aMem[pOp->p1];
  assert( pIn1->flags==MEM_Int );
  pc = (int)pIn1->u.i;
  pIn1->flags = MEM_Undefined;
  // break;
  return pc;
}
