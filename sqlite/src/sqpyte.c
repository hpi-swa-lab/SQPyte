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
int impl_OP_Compare(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
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
  Mem *pIn1 = 0;             /* 1st input operand */
  Mem *pIn3 = 0;             /* 3rd input operand */
  Mem *pOut = 0;             /* Output operand */

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
      u8 encoding = ENC(db);  // Added by DK
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
  Mem *pOut = 0;             /* Output operand */
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
  Mem *pOut = 0;             /* Output operand */

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
  Mem *pIn1 = 0;             /* 1st input operand */
  Mem *pOut = 0;             /* Output operand */

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
  Mem *pIn1 = 0;             /* 1st input operand */
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
  Mem *pIn3 = 0;             /* 3rd input operand */
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
