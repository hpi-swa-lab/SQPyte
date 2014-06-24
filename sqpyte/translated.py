from rpython.rtyper.lltypesystem import rffi
from capi import CConfig
from rpython.rtyper.lltypesystem import lltype
import capi

def allocateCursor(vdbe_struct, p1, nField, iDb, isBtreeCursor):
    return capi.sqlite3_allocateCursor(vdbe_struct, p1, nField, iDb, isBtreeCursor) 

def sqlite3VdbeMemIntegerify(pMem):
    return capi.sqlite3_sqlite3VdbeMemIntegerify(pMem)

def sqlite3BtreeCursor(p, iTable, wrFlag, pKeyInfo, pCur):
    return capi.sqlite3_sqlite3BtreeCursor(p, iTable, wrFlag, pKeyInfo, pCur)

def sqlite3BtreeCursorHints(btCursor, mask):
    return capi.sqlite3_sqlite3BtreeCursorHints(btCursor, mask)

def sqlite3VdbeSorterRewind(db, pC, res):
    with lltype.scoped_alloc(rffi.INTP.TO, 1) as res:
        errorcode = capi.sqlite3_sqlite3VdbeSorterRewind(db, pC, res)
        return rffi.cast(rffi.INTP, res[0])

def python_OP_OpenRead_translated(p, db, pc, pOp):
    assert pOp.p5 & (CConfig.OPFLAG_P2ISREG | CConfig.OPFLAG_BULKCSR) == pOp.p5
    assert pOp.opcode == CConfig.OP_OpenWrite or pOp.p5 == 0
    # assert(p.bIsReader)
    assert pOp.opcode == CConfig.OP_OpenRead or p.readOnly == 0

    if p.expired:
        rc = CConfig.SQLITE_ABORT
        return

    nField = 0
    pKeyInfo = lltype.nullptr(capi.KEYINFO)
    p2 = pOp.p2;
    iDb = pOp.p3
    wrFlag = 0

    assert iDb >= 0 and iDb < db.nDb
    #   assert( (p->btreeMask & (((yDbMask)1)<<iDb))!=0 );

    pDb = db.aDb[iDb]
    pX = pDb.pBt

    assert pX

    if pOp.opcode == CConfig.OP_OpenWrite:
        wrFlag = 1
        #     assert( sqlite3SchemaMutexHeld(db, iDb, 0) );
        if pDb.pSchema.file_format < p.minWriteFileFormat:
            p.minWriteFileFormat = pDb.pSchema.file_format
        else:
            wrFlag = 0

    if pOp.p5 & CConfig.OPFLAG_P2ISREG:
        assert p2 > 0
        assert p2 <= p.nMem - p.nCursor
        pIn2 = aMem[p2]
        #     assert( memIsValid(pIn2) );
        #     assert( (pIn2->flags & MEM_Int)!=0 );
        sqlite3VdbeMemIntegerify(pIn2)
        p2 = rffi.cast(rffi.INT, pIn2.u.i)
        #     /* The p2 value always comes from a prior OP_CreateTable opcode and
        #     ** that opcode will always set the p2 value to 2 or more or else fail.
        #     ** If there were a failure, the prepared statement would have halted
        #     ** before reaching this instruction. */
        #     if( NEVER(p2<2) ) {
        #       rc = SQLITE_CORRUPT_BKPT;
        #       goto abort_due_to_error;
        #     }
        if pOp.p4type == CConfig.P4_KEYINFO:
            pKeyInfo = pOp.p4.pKeyInfo
            #     assert( pKeyInfo->enc==ENC(db) );
            assert(pKeyInfo.db == db)
            nField = intmask(pKeyInfo.nField) + intmask(pKeyInfo.nXField)
        elif pOp.p4type == CConfig.P4_INT32:
            nField = pOp.p4.i

        assert pOp.p1 >= 0
        assert nField >= 0
        #   testcase( nField==0 );  /* Table with INTEGER PRIMARY KEY and nothing else */
        pCur = allocateCursor(p, pOp.p1, nField, iDb, 1)
        #   if( pCur==0 ) goto no_mem;
        pCur.nullRow = 1
        pCur.isOrdered = bool(1)
        rc = sqlite3BtreeCursor(pX, p2, wrFlag, pKeyInfo, pCur.pCursor)
        pCur.pKeyInfo = pKeyInfo
        assert CConfig.OPFLAG_BULKCSR == CConfig.BTREE_BULKLOAD
        sqlite3BtreeCursorHints(pCur.pCursor, (pOp.p5 & CConfig.OPFLAG_BULKCSR))

        #   /* Since it performs no memory allocation or IO, the only value that
        #   ** sqlite3BtreeCursor() may return is SQLITE_OK. */
        assert rc == CConfig.SQLITE_OK
        #   /* Set the VdbeCursor.isTable variable. Previous versions of
        #   ** SQLite used to check if the root-page flags were sane at this point
        #   ** and report database corruption if they were not, but this check has
        #   ** since moved into the btree layer.  */
        pCur.isTable = pOp.p4type != CConfig.P4_KEYINFO


def sqlite3BtreeNext(pCur, pRes):
    with lltype.scoped_alloc(rffi.INTP.TO, 1) as res:
        rc = capi.sqlite3_sqlite3BtreeNext(pCur, res)
        return rc, res[0]

def python_OP_Next_translated(p, db, pc, pOp):
    pcRet = pc
    assert pOp.p1 >= 0 and pOp.p1 < p.nCursor
    assert pOp.p5 < len(p.aCounter)
    pC = p.apCsr[pOp.p1]
    res = pOp.p3
    assert pC
    assert pC.deferredMoveto == 0
    assert pC.pCursor
    assert res == 0 or (res == 1 and pC.isTable == 0)
    # testcase( res==1 );
    # assert( pOp->opcode!=OP_Next || pOp->p4.xAdvance==sqlite3BtreeNext );
    # assert( pOp->opcode!=OP_Prev || pOp->p4.xAdvance==sqlite3BtreePrevious );
    # assert( pOp->opcode!=OP_NextIfOpen || pOp->p4.xAdvance==sqlite3BtreeNext );
    # assert( pOp->opcode!=OP_PrevIfOpen || pOp->p4.xAdvance==sqlite3BtreePrevious);
    
    # rc = pOp->p4.xAdvance(pC->pCursor, &res);
    rc, resRet = sqlite3BtreeNext(pC.pCursor, res)

    # next_tail:
    pC.cacheStatus = rffi.cast(rffi.UINT, CConfig.CACHE_STALE)
    # VdbeBranchTaken(res==0, 2)
    if resRet == 0:
        pC.nullRow = rffi.cast(rffi.UCHAR, 0)
        pcRet = pOp.p2 - 1
        p.aCounter[pOp.p5] += 1
        #ifdef SQLITE_TEST
            # sqlite3_search_count++;
        #endif
    else:
        pC.nullRow = rffi.cast(rffi.UCHAR, 1)

    pC.rowidIsValid = rffi.cast(rffi.UCHAR, 0)
    # goto check_for_interrupt;
    return pcRet, rc


def python_OP_Column_translated(p, db, pc, pOp):
    print 'entered'
    aMem = p.aMem
    encoding = db.aDb[0].pSchema.enc
    p2 = pOp.p2
    assert pOp.p3 > 0 and pOp.p3 <= (p.nMem - p.nCursor)
    pDest = aMem[pOp.p3]

  # memAboutToChange(p, pDest);

    assert pOp.p1 >= 0 and pOp.p1 < p.nCursor
    pC = p.apCsr[pOp.p1]
    assert pC
    assert p2 < pC.nField
    aType = pC.aType
    aOffset = aType + pC.nField

# #ifndef SQLITE_OMIT_VIRTUALTABLE
#   assert( pC->pVtabCursor==0 ); /* OP_Column never called on virtual table */
# #endif
    pCrsr = pC.pCursor
    assert pCrsr != 0 or pC.pseudoTableReg > 0
    assert pCrsr != 0 or pC.nullRow


  # /* If the cursor cache is stale, bring it up-to-date */
  # rc = sqlite3VdbeCursorMoveto(pC);
    if rc:
        # goto abort_due_to_error;
        print "In python_OP_Column_translated(): abort_due_to_error."
        assert False

    if pC.cacheStatus != p.cacheCtr or (pOp.p5 & CConfig.OPFLAG_CLEARCACHE) != 0:
        if pC.nullRow:
            if pCrsr == 0:
                assert pC.pseudoTableReg > 0
                pReg = aMem[pC.pseudoTableReg]
                assert pReg.flags & MEM_Blob
                # assert( memIsValid(pReg) );
                pC.payloadSize = pC.szRow = avail = pReg.n
                pC.aRow = pReg.z # pC->aRow = (u8*)pReg->z;
            else:
                # MemSetTypeFlag(pDest, MEM_Null);
                # goto op_column_out;
                pass
        else:
            assert pCrsr
            if pC.isTable == 0:
                # assert( sqlite3BtreeCursorIsValid(pCrsr) );
                # VVA_ONLY(rc =) sqlite3BtreeKeySize(pCrsr, &payloadSize64);
                assert rc == CConfig.SQLITE_OK
                # assert( (payloadSize64 & SQLITE_MAX_U32)==(u64)payloadSize64 );
                # pC->aRow = sqlite3BtreeKeyFetch(pCrsr, &avail);
                pC.payloadSize = payloadSize # pC->payloadSize = (u32)payloadSize64;
            else:
                # assert( sqlite3BtreeCursorIsValid(pCrsr) );
                # VVA_ONLY(rc =) sqlite3BtreeDataSize(pCrsr, &pC->payloadSize);
                assert rc == CConfig.SQLITE_OK
                # pC->aRow = sqlite3BtreeDataFetch(pCrsr, &avail);
            assert avail <= 65536
            if pC.payloadSize <= avail:
                pC.szRow = pC.payloadSize
            else:
                pC.szRow = avail
            if pC.payloadSize > db.aLimit[CConfig.SQLITE_LIMIT_LENGTH]:
                # goto too_big;
                print "In python_OP_Column_translated(): too_big."
                assert False
                pass

        pass
        if pC.cacheStatus != p.cacheCtr or (pOp.p5 & CConfig.OPFLAG_CLEARCACHE) != 0:
            pC.cacheStatus = p.cacheCtr
            # pC->iHdrOffset = getVarint32(pC->aRow, offset);
            pC.nHdrParsed = 0
            aOffset[0] = offset
            if avail < offset:
                pC.aRow = 0
                pC.szRow = 0
            if offset > 98307 or offset > pC.payloadSize:
                rc = CConfig.SQLITE_CORRUPT_BKPT
                # UPDATE_MAX_BLOBSIZE(pDest);
                # REGISTER_TRACE(pOp->p3, pDest);
                return rc

    # end of if pC.cacheStatus != p.cacheCtr or (pOp.p5 & CConfig.OPFLAG_CLEARCACHE) != 0

    if pC.nHdrParsed <= p2:
        if pC.iHdrOffset < aOffset[0]:
            if pC.aRow == 0:
                # memset(&sMem, 0, sizeof(sMem));
                # rc = sqlite3VdbeMemFromBtree(pCrsr, 0, aOffset[0], !pC->isTable, &sMem);
                if rc != CConfig.SQLITE_OK:
                    # UPDATE_MAX_BLOBSIZE(pDest);
                    # REGISTER_TRACE(pOp->p3, pDest);
                    return rc
                zData = sMem.z # zData = (u8*)sMem.z;
            else:
                zData = pC.aRow

            i = pC.nHdrParsed
            offset = aOffset[i]
            zHdr = zData + pC.iHdrOffset
            zEndHdr = zData + aOffset[0]
            assert i <= p2 and zHdr < zEndHdr

            while i <= p2 and zHdr < zEndHdr:
                if zHdr[0] < 128: # if( zHdr[0]<0x80 ){
                    t = zHdr[0]
                    zHdr += 1
                else:
                    # zHdr += sqlite3GetVarint32(zHdr, &t);
                    pass
                aType[i] = t
                # szField = sqlite3VdbeSerialTypeLen(t);
                offset += szField
                if offset < szField:
                    zHdr = zEndHdr[1] # zHdr = &zEndHdr[1];  /* Forces SQLITE_CORRUPT return below */
                    break
                i += 1
                aOffset[i] = offset

            pC.nHdrParsed = i
            pC.iHdrOffset = zHdr - zData # pC->iHdrOffset = (u32)(zHdr - zData);
            if pC.aRow == 0:
                # sqlite3VdbeMemRelease(&sMem);
                # sMem.flags = CConfig.
                pass

            if zHdr > zEndHdr or offset > pC.payloadSize or (zHdr == zEndHdr and offset != pC.payloadSize):
                rc = CConfig.SQLITE_CORRUPT_BKPT
                # UPDATE_MAX_BLOBSIZE(pDest);
                # REGISTER_TRACE(pOp->p3, pDest);
                return rc

        # end of if pC.iHdrOffset < aOffset[0]

        if pC.nHdrParsed <= p2:
            if pOp.p4type == CConfig.P4_MEM:
                # sqlite3VdbeMemShallowCopy(pDest, pOp->p4.pMem, MEM_Static);
                pass
            else:
                # MemSetTypeFlag(pDest, MEM_Null);
                pass
            pass
            # // Translated Deephemeralize(pDest);
            # if( ((pDest)->flags&MEM_Ephem)!=0 && sqlite3VdbeMemMakeWriteable(pDest) ) {
            #     // goto no_mem;
            #     printf("In impl_OP_Column(): no_mem.\n");
            #     assert(0);
            # }

    # end of if pC.nHdrParsed <= p2

    assert p2 < pC.nHdrParsed
    assert rc == CConfig.SQLITE_OK
    # assert( sqlite3VdbeCheckMemInvariants(pDest) );  
    if pC.szRow >= aOffset[p2 + 1]:
        # VdbeMemRelease(pDest);
        # sqlite3VdbeSerialGet(pC->aRow+aOffset[p2], aType[p2], pDest);
        pass
    else:
        t = aType[p2]
        if (pOp.p5 & (CConfig.OPFLAG_LENGTHARG | CConfig.OPFLAG_TYPEOFARG)) != 0 and \
           ((t >= 12 and t & 1 == 0) or (pOp.p5 & CConfig.OPFLAG_TYPEOFARG) != 0):
        # if( ((pOp->p5 & (OPFLAG_LENGTHARG|OPFLAG_TYPEOFARG))!=0
        #       && ((t>=12 && (t&1)==0) || (pOp->p5 & OPFLAG_TYPEOFARG)!=0))
        #  || (len = sqlite3VdbeSerialTypeLen(t))==0
            zData = payloadSize64 if t <= 13 else 0 # zData = t<=13 ? (u8*)&payloadSize64 : 0;
            sMem.zMalloc = 0
        else:
            # memset(&sMem, 0, sizeof(sMem));
            # sqlite3VdbeMemMove(&sMem, pDest);
            # rc = sqlite3VdbeMemFromBtree(pCrsr, aOffset[p2], len, !pC->isTable, &sMem);
            if rc != CConfig.SQLITE_OK:
                # // Translated Deephemeralize(pDest);
                # if( ((pDest)->flags&MEM_Ephem)!=0 && sqlite3VdbeMemMakeWriteable(pDest) ) {
                #     // goto no_mem;
                #     printf("In impl_OP_Column(): no_mem.\n");
                #     assert(0);
                # }
                pass
            zData = sMem.z # zData = (u8*)sMem.z;
        # sqlite3VdbeSerialGet(zData, t, pDest);
        if sMem.zMalloc:
            assert sMem.z == sMem.zMalloc
            # assert( VdbeMemDynamic(pDest)==0 );
            # assert( (pDest->flags & (MEM_Blob|MEM_Str))==0 || pDest->z==sMem.z );
            # pDest->flags &= ~(MEM_Ephem|MEM_Static);
            # pDest->flags |= MEM_Term;
            pDest.z = sMem.z
            pDest.zMalloc = sMem.zMalloc
    pDest.enc = encoding

    # // Translated Deephemeralize(pDest);
    # if( ((pDest)->flags&MEM_Ephem)!=0 && sqlite3VdbeMemMakeWriteable(pDest) ) {
    #     // goto no_mem;
    #     printf("In impl_OP_Column(): no_mem.\n");
    #     assert(0);
    # }

    # UPDATE_MAX_BLOBSIZE(pDest);
    # REGISTER_TRACE(pOp->p3, pDest);

    return rc



# int impl_OP_Column(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {
#   i64 payloadSize64; /* Number of bytes in the record */
#   int p2;            /* column number to retrieve */
#   VdbeCursor *pC;    /* The VDBE cursor */
#   BtCursor *pCrsr;   /* The BTree cursor */
#   u32 *aType;        /* aType[i] holds the numeric type of the i-th column */
#   u32 *aOffset;      /* aOffset[i] is offset to start of data for i-th column */
#   int len;           /* The length of the serialized data for the column */
#   int i;             /* Loop counter */
#   Mem *pDest;        /* Where to write the extracted value */
#   Mem sMem;          /* For storing the record being decoded */
#   const u8 *zData;   /* Part of the record being decoded */
#   const u8 *zHdr;    /* Next unparsed byte of the header */
#   const u8 *zEndHdr; /* Pointer to first byte after the header */
#   u32 offset;        /* Offset into the data */
#   u32 szField;       /* Number of bytes in the content of a field */
#   u32 avail;         /* Number of bytes of available data */
#   u32 t;             /* A type code from the record header */
#   Mem *pReg;         /* PseudoTable input register */
#   int rc;
#   Mem *aMem = p->aMem;
#   u8 encoding = ENC(db);     /* The database encoding */

#   p2 = pOp->p2;
#   assert( pOp->p3>0 && pOp->p3<=(p->nMem-p->nCursor) );
#   pDest = &aMem[pOp->p3];
#   memAboutToChange(p, pDest);
#   assert( pOp->p1>=0 && pOp->p1<p->nCursor );
#   pC = p->apCsr[pOp->p1];
#   assert( pC!=0 );
#   assert( p2<pC->nField );
#   aType = pC->aType;
#   aOffset = aType + pC->nField;
# #ifndef SQLITE_OMIT_VIRTUALTABLE
#   assert( pC->pVtabCursor==0 ); /* OP_Column never called on virtual table */
# #endif
#   pCrsr = pC->pCursor;
#   assert( pCrsr!=0 || pC->pseudoTableReg>0 ); /* pCrsr NULL on PseudoTables */
#   assert( pCrsr!=0 || pC->nullRow );          /* pC->nullRow on PseudoTables */

#   /* If the cursor cache is stale, bring it up-to-date */
#   rc = sqlite3VdbeCursorMoveto(pC);
#   if( rc ) {
#     // goto abort_due_to_error;
#       printf("In impl_OP_Column(): abort_due_to_error.\n");
#       assert(0);
#   }
#   if( pC->cacheStatus!=p->cacheCtr || (pOp->p5&OPFLAG_CLEARCACHE)!=0 ){
#     if( pC->nullRow ){
#       if( pCrsr==0 ){
#         assert( pC->pseudoTableReg>0 );
#         pReg = &aMem[pC->pseudoTableReg];
#         assert( pReg->flags & MEM_Blob );
#         assert( memIsValid(pReg) );
#         pC->payloadSize = pC->szRow = avail = pReg->n;
#         pC->aRow = (u8*)pReg->z;
#       }else{
#         MemSetTypeFlag(pDest, MEM_Null);
#         goto op_column_out;
#       }
#     }else{
#       assert( pCrsr );
#       if( pC->isTable==0 ){
#         assert( sqlite3BtreeCursorIsValid(pCrsr) );
#         VVA_ONLY(rc =) sqlite3BtreeKeySize(pCrsr, &payloadSize64);
#         assert( rc==SQLITE_OK ); /* True because of CursorMoveto() call above */
#         /* sqlite3BtreeParseCellPtr() uses getVarint32() to extract the
#         ** payload size, so it is impossible for payloadSize64 to be
#         ** larger than 32 bits. */
#         assert( (payloadSize64 & SQLITE_MAX_U32)==(u64)payloadSize64 );
#         pC->aRow = sqlite3BtreeKeyFetch(pCrsr, &avail);
#         pC->payloadSize = (u32)payloadSize64;
#       }else{
#         assert( sqlite3BtreeCursorIsValid(pCrsr) );
#         VVA_ONLY(rc =) sqlite3BtreeDataSize(pCrsr, &pC->payloadSize);
#         assert( rc==SQLITE_OK );   /* DataSize() cannot fail */
#         pC->aRow = sqlite3BtreeDataFetch(pCrsr, &avail);
#       }
#       assert( avail<=65536 );  /* Maximum page size is 64KiB */
#       if( pC->payloadSize <= (u32)avail ){
#         pC->szRow = pC->payloadSize;
#       }else{
#         pC->szRow = avail;
#       }
#       if( pC->payloadSize > (u32)db->aLimit[SQLITE_LIMIT_LENGTH] ){
#         // goto too_big;
#         printf("In impl_OP_Column(): too_big.\n");
#         assert(0);
#       }
#     }
#     pC->cacheStatus = p->cacheCtr;
#     pC->iHdrOffset = getVarint32(pC->aRow, offset);
#     pC->nHdrParsed = 0;
#     aOffset[0] = offset;
#     if( avail<offset ){
#       /* pC->aRow does not have to hold the entire row, but it does at least
#       ** need to cover the header of the record.  If pC->aRow does not contain
#       ** the complete header, then set it to zero, forcing the header to be
#       ** dynamically allocated. */
#       pC->aRow = 0;
#       pC->szRow = 0;
#     }

#     /* Make sure a corrupt database has not given us an oversize header.
#     ** Do this now to avoid an oversize memory allocation.
#     **
#     ** Type entries can be between 1 and 5 bytes each.  But 4 and 5 byte
#     ** types use so much data space that there can only be 4096 and 32 of
#     ** them, respectively.  So the maximum header length results from a
#     ** 3-byte type for each of the maximum of 32768 columns plus three
#     ** extra bytes for the header length itself.  32768*3 + 3 = 98307.
#     */
#     if( offset > 98307 || offset > pC->payloadSize ){
#       rc = SQLITE_CORRUPT_BKPT;
#       goto op_column_error;
#     }
#   }

#   /* Make sure at least the first p2+1 entries of the header have been
#   ** parsed and valid information is in aOffset[] and aType[].
#   */
#   if( pC->nHdrParsed<=p2 ){
#     /* If there is more header available for parsing in the record, try
#     ** to extract additional fields up through the p2+1-th field 
#     */
#     if( pC->iHdrOffset<aOffset[0] ){
#       /* Make sure zData points to enough of the record to cover the header. */
#       if( pC->aRow==0 ){
#         memset(&sMem, 0, sizeof(sMem));
#         rc = sqlite3VdbeMemFromBtree(pCrsr, 0, aOffset[0], 
#                                      !pC->isTable, &sMem);
#         if( rc!=SQLITE_OK ){
#           goto op_column_error;
#         }
#         zData = (u8*)sMem.z;
#       }else{
#         zData = pC->aRow;
#       }
  
#       /* Fill in aType[i] and aOffset[i] values through the p2-th field. */
#       i = pC->nHdrParsed;
#       offset = aOffset[i];
#       zHdr = zData + pC->iHdrOffset;
#       zEndHdr = zData + aOffset[0];
#       assert( i<=p2 && zHdr<zEndHdr );
#       do{
#         if( zHdr[0]<0x80 ){
#           t = zHdr[0];
#           zHdr++;
#         }else{
#           zHdr += sqlite3GetVarint32(zHdr, &t);
#         }
#         aType[i] = t;
#         szField = sqlite3VdbeSerialTypeLen(t);
#         offset += szField;
#         if( offset<szField ){  /* True if offset overflows */
#           zHdr = &zEndHdr[1];  /* Forces SQLITE_CORRUPT return below */
#           break;
#         }
#         i++;
#         aOffset[i] = offset;
#       }while( i<=p2 && zHdr<zEndHdr );
#       pC->nHdrParsed = i;
#       pC->iHdrOffset = (u32)(zHdr - zData);
#       if( pC->aRow==0 ){
#         sqlite3VdbeMemRelease(&sMem);
#         sMem.flags = MEM_Null;
#       }
  
#       /* If we have read more header data than was contained in the header,
#       ** or if the end of the last field appears to be past the end of the
#       ** record, or if the end of the last field appears to be before the end
#       ** of the record (when all fields present), then we must be dealing 
#       ** with a corrupt database.
#       */
#       if( (zHdr > zEndHdr)
#        || (offset > pC->payloadSize)
#        || (zHdr==zEndHdr && offset!=pC->payloadSize)
#       ){
#         rc = SQLITE_CORRUPT_BKPT;
#         goto op_column_error;
#       }
#     }

#     /* If after trying to extra new entries from the header, nHdrParsed is
#     ** still not up to p2, that means that the record has fewer than p2
#     ** columns.  So the result will be either the default value or a NULL.
#     */
#     if( pC->nHdrParsed<=p2 ){
#       if( pOp->p4type==P4_MEM ){
#         sqlite3VdbeMemShallowCopy(pDest, pOp->p4.pMem, MEM_Static);
#       }else{
#         MemSetTypeFlag(pDest, MEM_Null);
#       }
#       goto op_column_out;
#     }
#   }

#   /* Extract the content for the p2+1-th column.  Control can only
#   ** reach this point if aOffset[p2], aOffset[p2+1], and aType[p2] are
#   ** all valid.
#   */
#   assert( p2<pC->nHdrParsed );
#   assert( rc==SQLITE_OK );
#   assert( sqlite3VdbeCheckMemInvariants(pDest) );
#   if( pC->szRow>=aOffset[p2+1] ){
#     /* This is the common case where the desired content fits on the original
#     ** page - where the content is not on an overflow page */
#     VdbeMemRelease(pDest);
#     sqlite3VdbeSerialGet(pC->aRow+aOffset[p2], aType[p2], pDest);
#   }else{
#     /* This branch happens only when content is on overflow pages */
#     t = aType[p2];
#     if( ((pOp->p5 & (OPFLAG_LENGTHARG|OPFLAG_TYPEOFARG))!=0
#           && ((t>=12 && (t&1)==0) || (pOp->p5 & OPFLAG_TYPEOFARG)!=0))
#      || (len = sqlite3VdbeSerialTypeLen(t))==0
#     ){
#       /* Content is irrelevant for the typeof() function and for
#       ** the length(X) function if X is a blob.  So we might as well use
#       ** bogus content rather than reading content from disk.  NULL works
#       ** for text and blob and whatever is in the payloadSize64 variable
#       ** will work for everything else.  Content is also irrelevant if
#       ** the content length is 0. */
#       zData = t<=13 ? (u8*)&payloadSize64 : 0;
#       sMem.zMalloc = 0;
#     }else{
#       memset(&sMem, 0, sizeof(sMem));
#       sqlite3VdbeMemMove(&sMem, pDest);
#       rc = sqlite3VdbeMemFromBtree(pCrsr, aOffset[p2], len, !pC->isTable,
#                                    &sMem);
#       if( rc!=SQLITE_OK ){
#         goto op_column_error;
#       }
#       zData = (u8*)sMem.z;
#     }
#     sqlite3VdbeSerialGet(zData, t, pDest);
#     /* If we dynamically allocated space to hold the data (in the
#     ** sqlite3VdbeMemFromBtree() call above) then transfer control of that
#     ** dynamically allocated space over to the pDest structure.
#     ** This prevents a memory copy. */
#     if( sMem.zMalloc ){
#       assert( sMem.z==sMem.zMalloc );
#       assert( VdbeMemDynamic(pDest)==0 );
#       assert( (pDest->flags & (MEM_Blob|MEM_Str))==0 || pDest->z==sMem.z );
#       pDest->flags &= ~(MEM_Ephem|MEM_Static);
#       pDest->flags |= MEM_Term;
#       pDest->z = sMem.z;
#       pDest->zMalloc = sMem.zMalloc;
#     }
#   }
#   pDest->enc = encoding;

# op_column_out:
#   // Translated Deephemeralize(pDest);
#   if( ((pDest)->flags&MEM_Ephem)!=0 && sqlite3VdbeMemMakeWriteable(pDest) ) {
#     // goto no_mem;
#     printf("In impl_OP_Column(): no_mem.\n");
#     assert(0);
#   }

# op_column_error:
#   UPDATE_MAX_BLOBSIZE(pDest);
#   REGISTER_TRACE(pOp->p3, pDest);

#   return rc;
# }

