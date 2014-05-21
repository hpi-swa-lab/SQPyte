from rpython.rtyper.tool import rffi_platform as platform
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.translator.tool.cbuild import ExternalCompilationInfo
import sys

class CConfig:
    _compilation_info_ = ExternalCompilationInfo(
        includes = ['sqlite3.h', 'stdint.h', 'sqliteInt.h'],
        libraries = ['sqlite3'],
        include_dirs = ['/Users/dkurilov/workspace/sqlite/src/', '/Users/dkurilov/workspace/sqlite/'],
    )


    u8  = platform.SimpleType('uint8_t', rffi.UCHAR)
    i8  = platform.SimpleType('int8_t', rffi.CHAR)
    u16 = platform.SimpleType('uint16_t', rffi.USHORT)
    i16 = platform.SimpleType('int16_t', rffi.SHORT)
    u32 = platform.SimpleType('uint32_t', rffi.UINT)
    i32 = platform.SimpleType('int32_t', rffi.INT)


p4names = ['P4_INT32']

for name in p4names:
	setattr(CConfig, name, platform.DefinedConstantInteger(name))


CConfig.__dict__.update(platform.configure(CConfig))


for name in p4names:
	setattr(CConfig, name, chr(256 + getattr(CConfig, name)))

VDBEOP = lltype.Struct("VdbeOp",
	("opcode", rffi.UCHAR),
	("p4type", rffi.CHAR),
	("opflags", rffi.UCHAR),
	("p5", rffi.UCHAR),
	("p1", rffi.INT),
	("p2", rffi.INT),
	("p3", rffi.INT),
	("p4", lltype.Struct("p4",
		("i", rffi.INT),
		("pReal", rffi.DOUBLE),
		hints={"union": True})),

	)

VDBE = lltype.Struct("Vdbe",
	("db", rffi.VOIDP),
	("aOp", lltype.Ptr(lltype.Array(VDBEOP, hints={'nolength': True}))),
	("aMem", rffi.VOIDP),
	("apArg", rffi.VOIDPP),
	("aColName", rffi.VOIDP),
	("pResultSet", rffi.VOIDP),
	("pParse", rffi.VOIDP),
	("nMen", rffi.INT),
	("nOp", rffi.INT),
	("nOpAlloc", rffi.INT),
	("nLabel", rffi.INT),
	("aLabel", rffi.INTP)
	)

KEYINFO = lltype.Struct("KeyInfo",
	("nRef", CConfig.u32),
	("enc", CConfig.u8),
	("nField", rffi.UCHAR),
	("db", rffi.VOIDP),
	("aSortOrder", rffi.CCHARP),
	("aColl", rffi.VOIDP)
	)

# struct KeyInfo {
#   u32 nRef;           /* Number of references to this KeyInfo object */
#   u8 enc;             /* Text encoding - one of the SQLITE_UTF* values */
#   u16 nField;         /* Number of key columns in the index */
#   u16 nXField;        /* Number of columns beyond the key columns */
#   sqlite3 *db;        /* The database connection */
#   u8 *aSortOrder;     /* Sort order for each column. */
#   CollSeq *aColl[1];  /* Collating sequence for each term of the key */
# };

VDBECURSOR = lltype.Struct("Vdbe",
	("pCursor", rffi.VOIDP),
	("pBt", rffi.VOIDP),
	("pKeyInfo", KEYINFO),
	("seekResult", rffi.INT),
	("pseudoTableReg", rffi.INT),
	("nField", rffi.INT),
	("nHdrParsed", rffi.UCHAR),
	("iDb", rffi.CHAR),
	("nullRow", rffi.CHAR)
	)

#  struct VdbeCursor {
#   BtCursor *pCursor;    /* The cursor structure of the backend */
#   Btree *pBt;           /* Separate file holding temporary table */
#   KeyInfo *pKeyInfo;    /* Info about index keys needed by index cursors */
#   int seekResult;       /* Result of previous sqlite3BtreeMoveto() */
#   int pseudoTableReg;   /* Register holding pseudotable content. */
#   i16 nField;           /* Number of fields in the header */
#   u16 nHdrParsed;       /* Number of header fields parsed so far */
#   i8 iDb;               /* Index of cursor database in db->aDb[] (or -1) */
#   u8 nullRow;           /* True if pointing to a row with no data */
#   u8 rowidIsValid;      /* True if lastRowid is valid */
#   u8 deferredMoveto;    /* A call to sqlite3BtreeMoveto() is needed */
#   Bool isEphemeral:1;   /* True for an ephemeral table */
#   Bool useRandomRowid:1;/* Generate new record numbers semi-randomly */
#   Bool isTable:1;       /* True if a table requiring integer keys */
#   Bool isOrdered:1;     /* True if the underlying table is BTREE_UNORDERED */
#   sqlite3_vtab_cursor *pVtabCursor;  /* The cursor for a virtual table */
#   i64 seqCount;         /* Sequence counter */
#   i64 movetoTarget;     /* Argument to the deferred sqlite3BtreeMoveto() */
#   i64 lastRowid;        /* Rowid being deleted by OP_Delete */
#   VdbeSorter *pSorter;  /* Sorter object for OP_SorterOpen cursors */

#   /* Cached information about the header for the data record that the
#   ** cursor is currently pointing to.  Only valid if cacheStatus matches
#   ** Vdbe.cacheCtr.  Vdbe.cacheCtr will never take on the value of
#   ** CACHE_STALE and so setting cacheStatus=CACHE_STALE guarantees that
#   ** the cache is out of date.
#   **
#   ** aRow might point to (ephemeral) data for the current row, or it might
#   ** be NULL.
#   */
#   u32 cacheStatus;      /* Cache is valid if this matches Vdbe.cacheCtr */
#   u32 payloadSize;      /* Total number of bytes in the record */
#   u32 szRow;            /* Byte available in aRow */
#   u32 iHdrOffset;       /* Offset to next unparsed byte of the header */
#   const u8 *aRow;       /* Data for the current row, if all on one page */
#   u32 aType[1];         /* Type values for all entries in the record */
#   /* 2*nField extra array elements allocated for aType[], beyond the one
#   ** static element declared in the structure.  nField total array slots for
#   ** aType[] and nField+1 array slots for aOffset[] */
# };


VDBEP = lltype.Ptr(VDBE)

sqlite3_open = rffi.llexternal('sqlite3_open', [rffi.CCHARP, rffi.VOIDPP],
							   rffi.INT, compilation_info=CConfig._compilation_info_)
sqlite3_prepare = rffi.llexternal('sqlite3_prepare', [rffi.VOIDP, rffi.CCHARP, rffi.INT, rffi.VOIDPP, rffi.CCHARPP],
								  rffi.INT, compilation_info=CConfig._compilation_info_)
sqlite3_allocateCursor = rffi.llexternal('allocateCursor', [lltype.Ptr(VDBE), rffi.INT, rffi.INT, rffi.INT, rffi.INT],
	rffi.VOIDP, compilation_info=CConfig._compilation_info_)

def opendb(db_name):
	with rffi.scoped_str2charp(db_name) as db_name, lltype.scoped_alloc(rffi.VOIDPP.TO, 1) as result:
		errorcode = sqlite3_open(db_name, result)
		assert errorcode == 0
		return result[0]

def prepare(db, query):
	length = len(query)
	with rffi.scoped_str2charp(query) as query, lltype.scoped_alloc(rffi.VOIDPP.TO, 1) as result, lltype.scoped_alloc(rffi.CCHARPP.TO, 1) as unused_buffer:
		errorcode = sqlite3_prepare(db, query, length, result, unused_buffer)
		assert errorcode == 0
		return rffi.cast(VDBEP, result[0])

def allocateCursor(vdbe_struct):
	vdbeCursor = sqlite3_allocateCursor()	


def mainloop(vdbe_struct):
	pc = 0
	OPFLAG_BULKCSR = hex(01)
	OPFLAG_P2ISREG = hex(02)
	length = vdbe_struct.nOp
	ops = vdbe_struct.aOp

	while pc < length:
		pOp = ops[pc]
		print ops[pc].opcode

		if pOp.opcode == 115:
			if pOp.p2:
				pc = pOp.p2 - 1
		elif pOp.opcode == 52 or pOp.opcode == 53: # 52   OpenRead; 53    OpenWrite
			nField = 0
			pKeyInfo = 0
			p2 = pOp.p2;
			iDb = pOp.p3
			if pOp.p5 and OPFLAG_P2ISREG:
				assert(p2 > 2)
			assert(pOp.p4type == CConfig.P4_INT32)
			nField = pOp.p4.i
			# assert(pOp.p1 >= 0)
			# assert(nField >= 0)
		pc += 1

	return pc


  # int nField;
  # KeyInfo *pKeyInfo;
  # int p2;
  # int iDb;
  # int wrFlag;
  # Btree *pX;
  # VdbeCursor *pCur;
  # Db *pDb;

  
  # pCur = allocateCursor(p, pOp->p1, nField, iDb, 1);
  # if( pCur==0 ) goto no_mem;
  # pCur->nullRow = 1;
  # pCur->isOrdered = 1;
  # rc = sqlite3BtreeCursor(pX, p2, wrFlag, pKeyInfo, pCur->pCursor);
  # pCur->pKeyInfo = pKeyInfo;
  # assert( OPFLAG_BULKCSR==BTREE_BULKLOAD );
  # sqlite3BtreeCursorHints(pCur->pCursor, (pOp->p5 & OPFLAG_BULKCSR));

  # /* Since it performs no memory allocation or IO, the only value that
  # ** sqlite3BtreeCursor() may return is SQLITE_OK. */
  # assert( rc==SQLITE_OK );

  # /* Set the VdbeCursor.isTable variable. Previous versions of
  # ** SQLite used to check if the root-page flags were sane at this point
  # ** and report database corruption if they were not, but this check has
  # ** since moved into the btree layer.  */  
  # pCur->isTable = pOp->p4type!=P4_KEYINFO;
  # break;


# db = opendb('test.db')
# mainloop(prepare(db, 'select * from contacts;'))


	

