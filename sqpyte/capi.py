from rpython.rtyper.tool import rffi_platform as platform
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.translator.tool.cbuild import ExternalCompilationInfo

class CConfig:
    _compilation_info_ = ExternalCompilationInfo(
        includes = ['sqlite3.h', 'stdint.h', 'sqliteInt.h'],
        libraries = ['sqlite3'],
        library_dirs = ['/Users/dkurilov/workspace/sqlite/'],
        include_dirs = ['/Users/dkurilov/workspace/sqlite/src/', '/Users/dkurilov/workspace/sqlite/'],
    )

    u8  = platform.SimpleType('uint8_t', rffi.UCHAR)
    i8  = platform.SimpleType('int8_t', rffi.CHAR)
    u16 = platform.SimpleType('uint16_t', rffi.USHORT)
    i16 = platform.SimpleType('int16_t', rffi.SHORT)
    u32 = platform.SimpleType('uint32_t', rffi.UINT)
    i32 = platform.SimpleType('int32_t', rffi.INT)
    u64 = platform.SimpleType('uint64_t', rffi.ULONGLONG)
    i64 = platform.SimpleType('int64_t', rffi.LONGLONG)

opnames = ['OP_Init', 'OP_OpenRead', 'OP_OpenWrite']
p4names = ['P4_INT32', 'P4_KEYINFO']
p5flags = ['OPFLAG_P2ISREG', 'OPFLAG_BULKCSR']
result_codes = ['SQLITE_OK', 'SQLITE_ABORT']

for name in p4names + opnames + p5flags + result_codes:
	setattr(CConfig, name, platform.DefinedConstantInteger(name))

CConfig.__dict__.update(platform.configure(CConfig))

for name in p4names:
	setattr(CConfig, name, chr(256 + getattr(CConfig, name)))

SQLITE3 = lltype.ForwardReference()
SQLITE3P = lltype.Ptr(SQLITE3)
SQLITE3PP = rffi.CArrayPtr(SQLITE3P)
VDBE = lltype.ForwardReference()
VDBEP = lltype.Ptr(VDBE)
VDBECURSOR = lltype.ForwardReference()
VDBECURSORP = lltype.Ptr(VDBECURSOR)
VDBECURSORPP = rffi.CArrayPtr(VDBECURSORP)
# MEM = lltype.ForwardReference()
# MEMP = rffi.CArrayPtr(MEM)
# MEMPP = rffi.CArrayPtr(MEMP)

VDBEOP = lltype.Struct("VdbeOp",
	("opcode", CConfig.u8),
	("p4type", rffi.CHAR),
	("opflags", CConfig.u8),
	("p5", CConfig.u8),
	("p1", rffi.INT),
	("p2", rffi.INT),
	("p3", rffi.INT),
	("p4", lltype.Struct("p4",
		("i", rffi.INT),
		("pReal", rffi.DOUBLE),
		hints={"union": True}))
	)

# struct VdbeOp {
#   u8 opcode;          /* What operation to perform */
#   signed char p4type; /* One of the P4_xxx constants for p4 */
#   u8 opflags;         /* Mask of the OPFLG_* flags in opcodes.h */
#   u8 p5;              /* Fifth parameter is an unsigned character */
#   int p1;             /* First operand */
#   int p2;             /* Second parameter (often the jump destination) */
#   int p3;             /* The third parameter */
#   union {             /* fourth parameter */
#     int i;                 /* Integer value if p4type==P4_INT32 */
#     void *p;               /* Generic pointer */
#     char *z;               /* Pointer to data for string (char array) types */
#     i64 *pI64;             /* Used when p4type is P4_INT64 */
#     double *pReal;         /* Used when p4type is P4_REAL */
#     FuncDef *pFunc;        /* Used when p4type is P4_FUNCDEF */
#     CollSeq *pColl;        /* Used when p4type is P4_COLLSEQ */
#     Mem *pMem;             /* Used when p4type is P4_MEM */
#     VTable *pVtab;         /* Used when p4type is P4_VTAB */
#     KeyInfo *pKeyInfo;     /* Used when p4type is P4_KEYINFO */
#     int *ai;               /* Used when p4type is P4_INTARRAY */
#     SubProgram *pProgram;  /* Used when p4type is P4_SUBPROGRAM */
#     int (*xAdvance)(BtCursor *, int *);
#   } p4;
# #ifdef SQLITE_ENABLE_EXPLAIN_COMMENTS
#   char *zComment;          /* Comment to improve readability */
# #endif
# #ifdef VDBE_PROFILE
#   u32 cnt;                 /* Number of times this instruction was executed */
#   u64 cycles;              /* Total time spent executing this instruction */
# #endif
# #ifdef SQLITE_VDBE_COVERAGE
#   int iSrcLine;            /* Source-code line that generated this opcode */
# #endif
# };


SQLITE3.become(lltype.Struct("sqlite3",
	("pVfs", rffi.VOIDP),
	("pVdbe", lltype.Ptr(VDBE)),
	("pDfltColl", rffi.VOIDP),
	("mutex", rffi.VOIDP),
	("aDb", rffi.VOIDP),
	("nDb", rffi.INT),
	("flags", rffi.INT)
	))

# struct sqlite3 {
#   sqlite3_vfs *pVfs;            /* OS Interface */
#   struct Vdbe *pVdbe;           /* List of active virtual machines */
#   CollSeq *pDfltColl;           /* The default collating sequence (BINARY) */
#   sqlite3_mutex *mutex;         /* Connection mutex */
#   Db *aDb;                      /* All backends */
#   int nDb;                      /* Number of backends currently in use */
#   int flags;                    /* Miscellaneous flags. See below */
#   i64 lastRowid;                /* ROWID of most recent insert (see above) */
#   i64 szMmap;                   /* Default mmap_size setting */
#   unsigned int openFlags;       /* Flags passed to sqlite3_vfs.xOpen() */
#   int errCode;                  /* Most recent error code (SQLITE_*) */
#   int errMask;                  /* & result codes with this before returning */
#   u16 dbOptFlags;               /* Flags to enable/disable optimizations */
#   u8 autoCommit;                /* The auto-commit flag. */
#   u8 temp_store;                /* 1: file 2: memory 0: default */
#   u8 mallocFailed;              /* True if we have seen a malloc failure */
#   u8 dfltLockMode;              /* Default locking-mode for attached dbs */
#   signed char nextAutovac;      /* Autovac setting after VACUUM if >=0 */
#   u8 suppressErr;               /* Do not issue error messages if true */
#   u8 vtabOnConflict;            /* Value to return for s3_vtab_on_conflict() */
#   u8 isTransactionSavepoint;    /* True if the outermost savepoint is a TS */
#   int nextPagesize;             /* Pagesize after VACUUM if >0 */
#   u32 magic;                    /* Magic number for detect library misuse */
#   int nChange;                  /* Value returned by sqlite3_changes() */
#   int nTotalChange;             /* Value returned by sqlite3_total_changes() */
#   int aLimit[SQLITE_N_LIMIT];   /* Limits */
#   struct sqlite3InitInfo {      /* Information used during initialization */
#     int newTnum;                /* Rootpage of table being initialized */
#     u8 iDb;                     /* Which db file is being initialized */
#     u8 busy;                    /* TRUE if currently initializing */
#     u8 orphanTrigger;           /* Last statement is orphaned TEMP trigger */
#   } init;
#   int nVdbeActive;              /* Number of VDBEs currently running */
#   int nVdbeRead;                /* Number of active VDBEs that read or write */
#   int nVdbeWrite;               /* Number of active VDBEs that read and write */
#   int nVdbeExec;                /* Number of nested calls to VdbeExec() */
#   int nExtension;               /* Number of loaded extensions */
#   void **aExtension;            /* Array of shared library handles */
#   void (*xTrace)(void*,const char*);        /* Trace function */
#   void *pTraceArg;                          /* Argument to the trace function */
#   void (*xProfile)(void*,const char*,u64);  /* Profiling function */
#   void *pProfileArg;                        /* Argument to profile function */
#   void *pCommitArg;                 /* Argument to xCommitCallback() */   
#   int (*xCommitCallback)(void*);    /* Invoked at every commit. */
#   void *pRollbackArg;               /* Argument to xRollbackCallback() */   
#   void (*xRollbackCallback)(void*); /* Invoked at every commit. */
#   void *pUpdateArg;
#   void (*xUpdateCallback)(void*,int, const char*,const char*,sqlite_int64);
# #ifndef SQLITE_OMIT_WAL
#   int (*xWalCallback)(void *, sqlite3 *, const char *, int);
#   void *pWalArg;
# #endif
#   void(*xCollNeeded)(void*,sqlite3*,int eTextRep,const char*);
#   void(*xCollNeeded16)(void*,sqlite3*,int eTextRep,const void*);
#   void *pCollNeededArg;
#   sqlite3_value *pErr;          /* Most recent error message */
#   union {
#     volatile int isInterrupted; /* True if sqlite3_interrupt has been called */
#     double notUsed1;            /* Spacer */
#   } u1;
#   Lookaside lookaside;          /* Lookaside malloc configuration */
# #ifndef SQLITE_OMIT_AUTHORIZATION
#   int (*xAuth)(void*,int,const char*,const char*,const char*,const char*);
#                                 /* Access authorization function */
#   void *pAuthArg;               /* 1st argument to the access auth function */
# #endif
# #ifndef SQLITE_OMIT_PROGRESS_CALLBACK
#   int (*xProgress)(void *);     /* The progress callback */
#   void *pProgressArg;           /* Argument to the progress callback */
#   unsigned nProgressOps;        /* Number of opcodes for progress callback */
# #endif
# #ifndef SQLITE_OMIT_VIRTUALTABLE
#   int nVTrans;                  /* Allocated size of aVTrans */
#   Hash aModule;                 /* populated by sqlite3_create_module() */
#   VtabCtx *pVtabCtx;            /* Context for active vtab connect/create */
#   VTable **aVTrans;             /* Virtual tables with open transactions */
#   VTable *pDisconnect;    /* Disconnect these in next sqlite3_prepare() */
# #endif
#   FuncDefHash aFunc;            /* Hash table of connection functions */
#   Hash aCollSeq;                /* All collating sequences */
#   BusyHandler busyHandler;      /* Busy callback */
#   Db aDbStatic[2];              /* Static space for the 2 default backends */
#   Savepoint *pSavepoint;        /* List of active savepoints */
#   int busyTimeout;              /* Busy handler timeout, in msec */
#   int nSavepoint;               /* Number of non-transaction savepoints */
#   int nStatement;               /* Number of nested statement-transactions  */
#   i64 nDeferredCons;            /* Net deferred constraints this transaction. */
#   i64 nDeferredImmCons;         /* Net deferred immediate constraints */
#   int *pnBytesFreed;            /* If not NULL, increment this in DbFree() */

# #ifdef SQLITE_ENABLE_UNLOCK_NOTIFY
#   /* The following variables are all protected by the STATIC_MASTER 
#   ** mutex, not by sqlite3.mutex. They are used by code in notify.c. 
#   **
#   ** When X.pUnlockConnection==Y, that means that X is waiting for Y to
#   ** unlock so that it can proceed.
#   **
#   ** When X.pBlockingConnection==Y, that means that something that X tried
#   ** tried to do recently failed with an SQLITE_LOCKED error due to locks
#   ** held by Y.
#   */
#   sqlite3 *pBlockingConnection; /* Connection that caused SQLITE_LOCKED */
#   sqlite3 *pUnlockConnection;           /* Connection to watch for unlock */
#   void *pUnlockArg;                     /* Argument to xUnlockNotify */
#   void (*xUnlockNotify)(void **, int);  /* Unlock notify callback */
#   sqlite3 *pNextBlocked;        /* Next in list of all blocked connections */
# #endif
# };

MEM = lltype.Struct("Mem",
	("db", SQLITE3P),
	("z", rffi.CCHARP),
	("r", rffi.DOUBLE),
	("u", lltype.Struct("u",
	("i", CConfig.i64),
	("nZero", rffi.INT),
	hints={"union": True})),
	("n", rffi.INT),
	("flags", CConfig.u16),
	("enc", CConfig.u8),
	("xDel", rffi.VOIDP),
	("zMalloc", rffi.VOIDP)	
	)

# struct Mem {
#   sqlite3 *db;        /* The associated database connection */
#   char *z;            /* String or BLOB value */
#   double r;           /* Real value */
#   union {
#     i64 i;              /* Integer value used when MEM_Int is set in flags */
#     int nZero;          /* Used when bit MEM_Zero is set in flags */
#     FuncDef *pDef;      /* Used only when flags==MEM_Agg */
#     RowSet *pRowSet;    /* Used only when flags==MEM_RowSet */
#     VdbeFrame *pFrame;  /* Used when flags==MEM_Frame */
#   } u;
#   int n;              /* Number of characters in string value, excluding '\0' */
#   u16 flags;          /* Some combination of MEM_Null, MEM_Str, MEM_Dyn, etc. */
#   u8  enc;            /* SQLITE_UTF8, SQLITE_UTF16BE, SQLITE_UTF16LE */
# #ifdef SQLITE_DEBUG
#   Mem *pScopyFrom;    /* This Mem is a shallow copy of pScopyFrom */
#   void *pFiller;      /* So that sizeof(Mem) is a multiple of 8 */
# #endif
#   void (*xDel)(void *);  /* If not null, call this function to delete Mem.z */
#   char *zMalloc;      /* Dynamic buffer allocated by sqlite3_malloc() */
# };


VDBE.become(lltype.Struct("Vdbe",
	("db", SQLITE3P),
	("aOp", lltype.Ptr(lltype.Array(VDBEOP, hints={'nolength': True}))),
	("aMem", rffi.CArrayPtr(MEM)),
	("apArg", rffi.VOIDPP),
	("aColName", rffi.VOIDP),
	("pResultSet", rffi.VOIDP),
	("pParse", rffi.VOIDP),
	("nMen", rffi.INT),
	("nOp", rffi.INT),
	("nCursor", rffi.INT),
	("magic", CConfig.u32),
	("zErrMsg", rffi.CCHARP),
	("pPrev", VDBEP),
	("pNext", VDBEP),
	("apCsr", VDBECURSORPP),
	("aVar", rffi.VOIDP),
	("azVar", rffi.CCHARPP),
	("nVar", rffi.INT),
	("nzVar", rffi.INT),
	("cacheCtr", CConfig.u32)
	))

# struct Vdbe {
#   sqlite3 *db;            /* The database connection that owns this statement */
#   Op *aOp;                /* Space to hold the virtual machine's program */
#   Mem *aMem;              /* The memory locations */
#   Mem **apArg;            /* Arguments to currently executing user function */
#   Mem *aColName;          /* Column names to return */
#   Mem *pResultSet;        /* Pointer to an array of results */
#   Parse *pParse;          /* Parsing context used to create this Vdbe */
#   int nMem;               /* Number of memory locations currently allocated */
#   int nOp;                /* Number of instructions in the program */
#   int nCursor;            /* Number of slots in apCsr[] */
#   u32 magic;              /* Magic number for sanity checking */
#   char *zErrMsg;          /* Error message written here */
#   Vdbe *pPrev,*pNext;     /* Linked list of VDBEs with the same Vdbe.db */
#   VdbeCursor **apCsr;     /* One element of this array for each open cursor */
#   Mem *aVar;              /* Values for the OP_Variable opcode. */
#   char **azVar;           /* Name of variables */
#   ynVar nVar;             /* Number of entries in aVar[] */
#   ynVar nzVar;            /* Number of entries in azVar[] */
#   u32 cacheCtr;           /* VdbeCursor row cache generation counter */
#   int pc;                 /* The program counter */
#   int rc;                 /* Value to return */
#   u16 nResColumn;         /* Number of columns in one row of the result set */
#   u8 errorAction;         /* Recovery action to do in case of an error */
#   u8 minWriteFileFormat;  /* Minimum file format for writable database files */
#   bft explain:2;          /* True if EXPLAIN present on SQL command */
#   bft inVtabMethod:2;     /* See comments above */
#   bft changeCntOn:1;      /* True to update the change-counter */
#   bft expired:1;          /* True if the VM needs to be recompiled */
#   bft runOnlyOnce:1;      /* Automatically expire on reset */
#   bft usesStmtJournal:1;  /* True if uses a statement journal */
#   bft readOnly:1;         /* True for statements that do not write */
#   bft bIsReader:1;        /* True for statements that read */
#   bft isPrepareV2:1;      /* True if prepared with prepare_v2() */
#   bft doingRerun:1;       /* True if rerunning after an auto-reprepare */
#   int nChange;            /* Number of db changes made since last reset */
#   yDbMask btreeMask;      /* Bitmask of db->aDb[] entries referenced */
#   yDbMask lockMask;       /* Subset of btreeMask that requires a lock */
#   int iStatement;         /* Statement number (or 0 if has not opened stmt) */
#   u32 aCounter[5];        /* Counters used by sqlite3_stmt_status() */
# #ifndef SQLITE_OMIT_TRACE
#   i64 startTime;          /* Time when query started - used for profiling */
# #endif
#   i64 iCurrentTime;       /* Value of julianday('now') for this statement */
#   i64 nFkConstraint;      /* Number of imm. FK constraints this VM */
#   i64 nStmtDefCons;       /* Number of def. constraints when stmt started */
#   i64 nStmtDefImmCons;    /* Number of def. imm constraints when stmt started */
#   char *zSql;             /* Text of the SQL statement that generated this */
#   void *pFree;            /* Free this when deleting the vdbe */
# #ifdef SQLITE_ENABLE_TREE_EXPLAIN
#   Explain *pExplain;      /* The explainer */
#   char *zExplain;         /* Explanation of data structures */
# #endif
#   VdbeFrame *pFrame;      /* Parent frame */
#   VdbeFrame *pDelFrame;   /* List of frame objects to free on VM reset */
#   int nFrame;             /* Number of frames in pFrame list */
#   u32 expmask;            /* Binding to these vars invalidates VM */
#   SubProgram *pProgram;   /* Linked list of all sub-programs used by VM */
#   int nOnceFlag;          /* Size of array aOnceFlag[] */
#   u8 *aOnceFlag;          /* Flags for OP_Once */
#   AuxData *pAuxData;      /* Linked list of auxdata allocations */
# };

KEYINFO = lltype.Struct("KeyInfo",
	("nRef", CConfig.u32),
	("enc", CConfig.u8),
	("nField", CConfig.u16),
	("nXField", CConfig.u16),
	("db", SQLITE3),
	("aSortOrder", CConfig.u8),
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

VDBECURSOR.become(lltype.Struct("Vdbe",
	("pCursor", rffi.VOIDP),
	("pBt", rffi.VOIDP),
	("pKeyInfo", KEYINFO),
	("seekResult", rffi.INT),
	("pseudoTableReg", rffi.INT),
	("nField", rffi.INT),
	("nHdrParsed", rffi.UCHAR),
	("iDb", rffi.CHAR),
	("nullRow", rffi.CHAR)
	))

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



sqlite3_open = rffi.llexternal('sqlite3_open', [rffi.CCHARP, SQLITE3PP],
							   rffi.INT, compilation_info=CConfig._compilation_info_)
sqlite3_prepare = rffi.llexternal('sqlite3_prepare', [rffi.VOIDP, rffi.CCHARP, rffi.INT, rffi.VOIDPP, rffi.CCHARPP],
								  rffi.INT, compilation_info=CConfig._compilation_info_)
sqlite3_allocateCursor = rffi.llexternal('allocateCursor', [lltype.Ptr(VDBE), rffi.INT, rffi.INT, rffi.INT, rffi.INT],
	rffi.VOIDP, compilation_info=CConfig._compilation_info_)
sqlite3_sqlite3VdbeMemIntegerify = rffi.llexternal('sqlite3VdbeMemIntegerify', [lltype.Ptr(MEM)],
	rffi.INT, compilation_info=CConfig._compilation_info_)
