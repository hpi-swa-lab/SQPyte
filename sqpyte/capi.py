import os
import sys
from rpython.rtyper.tool import rffi_platform as platform
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.translator.tool.cbuild import ExternalCompilationInfo


sqlite_inst_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "sqlite-install")
sqlite_src_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "sqlite")

lib_dir = os.path.join(sqlite_inst_dir, "lib")
if os.environ.has_key("LD_LIBRARY_PATH"):
    os.environ["LD_LIBRARY_PATH"] = "%s:%s" % (lib_dir, os.environ["LD_LIBRARY_PATH"])
else:
    os.environ["LD_LIBRARY_PATH"] = lib_dir

class CConfig:
    _compilation_info_ = ExternalCompilationInfo(
        includes = ['sqlite3.h', 'stdint.h', 'sqliteInt.h', 'btreeInt.h', 'sqpyte.h'],
        libraries = ['sqlite3', 'dl'],
        library_dirs = [os.path.join(sqlite_inst_dir, "lib")],
        include_dirs = [sqlite_src_dir, os.path.join(sqlite_src_dir, "src")],
        link_files = [os.path.join(sqlite_src_dir, "sqlite3.o")]
    )

    u8  = platform.SimpleType('uint8_t', rffi.UCHAR)
    i8  = platform.SimpleType('int8_t', rffi.CHAR)
    u16 = platform.SimpleType('uint16_t', rffi.USHORT)
    i16 = platform.SimpleType('int16_t', rffi.SHORT)
    u32 = platform.SimpleType('uint32_t', rffi.UINT)
    i32 = platform.SimpleType('int32_t', rffi.INT)
    u64 = platform.SimpleType('uint64_t', rffi.ULONGLONG)
    i64 = platform.SimpleType('int64_t', rffi.LONGLONG)
    ynVar = platform.SimpleType('ynVar', rffi.SHORT)

opnames = ['OP_Init', 'OP_OpenRead', 'OP_OpenWrite', 'OP_Rewind', 
           'OP_Transaction', 'OP_TableLock', 'OP_Goto', 'OP_Column',
           'OP_ResultRow', 'OP_Next', 'OP_Close', 'OP_Halt',
           'OP_Eq', 'OP_Ne', 'OP_Lt', 'OP_Le', 'OP_Gt', 'OP_Ge',
           'OP_Integer', 'OP_Null', 'OP_AggStep', 'OP_AggFinal',
           'OP_Copy', 'OP_MustBeInt', 'OP_NotExists', 'OP_String',
           'OP_String8', 'OP_Function', 'OP_Real', 'OP_RealAffinity',
           'OP_Add', 'OP_Subtract', 'OP_Multiply', 'OP_Divide', 'OP_Remainder',
           'OP_If', 'OP_IfNot', 'OP_Rowid', 'OP_IsNull',
           'OP_SeekLT', 'OP_SeekLE', 'OP_SeekGE', 'OP_SeekGT',
           'OP_Move', 'OP_IfZero', 'OP_IdxRowid',
           'OP_IdxLE', 'OP_IdxGT', 'OP_IdxLT', 'OP_IdxGE',
           'OP_Seek', 'OP_Once', 'OP_SCopy', 'OP_Affinity',
           'OP_OpenAutoindex', 'OP_OpenEphemeral', 'OP_MakeRecord',
           'OP_SorterInsert', 'OP_IdxInsert',
           'OP_NoConflict', 'OP_NotFound', 'OP_Found', 'OP_RowSetTest',
           'OP_Gosub', 'OP_Return', 'OP_SorterOpen', 'OP_NextIfOpen',
           'OP_Sequence', 'OP_OpenPseudo', 'OP_SorterSort', 'OP_Sort',
           'OP_SorterData', 'OP_SorterNext', 'OP_Noop', 'OP_Explain',
           'OP_Compare', 'OP_Jump', 'OP_IfPos', 'OP_CollSeq', 'OP_NotNull',
           'OP_InitCoroutine', 'OP_Yield', 'OP_NullRow', 'OP_EndCoroutine',
           'OP_ReadCookie', 'OP_NewRowid', 'OP_Insert', 'OP_InsertInt',
           'OP_SetCookie', 'OP_ParseSchema', 'OP_RowSetAdd', 'OP_RowSetRead',
           'OP_Delete', 'OP_DropTable', 'OP_RowKey', 'OP_RowData', 'OP_Blob',
           'OP_Cast', 'OP_Concat', 'OP_Variable']
p4names = ['P4_INT32', 'P4_KEYINFO', 'P4_COLLSEQ', 'P4_FUNCDEF']
p5flags = ['OPFLAG_P2ISREG', 'OPFLAG_BULKCSR', 'OPFLAG_CLEARCACHE', 'OPFLAG_LENGTHARG', 'OPFLAG_TYPEOFARG', 'OPFLG_OUT2_PRERELEASE', 'OPFLAG_PERMUTE']
result_codes = ['SQLITE_OK', 'SQLITE_ABORT', 'SQLITE_N_LIMIT', 'SQLITE_DONE', 'SQLITE_ROW', 'SQLITE_BUSY', 'SQLITE_CORRUPT_BKPT', 'SQLITE_NOMEM']
sqlite_codes = ['SQLITE_NULLEQ', 'SQLITE_JUMPIFNULL', 'SQLITE_STOREP2', 'SQLITE_AFF_MASK', 'SQLITE_FUNC_NEEDCOLL']
mem_codes = ['SQLITE_STATIC', 'SQLITE_TRANSIENT']
affinity_codes = ['SQLITE_AFF_TEXT', 'SQLITE_AFF_NONE', 'SQLITE_AFF_INTEGER', 'SQLITE_AFF_REAL', 'SQLITE_AFF_NUMERIC']
btree_values = ['BTCURSOR_MAX_DEPTH', 'BTREE_BULKLOAD']
other_constants = ['SQLITE_MAX_VARIABLE_NUMBER', 'CACHE_STALE', 'SQLITE_LIMIT_LENGTH', 'CURSOR_VALID', 'SAVEPOINT_RELEASE',
                   'SQLITE_STMTSTATUS_VM_STEP', 'VDBE_MAGIC_RUN']
encodings = ['SQLITE_UTF8']
memValues = ['MEM_Null', 'MEM_Real', 'MEM_Cleared', 'MEM_TypeMask', 'MEM_Zero',
             'MEM_Int', 'MEM_Str', 'MEM_RowSet', 'MEM_Blob', 'MEM_Agg',
             'MEM_Dyn', 'MEM_Frame', 'MEM_Ephem', 'MEM_Static',
             'MEM_Undefined', 'MEM_AffMask', 'MEM_Term']
sqlite_types = [
    'SQLITE_INTEGER',
    'SQLITE_FLOAT',
    'SQLITE_BLOB',
    'SQLITE_NULL',
    'SQLITE_TEXT',
]

for name in (p4names + opnames + p5flags + result_codes + sqlite_codes +
             btree_values + other_constants + memValues + affinity_codes +
             encodings + sqlite_types + mem_codes):
    setattr(CConfig, name, platform.DefinedConstantInteger(name))


CConfig.__dict__.update(platform.configure(CConfig))

for name in p4names:
    setattr(CConfig, name, chr(256 + getattr(CConfig, name)))

for name in memValues:
    setattr(CConfig, name, rffi.cast(lltype.Unsigned, getattr(CConfig, name)))

opnames_dict = {}
for name in opnames:
    opnames_dict[getattr(CConfig, name)] = name

assert CConfig.SQLITE_MAX_VARIABLE_NUMBER < 32767

U8P = lltype.Ptr(lltype.Array(CConfig.u8, hints={'nolength': True}))

SQLITE3 = lltype.ForwardReference()
SQLITE3P = lltype.Ptr(SQLITE3)
SQLITE3PP = rffi.CArrayPtr(SQLITE3P)
VDBE = lltype.ForwardReference()
VDBEP = lltype.Ptr(VDBE)
VDBECURSOR = lltype.ForwardReference()
VDBECURSORP = lltype.Ptr(VDBECURSOR)
VDBECURSORPP = rffi.CArrayPtr(VDBECURSORP)
BTREE = lltype.ForwardReference()
BTREEP = lltype.Ptr(BTREE)
BTCURSOR = lltype.ForwardReference()
BTCURSORP = lltype.Ptr(BTCURSOR)
FUNCDEF = lltype.ForwardReference()
FUNCDEFP = lltype.Ptr(FUNCDEF)
CONTEXT = lltype.ForwardReference()
CONTEXTP = lltype.Ptr(CONTEXT)

HASH = lltype.Struct("Hash",                # src/hash.h: 43
    ("htsize", rffi.UINT),                  # Number of buckets in the hash table
    ("count", rffi.UINT),                   # Number of entries in this table
    ("first", rffi.VOIDP),                  #   HashElem *first;          /* The first element of the array */
    ("ht", lltype.Ptr(lltype.Struct("_ht",  # the hash table
        ("count", rffi.INT),                # Number of entries with this hash
        ("chain", rffi.VOIDP))))            #     HashElem *chain;           /* Pointer to first entry with this hash */
    )


SCHEMA = lltype.Struct("Schema",    # src/sqliteInt.h: 869
    ("schema_cookie", rffi.INT),    # Database schema version number for this file
    ("iGeneration", rffi.INT),      # Generation counter.  Incremented with each change
    ("tblHash", HASH),              # All tables indexed by name
    ("idxHash", HASH),              # All (named) indices indexed by name
    ("trigHash", HASH),             # All triggers indexed by name
    ("fkeyHash", HASH),             # All foreign keys by referenced table name
    ("pSeqTab", rffi.VOIDP),        #   Table *pSeqTab;      /* The sqlite_sequence table used by AUTOINCREMENT */
    ("file_format", CConfig.u8),    # Schema format version for this file
    ("enc", CConfig.u8),            # Text encoding used by this database
    ("flags", CConfig.u16),         # Flags associated with this schema
    ("cache_size", rffi.INT)        # Number of pages to use in the cache
    )
SCHEMAP = lltype.Ptr(SCHEMA)


SQLITE3_MUTEX = lltype.Struct("sqlite3_mutex",  # src/mutex_unix.c: 41
    ("mutex", rffi.INT)                         #   pthread_mutex_t mutex;     /* Mutex controlling the lock */
    # #if SQLITE_MUTEX_NREF
    #   int id;                    /* Mutex type */
    #   volatile int nRef;         /* Number of entrances */
    #   volatile pthread_t owner;  /* Thread that is within this mutex */
    #   int trace;                 /* True to trace changes */
    # #endif
    )


BTSHARED = lltype.Struct("BtShared",    # src/btreeInt.h: 406
    ("pPager", rffi.VOIDP),             # The page cache
    ("db", SQLITE3P),                   # Database connection currently using this Btree
    ("pCursor", BTCURSORP),             # A list of all open cursors
    ("pPage1", rffi.VOIDP),             # First page of the database
    ("openFlags", CConfig.u8),          # Flags to sqlite3BtreeOpen()
    # #ifndef SQLITE_OMIT_AUTOVACUUM
    #   u8 autoVacuum;        /* True if auto-vacuum is enabled */
    #   u8 incrVacuum;        /* True if incr-vacuum is enabled */
    #   u8 bDoTruncate;       /* True to truncate db on commit */
    # #endif
    ("inTransaction", CConfig.u8),      # Transaction state
    ("max1bytePayload", CConfig.u8),    # Maximum first byte of cell for a 1-byte payload
    ("btsFlags", CConfig.u16),          # Boolean parameters.  See BTS_* macros below
    ("maxLocal", CConfig.u16),          # Maximum local payload in non-LEAFDATA tables
    ("minLocal", CConfig.u16),          # Minimum local payload in non-LEAFDATA tables
    ("maxLeaf", CConfig.u16),           # Maximum local payload in a LEAFDATA table
    ("minLeaf", CConfig.u16),           # Minimum local payload in a LEAFDATA table
    ("pageSize", CConfig.u32),          # Total number of bytes on a page
    ("usableSize", CConfig.u32),        # Number of usable bytes on each page
    ("nTransaction", rffi.INT),         # Number of open transactions (read + write)
    ("nPage", CConfig.u32),             # Number of pages in the database
    ("pSchema", rffi.VOIDP),            # Pointer to space allocated by sqlite3BtreeSchema()
    ("xFreeSchema", rffi.VOIDP),        #   void (*xFreeSchema)(void*);  /* Destructor for BtShared.pSchema */
    ("mutex", rffi.VOIDP),              #   sqlite3_mutex *mutex; /* Non-recursive mutex required to access this object */
    ("pHasContent", rffi.VOIDP),        #   Bitvec *pHasContent;  /* Set of pages moved to free-list this transaction */
    # #ifndef SQLITE_OMIT_SHARED_CACHE
    #   int nRef;             /* Number of references to this structure */
    #   BtShared *pNext;      /* Next on a list of sharable BtShared structs */
    #   BtLock *pLock;        /* List of locks held on this shared-btree struct */
    #   Btree *pWriter;       /* Btree with currently open write transaction */
    # #endif
    ("pTmpSpace", rffi.UCHARP)          # BtShared.pageSize bytes of space for tmp use
    )
BTSHAREDP = lltype.Ptr(BTSHARED)


BTREE.become(lltype.Struct("Btree",     # src/btreeInt.h: 345
    ("db", SQLITE3P),                   # The database connection holding this btree
    ("pBt", BTSHAREDP),                 # Sharable content of this btree
    ("inTrans", CConfig.u8),            # TRANS_NONE, TRANS_READ or TRANS_WRITE
    ("sharable", CConfig.u8),           # True if we can share pBt with another db
    ("locked", CConfig.u8),             # True if db currently has pBt locked
    ("wantToLock", rffi.INT),           # Number of nested calls to sqlite3BtreeEnter()
    ("nBackup", rffi.INT),              # Number of backup operations reading this btree
    ("pNext", BTREEP),                  # List of other sharable Btrees from the same db
    ("pPrev", BTREEP)                   # Back pointer of the same list
    # #ifndef SQLITE_OMIT_SHARED_CACHE
    #   BtLock lock;       /* Object used to lock page 1 */
    # #endif
    ))


DB = lltype.Struct("Db",            # src/sqliteInt.h: 845
    ("zName", rffi.CCHARP),         # Name of this database
    ("pBt", BTREEP),                # The B*Tree structure for this database file
    ("safety_level", CConfig.u8),   # How aggressive at syncing data to disk
    ("pSchema", SCHEMAP)            # Pointer to database schema (possibly shared)
    )
DBP = lltype.Ptr(DB)


COLLSEQ = lltype.Struct("CollSeq",  # src/sqliteInt.h: 1326
    ("zName", rffi.CCHARP),         # Name of the collating sequence, UTF-8 encoded
    ("enc", CConfig.u8),            # Text encoding handled by xCmp()
    ("pUser", rffi.VOIDP),          # First argument to xCmp()
    ("xCmp", rffi.INTP),            #   int (*xCmp)(void*,int, const void*, int, const void*);
    ("xDel", rffi.VOIDP)            #   void (*xDel)(void*);  /* Destructor for pUser */
    )
COLLSEQP = lltype.Ptr(COLLSEQ)


SQLITE3.become(lltype.Struct("sqlite3",         # src/sqliteInt.h: 960
    ("pVfs", rffi.VOIDP),                       #   sqlite3_vfs *pVfs;            /* OS Interface */
    ("pVdbe", VDBEP),                           # List of active virtual machines
    ("pDfltColl", COLLSEQP),                    # The default collating sequence (BINARY)
    ("mutex", rffi.VOIDP),                      #   sqlite3_mutex *mutex;         /* Connection mutex */
    ("aDb", lltype.Ptr(lltype.Array(DB,         # All backends
        hints={'nolength': True}))),
    ("nDb", rffi.INT),                          # Number of backends currently in use
    ("flags", rffi.INT),                        # Miscellaneous flags. See below
    ("lastRowid", CConfig.i64),                 # ROWID of most recent insert (see above)
    ("szMmap", CConfig.i64),                    # Default mmap_size setting
    ("openFlags", rffi.UINT),                   # Flags passed to sqlite3_vfs.xOpen()
    ("errCode", rffi.INT),                      # Most recent error code (SQLITE_*)
    ("errMask", rffi.INT),                      # & result codes with this before returning
    ("dbOptFlags", CConfig.u16),                # Flags to enable/disable optimizations
    ("enc", CConfig.u8),                        # Text encoding.
    ("autoCommit", CConfig.u8),                 # The auto-commit flag.
    ("temp_store", CConfig.u8),                 # 1: file 2: memory 0: default
    ("mallocFailed", CConfig.u8),               # True if we have seen a malloc failure
    ("dfltLockMode", CConfig.u8),               # Default locking-mode for attached dbs
    ("nextAutovac", rffi.CHAR),                 # Autovac setting after VACUUM if >=0
    ("suppressErr", CConfig.u8),                # Do not issue error messages if true
    ("vtabOnConflict", CConfig.u8),             # Value to return for s3_vtab_on_conflict()
    ("isTransactionSavepoint", CConfig.u8),     # True if the outermost savepoint is a TS
    ("nextPagesize", rffi.INT),                 # Pagesize after VACUUM if >0
    ("magic", CConfig.u32),                     # Magic number for detect library misuse
    ("nChange", rffi.INT),                      # Value returned by sqlite3_changes()
    ("nTotalChange", rffi.INT),                 # Value returned by sqlite3_total_changes()
    ("aLimit", lltype.FixedSizeArray(rffi.INT,  # Limits
        CConfig.SQLITE_N_LIMIT)),
    ("nMaxSorterMmap", rffi.INT),               # Maximum size of regions mapped by sorter
    ("init", lltype.Struct("sqlite3InitInfo",   # Information used during initialization
        ("newTnum", rffi.INT),                  # Rootpage of table being initialized
        ("iDb", CConfig.u8),                    # Which db file is being initialized
        ("busy", CConfig.u8),                   # TRUE if currently initializing
        ("orphanTrigger", CConfig.u8))),        # Last statement is orphaned TEMP trigger
    ("nVdbeActive", rffi.INT),                  # Number of VDBEs currently running
    ("nVdbeRead", rffi.INT),                    # Number of active VDBEs that read or write
    ("nVdbeWrite", rffi.INT),                   # Number of active VDBEs that read and write
    ("nVdbeExec", rffi.INT),                    # Number of nested calls to VdbeExec()
    ("nExtension", rffi.INT),                   # Number of loaded extensions
    ("aExtension", rffi.VOIDPP),                # Array of shared library handles
    ("xTrace", rffi.VOIDP),                     #   void (*xTrace)(void*,const char*);        /* Trace function */
    ("pTraceArg", rffi.VOIDP),                  # Argument to the trace function
    ("xProfile", rffi.VOIDP),                   #   void (*xProfile)(void*,const char*,u64);  /* Profiling function */
    ("pProfileArg", rffi.VOIDP),                # Argument to profile function
    ("pCommitArg", rffi.VOIDP),                 # Argument to xCommitCallback()
    ("xCommitCallback", rffi.VOIDP),            #   int (*xCommitCallback)(void*);    /* Invoked at every commit. */
    ("pRollbackArg", rffi.VOIDP),               # Argument to xRollbackCallback()
    ("xRollbackCallback", rffi.VOIDP),          #   void (*xRollbackCallback)(void*); /* Invoked at every commit. */
    ("pUpdateArg", rffi.VOIDP),                 # Argument to xUpdateCallback()
    ("xUpdateCallback", rffi.VOIDP),            #   void (*xUpdateCallback)(void*,int, const char*,const char*,sqlite_int64);
    # #ifndef SQLITE_OMIT_WAL
    #   int (*xWalCallback)(void *, sqlite3 *, const char *, int);
    #   void *pWalArg;
    # #endif
    ("xCollNeeded", rffi.VOIDP),                #   void(*xCollNeeded)(void*,sqlite3*,int eTextRep,const char*);
    ("xCollNeeded16", rffi.VOIDP),              #   void(*xCollNeeded16)(void*,sqlite3*,int eTextRep,const void*);
    ("pCollNeededArg", rffi.VOIDP),
    ("pErr", rffi.VOIDP),                       #   sqlite3_value *pErr;          /* Most recent error message */
    ("u1", lltype.Struct("u1",
        ("isInterrupted", rffi.INT),            #     volatile int isInterrupted; /* True if sqlite3_interrupt has been called */
        ("notUsed1", rffi.DOUBLE),              # Spacer
        hints={"union": True}))
    #   Lookaside lookaside;          /* Lookaside malloc configuration */
    # #ifndef SQLITE_OMIT_AUTHORIZATION
    #   sqlite3_xauth xAuth;          /* Access authorization function */
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
    ))


KEYINFO = lltype.Struct("KeyInfo",                  # src/sqliteInt.h: 1616
    ("nRef", CConfig.u32),                          # Number of references to this KeyInfo object
    ("enc", CConfig.u8),                            # Text encoding - one of the SQLITE_UTF* values
    ("nField", CConfig.u16),                        # Number of key columns in the index
    ("nXField", CConfig.u16),                       # Number of columns beyond the key columns
    ("db", SQLITE3P),                               # The database connection
    ("aSortOrder", rffi.UCHARP),                    # Sort order for each column.
    ("aColl", lltype.FixedSizeArray(COLLSEQP, 1))   # Collating sequence for each term of the key
    )
KEYINFOP = lltype.Ptr(KEYINFO)


CELLINFO = lltype.Struct("CellInfo",    # src/btreeInt.h: 458
    ("nKey", CConfig.i64),              # The key for INTKEY tables, or number of bytes in key
    ("pCell", rffi.UCHARP),             # Pointer to the start of cell content
    ("nData", CConfig.u32),             # Number of bytes of data
    ("nPayload", CConfig.u32),          # Total amount of payload
    ("nHeader", CConfig.u16),           # Size of the cell content header in bytes
    ("nLocal", CConfig.u16),            # Amount of payload held locally
    ("iOverflow", CConfig.u16),         # Offset to overflow page number.  Zero if no overflow
    ("nSize", CConfig.u16)              # Size of the cell content on the main b-tree page
    )


BTCURSOR.become(lltype.Struct("BtCursor",           # src/btreeInt.h: 494
    ("pBtree", BTREEP),                             # The Btree to which this cursor belongs
    ("pBt", rffi.VOIDP),                            #   BtShared *pBt;            /* The BtShared this cursor points to */
    ("pNext", BTCURSORP),                           # Forms a linked list of all cursors
    ("pPrev", BTCURSORP),                           # Forms a linked list of all cursors
    ("pKeyInfo", KEYINFOP),                         # Argument passed to comparison function
    ("aOverflow", rffi.UINTP),                      # Cache of overflow page locations
    ("info", CELLINFO),                             # A parse of the cell we are pointing at
    ("nKey", CConfig.i64),                          # Size of pKey, or last integer key
    ("pKey", rffi.VOIDP),                           # Saved key that was cursor last known position
    ("pgnoRoot", CConfig.u32),                      # The root page of this tree
    ("nOvflAlloc", rffi.INT),                       # Allocated size of aOverflow[] array
    ("skipNext", rffi.INT),                         # Prev() is noop if negative. Next() is noop if positive
    ("curFlags", CConfig.u8),                       # zero or more BTCF_* flags defined below
    ("eState", CConfig.u8),                         # One of the CURSOR_XXX constants (see below)
    ("hints", CConfig.u8),                          # As configured by CursorSetHints()
    ("iPage", CConfig.i16),                         # Index of current page in apPage
    ("aiIdx", lltype.FixedSizeArray(CConfig.u16,    # Current index in apPage[i]
        CConfig.BTCURSOR_MAX_DEPTH)),
    ("apPage", rffi.VOIDP)                          #   MemPage *apPage[BTCURSOR_MAX_DEPTH];  /* Pages from root to current page */
    ))


MEM = lltype.Struct("Mem",          # src/vdbeInt.h: 164
    ("u", lltype.Struct("MemValue",
        ("r", rffi.DOUBLE),         # Real value used when MEM_Real is set in flags
        ("i", CConfig.i64),         # Integer value used when MEM_Int is set in flags
        ("nZero", rffi.INT),        # Used when bit MEM_Zero is set in flags
        ("pDef", FUNCDEFP),         # Used only when flags==MEM_Agg
        ("pRowSet", rffi.VOIDP),    #     RowSet *pRowSet;    /* Used only when flags==MEM_RowSet */
        ("pFrame", rffi.VOIDP),     #     VdbeFrame *pFrame;  /* Used when flags==MEM_Frame */
        hints={"union": True})),
    ("flags", CConfig.u16),         # Some combination of MEM_Null, MEM_Str, MEM_Dyn, etc.
    ("enc", CConfig.u8),            # SQLITE_UTF8, SQLITE_UTF16BE, SQLITE_UTF16LE
    ("n", rffi.INT),                # Number of characters in string value, excluding '\0'
    ("z", rffi.CCHARP),             # String or BLOB value    
    ("zMalloc", rffi.CCHARP),       # Dynamic buffer allocated by sqlite3_malloc()
    ("szMalloc", rffi.INT),         # Size of the zMalloc allocation
    ("uTemp", CConfig.u32),         # Transient storage for serial_type in OP_MakeRecord
    ("db", SQLITE3P),               # The associated database connection
    ("xDel", rffi.VOIDP)            #   void (*xDel)(void *);  /* If not null, call this function to delete Mem.z */
    # #ifdef SQLITE_DEBUG
    #   Mem *pScopyFrom;    /* This Mem is a shallow copy of pScopyFrom */
    #   void *pFiller;      /* So that sizeof(Mem) is a multiple of 8 */
    # #endif
    )
MEMP = lltype.Ptr(MEM)
MEMPP = rffi.CArrayPtr(MEMP)

FUNCTYPESTEPP = lltype.Ptr(lltype.FuncType([CONTEXTP, rffi.INT, MEMPP], lltype.Void))
FUNCTYPEFINALIZEP = lltype.Ptr(lltype.FuncType([CONTEXTP], lltype.Void))


FUNCDEF.become(lltype.Struct("FuncDef",     # src/sqliteInt.h: 1165
    ("nArg", CConfig.i16),                  # Number of arguments.  -1 means unlimited
    ("funcFlags", CConfig.u16),             #   Some combination of SQLITE_FUNC_*
    ("pUserData", rffi.VOIDP),              # User data parameter
    ("pNext", FUNCDEFP),                    # Next function with same name
    ("xFunc", rffi.VOIDP),                  #   void (*xFunc)(sqlite3_context*,int,sqlite3_value**); /* Regular function */
    ("xStep", rffi.VOIDP),                  #   void (*xStep)(sqlite3_context*,int,sqlite3_value**); /* Aggregate step */
    ("xFinalize", rffi.VOIDP),              #   void (*xFinalize)(sqlite3_context*);                /* Aggregate finalizer */
    ("zName", rffi.CCHARP),                 # SQL name of the function.
    ("pHash", FUNCDEFP),                    # Next with a different name but the same hash
    ("pDestructor", rffi.VOIDP)             #   FuncDestructor *pDestructor;   /* Reference counted destructor function */
    ))


VDBEOP = lltype.Struct("VdbeOp",                # src/vdbe.h: 41
    ("opcode", CConfig.u8),                     # What operation to perform
    ("p4type", rffi.CHAR),                      # One of the P4_xxx constants for p4
    ("opflags", CConfig.u8),                    # Mask of the OPFLG_* flags in opcodes.h
    ("p5", CConfig.u8),                         # Fifth parameter is an unsigned character
    ("p1", rffi.INT),                           # First operand
    ("p2", rffi.INT),                           # Second parameter (often the jump destination)
    ("p3", rffi.INT),                           # The third parameter
    ("p4", lltype.Struct("p4",                  # fourth parameter
        ("i", rffi.INT),                        # Integer value if p4type==P4_INT32
        ("p", rffi.VOIDP),                      # Generic pointer
        ("z", rffi.CCHARP),                     # Pointer to data for string (char array) types
        ("pI64", CConfig.i64),                  # Used when p4type is P4_INT64
        ("pReal", rffi.DOUBLEP),                # Used when p4type is P4_REAL
        ("pFunc", FUNCDEFP),                    # Used when p4type is P4_FUNCDEF
        ("pColl", COLLSEQP),                    # Used when p4type is P4_COLLSEQ
        ("pMem", MEMP),                         # Used when p4type is P4_MEM
        ("pVtab", rffi.VOIDP),                  #     VTable *pVtab;         /* Used when p4type is P4_VTAB */
        ("pKeyInfo", KEYINFOP),                 # Used when p4type is P4_KEYINFO
        ("ai", rffi.INTP),                      # Used when p4type is P4_INTARRAY
        ("pProgram", rffi.VOIDP),               #     SubProgram *pProgram;  /* Used when p4type is P4_SUBPROGRAM */
        ("xAdvance", rffi.VOIDP),               #     int (*xAdvance)(BtCursor *, int *);
        hints={"union": True}))
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
    )
VDBEOPP = lltype.Ptr(VDBEOP)


VDBE.become(lltype.Struct("Vdbe",               # src/vdbeInt.h: 325
    ("db", SQLITE3P),                           # The database connection that owns this statement
    ("aOp", lltype.Ptr(lltype.Array(VDBEOP,     # Space to hold the virtual machine's program
        hints={'nolength': True}))),
    ("aMem", rffi.CArrayPtr(MEM)),              # The memory locations
    ("apArg", MEMPP),                           # Arguments to currently executing user function
    ("aColName", rffi.CArrayPtr(MEM)),          # Column names to return
    ("pResultSet", rffi.CArrayPtr(MEM)),        # Pointer to an array of results
    ("pParse", rffi.VOIDP),                     #   Parse *pParse;          /* Parsing context used to create this Vdbe */
    ("nMem", rffi.INT),                         # Number of memory locations currently allocated
    ("nOp", rffi.INT),                          # Number of instructions in the program
    ("nCursor", rffi.INT),                      # Number of slots in apCsr[]
    ("magic", CConfig.u32),                     # Magic number for sanity checking
    ("zErrMsg", rffi.CCHARP),                   # Error message written here
    ("pPrev", VDBEP),                           # Linked list of VDBEs with the same Vdbe.db
    ("pNext", VDBEP),                           # Linked list of VDBEs with the same Vdbe.db
    ("apCsr", VDBECURSORPP),                    # One element of this array for each open cursor
    ("aVar", rffi.CArrayPtr(MEM)),              # Values for the OP_Variable opcode.
    ("azVar", rffi.CCHARPP),                    # Name of variables
    ("nVar", CConfig.ynVar),                    # Number of entries in aVar[]
    ("nzVar", CConfig.ynVar),                   # Number of entries in azVar[]
    ("cacheCtr", CConfig.u32),                  # VdbeCursor row cache generation counter
    ("pc", rffi.INT),                           # The program counter
    ("rc", rffi.INT),                           # Value to return
    ("nResColumn", CConfig.u16),                # Number of columns in one row of the result set
    ("errorAction", CConfig.u8),                # Recovery action to do in case of an error
    ("minWriteFileFormat", CConfig.u8),         # Minimum file format for writable database files
    ("scary_bitfield", lltype.Signed),          #   bft explain:2;          /* True if EXPLAIN present on SQL command */
    #("inVtabMethod", CConfig.u16),              #   bft inVtabMethod:2;     /* See comments above */        
    #("changeCntOn", CConfig.u16),               #   bft changeCntOn:1;      /* True to update the change-counter */
    #("expired", CConfig.u16),                   #   bft expired:1;          /* True if the VM needs to be recompiled */
    #("runOnlyOnce", CConfig.u16),               #   bft runOnlyOnce:1;      /* Automatically expire on reset */
    #("usesStmtJournal", CConfig.u16),           #   bft usesStmtJournal:1;  /* True if uses a statement journal */
    #("readOnly", CConfig.u16),                  #   bft readOnly:1;         /* True for statements that do not write */
    #("bIsReader", CConfig.u16),                 #   bft bIsReader:1;        /* True for statements that read */
    #("isPrepareV2", CConfig.u16),               #   bft isPrepareV2:1;      /* True if prepared with prepare_v2() */
    #("doingRerun", CConfig.u16),                #   bft doingRerun:1;       /* True if rerunning after an auto-reprepare */
    ("nChange", rffi.INT),                      # Number of db changes made since last reset
    ("btreeMask", rffi.UINT),                   #   yDbMask btreeMask;      /* Bitmask of db->aDb[] entries referenced */
    ("lockMask", rffi.UINT),                    #   yDbMask lockMask;       /* Subset of btreeMask that requires a lock */
    ("iStatement", rffi.UINT),                  # Statement number (or 0 if has not opened stmt)
    ("aCounter", lltype.FixedSizeArray(CConfig.u32, 5)), # Counters used by sqlite3_stmt_status()
    # #ifndef SQLITE_OMIT_TRACE
    #   i64 startTime;          /* Time when query started - used for profiling */
    # #endif
    ("iCurrentTime", CConfig.i64),              # Value of julianday('now') for this statement
    ("nFkConstraint", CConfig.i64),             # Number of imm. FK constraints this VM
    ("nStmtDefCons", CConfig.i64),              # Number of def. constraints when stmt started
    ("nStmtDefImmCons", CConfig.i64),           # Number of def. imm constraints when stmt started
    ("zSql", rffi.CCHARP),                      # Text of the SQL statement that generated this
    ("pFree", rffi.VOIDP),                      # Free this when deleting the vdbe
    ("pFrame", rffi.VOIDP),                             #   VdbeFrame *pFrame;      /* Parent frame */
    ("pDelFrame", rffi.VOIDP),                          #   VdbeFrame *pDelFrame;   /* List of frame objects to free on VM reset */
    ("nFrame", rffi.INT),                               # Number of frames in pFrame list
    ("expmask", CConfig.u32),                           # Binding to these vars invalidates VM
    ("pProgram", rffi.VOIDP),                           #   SubProgram *pProgram;   /* Linked list of all sub-programs used by VM */
    ("nOnceFlag", rffi.INT),                            # Size of array aOnceFlag[]
    ("aOnceFlag", U8P),                                 # Flags for OP_Once
    ("pAuxData", rffi.VOIDP)                            #   AuxData *pAuxData;      /* Linked list of auxdata allocations */
    # #ifdef SQLITE_ENABLE_STMT_SCANSTATUS
    #   i64 *anExec;            /* Number of times each op has been executed */
    #   int nScan;              /* Entries in aScan[] */
    #   ScanStatus *aScan;      /* Scan definitions for sqlite3_stmt_scanstatus() */
    # #endif    
    ))


VDBECURSOR.become(lltype.Struct("VdbeCursor",   # src/vdbeInt.h: 63
    ("pCursor", BTCURSORP),                     # The cursor structure of the backend
    ("pBt", BTREEP),                            # Separate file holding temporary table
    ("pKeyInfo", KEYINFOP),                     # Info about index keys needed by index cursors
    ("seekResult", rffi.INT),                   # Result of previous sqlite3BtreeMoveto()
    ("pseudoTableReg", rffi.INT),               # Register holding pseudotable content.
    ("nField", CConfig.i16),                    # Number of fields in the header
    ("nHdrParsed", CConfig.i16),                # Number of header fields parsed so far
    # #ifdef SQLITE_DEBUG
    #   u8 seekOp;            /* Most recent seek operation on this cursor */
    # #endif
    ("iDb", CConfig.i8),                        # Index of cursor database in db->aDb[] (or -1)
    ("nullRow", CConfig.u8),                    # True if pointing to a row with no data
    ("deferredMoveto", CConfig.u8),             # A call to sqlite3BtreeMoveto() is needed
    ("scary_bitfield", CConfig.u8),              #
    #("isEphemeral", lltype.Bool),               #   Bool isEphemeral:1;   /* True for an ephemeral table */
    #("useRandomRowid", lltype.Bool),            #   Bool useRandomRowid:1;/* Generate new record numbers semi-randomly */
    #("isTable", lltype.Bool),                   #   Bool isTable:1;       /* True if a table requiring integer keys */
    #("isOrdered", lltype.Bool),                 #   Bool isOrdered:1;     /* True if the underlying table is BTREE_UNORDERED */
    ("pgnoRoot", CConfig.u32),                  # Pgno pgnoRoot;        /* Root page of the open btree cursor */
    ("pVtabCursor", rffi.VOIDP),                #   sqlite3_vtab_cursor *pVtabCursor;  /* The cursor for a virtual table */
    ("seqCount", CConfig.i64),                  # Sequence counter
    ("movetoTarget", CConfig.i64),              # Argument to the deferred sqlite3BtreeMoveto()
    ("pSorter", rffi.VOIDP),                    #   VdbeSorter *pSorter;  /* Sorter object for OP_SorterOpen cursors */
    #   /* Cached information about the header for the data record that the
    #   ** cursor is currently pointing to.  Only valid if cacheStatus matches
    #   ** Vdbe.cacheCtr.  Vdbe.cacheCtr will never take on the value of
    #   ** CACHE_STALE and so setting cacheStatus=CACHE_STALE guarantees that
    #   ** the cache is out of date.
    #   **
    #   ** aRow might point to (ephemeral) data for the current row, or it might
    #   ** be NULL.
    #   */
    ("cacheStatus", CConfig.u32),                   # Cache is valid if this matches Vdbe.cacheCtr
    ("payloadSize", CConfig.u32),                   # Total number of bytes in the record
    ("szRow", CConfig.u32),                         # Byte available in aRow
    ("iHdrOffset", CConfig.u32),                    # Offset to next unparsed byte of the header
    ("aRow", rffi.UCHARP),                          #   const u8 *aRow;       /* Data for the current row, if all on one page */
    # ("aType", lltype.Ptr(lltype.Array(CConfig.u32,  # Type values for all entries in the record
    #     hints={'nolength': True})))
    ("aOffset", rffi.UINTP),                        # u32 *aOffset;         /* Pointer to aType[nField] */
    ("aType", lltype.FixedSizeArray(CConfig.u32, 1)) # Type values for all entries in the record
    #   /* 2*nField extra array elements allocated for aType[], beyond the one
    #   ** static element declared in the structure.  nField total array slots for
    #   ** aType[] and nField+1 array slots for aOffset[] */
    ))


# The "context" argument for a installable function.  A pointer to an
# instance of this structure is the first argument to the routines used
# implement the SQL functions.
#
# There is a typedef for this structure in sqlite.h.  So all routines,
# even the public interface to SQLite, can use a pointer to this structure.
# But this file is the only place where the internal details of this
# structure are known.
#
# This structure is defined inside of vdbeInt.h because it uses substructures
# (Mem) which are only defined there.

CONTEXT.become(lltype.Struct("CONTEXT",
    ("pOut", MEMP),                # The return value is stored here
    ("pFunc", FUNCDEFP),           # Pointer to function information
    ("pMem", MEMP),                # Memory cell used to store aggregate context
    ("pVdbe", VDBEP),              # The VM that owns this context
    ("iOp", rffi.INT),             # Instruction number of OP_Function
    ("isError", rffi.INT),         # Error code returned by the function.
    ("skipFlag", CConfig.u8),      # Skip skip accumulator loading if true
    ("fErrorOrAux", CConfig.u8)    # isError!=0 or pVdbe->pAuxData modified
))

UNPACKEDRECORD = lltype.Struct("UnpackedRecord",    # src/sqliteInt.h: 1643
    ("pKeyInfo", KEYINFOP),                         # Collation and sort-order information
    ("nField", CConfig.u16),                        # Number of entries in apMem[]
    ("default_rc", CConfig.i8),                     # Comparison result if keys are equal
    ("errCode", CConfig.u8),                        # Error detected by xRecordCompare (CORRUPT or NOMEM)
    ("aMem", MEMP),                                 # Values
    ("r1", rffi.INT),                               # Value to return if (lhs > rhs)
    ("r2", rffi.INT),                               # Value to return if (rhs < lhs)
    )
UNPACKEDRECORDP = lltype.Ptr(UNPACKEDRECORD)

def llexternal(name, args, result, **kwargs):
    return rffi.llexternal(
        name, args, result, compilation_info=CConfig._compilation_info_,
        releasegil=False, **kwargs)


sqlite3_open = llexternal('sqlite3_open', [rffi.CCHARP, SQLITE3PP],
                               rffi.INT)
sqlite3_close = llexternal('sqlite3_close', [SQLITE3P], rffi.INT)
sqlite3_prepare_v2 = llexternal('sqlite3_prepare_v2', [rffi.VOIDP, rffi.CCHARP, rffi.INT, rffi.VOIDPP, rffi.CCHARPP],
                                rffi.INT)
sqlite3_allocateCursor = llexternal('allocateCursor', [lltype.Ptr(VDBE), rffi.INT, rffi.INT, rffi.INT, rffi.INT],
    VDBECURSORP)
sqlite3_sqlite3VdbeMemIntegerify = llexternal('sqlite3VdbeMemIntegerify', [lltype.Ptr(MEM)],
    rffi.INT)
sqlite3_sqlite3BtreeCursor = llexternal('sqlite3BtreeCursor', [rffi.VOIDP, rffi.INT, rffi.INT, rffi.VOIDP, rffi.VOIDP],
    rffi.INT)
sqlite3_sqlite3BtreeCursorHints = llexternal('sqlite3BtreeCursorHints', [BTCURSORP, rffi.UINT],
    lltype.Void)
sqlite3VdbeCursorRestore = llexternal('sqlite3VdbeCursorRestore', [VDBECURSORP],
    rffi.INT)
sqlite3_sqlite3VdbeSorterRewind = llexternal('sqlite3VdbeSorterRewind', [SQLITE3P, VDBECURSORP, rffi.INTP],
    rffi.INT)

impl_OP_Transaction = llexternal('impl_OP_Transaction', [VDBEP, SQLITE3P, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_TableLock = llexternal('impl_OP_TableLock', [VDBEP, SQLITE3P, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_Goto = llexternal('impl_OP_Goto', [VDBEP, SQLITE3P, rffi.LONGP, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_OpenRead_OpenWrite = llexternal('impl_OP_OpenRead_OpenWrite', [VDBEP, SQLITE3P, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_Rewind = llexternal('impl_OP_Rewind', [VDBEP, SQLITE3P, rffi.LONGP, VDBEOPP],
    rffi.LONG)
impl_OP_Column = llexternal('impl_OP_Column', [VDBEP, SQLITE3P, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_ResultRow = llexternal('impl_OP_ResultRow', [VDBEP, SQLITE3P, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_Next = llexternal('impl_OP_Next', [VDBEP, SQLITE3P, rffi.LONGP, VDBEOPP],
    rffi.LONG)
impl_OP_Close = llexternal('impl_OP_Close', [VDBEP, VDBEOPP],
    lltype.Void)
impl_OP_Halt = llexternal('impl_OP_Halt', [VDBEP, SQLITE3P, rffi.LONGP, VDBEOPP],
    rffi.LONG)
impl_OP_Ne_Eq_Gt_Le_Lt_Ge = llexternal('impl_OP_Ne_Eq_Gt_Le_Lt_Ge', [VDBEP, SQLITE3P, rffi.LONGP, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_Integer = llexternal('impl_OP_Integer', [VDBEP, VDBEOPP],
    lltype.Void)
impl_OP_Null = llexternal('impl_OP_Null', [VDBEP, VDBEOPP],
    lltype.Void)
impl_OP_AggStep = llexternal('impl_OP_AggStep', [VDBEP, SQLITE3P, rffi.LONG, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_AggFinal = llexternal('impl_OP_AggFinal', [VDBEP, SQLITE3P, rffi.LONG, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_Copy = llexternal('impl_OP_Copy', [VDBEP, SQLITE3P, rffi.LONG, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_MustBeInt = llexternal('impl_OP_MustBeInt', [VDBEP, SQLITE3P, rffi.LONGP, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_NotExists = llexternal('impl_OP_NotExists', [VDBEP, SQLITE3P, rffi.LONGP, VDBEOPP],
    rffi.LONG)
impl_OP_String = llexternal('impl_OP_String', [VDBEP, SQLITE3P, VDBEOPP],
    lltype.Void)
impl_OP_String8 = llexternal('impl_OP_String8', [VDBEP, SQLITE3P, rffi.LONG, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_Function = llexternal('impl_OP_Function', [VDBEP, SQLITE3P, rffi.LONG, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_Real = llexternal('impl_OP_Real', [VDBEP, VDBEOPP],
    lltype.Void)
impl_OP_RealAffinity = llexternal('impl_OP_RealAffinity', [VDBEP, VDBEOPP],
    lltype.Void)
impl_OP_Add_Subtract_Multiply_Divide_Remainder = llexternal('impl_OP_Add_Subtract_Multiply_Divide_Remainder', [VDBEP, VDBEOPP],
    lltype.Void)
impl_OP_If_IfNot = llexternal('impl_OP_If_IfNot', [VDBEP, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_Rowid = llexternal('impl_OP_Rowid', [VDBEP, SQLITE3P, rffi.LONG, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_IsNull = llexternal('impl_OP_IsNull', [VDBEP, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_SeekLT_SeekLE_SeekGE_SeekGT = llexternal('impl_OP_SeekLT_SeekLE_SeekGE_SeekGT', [VDBEP, SQLITE3P, rffi.LONGP, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_Move = llexternal('impl_OP_Move', [VDBEP, VDBEOPP],
    lltype.Void)
impl_OP_IfZero = llexternal('impl_OP_IfZero', [VDBEP, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_IdxRowid = llexternal('impl_OP_IdxRowid', [VDBEP, SQLITE3P, rffi.LONG, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_IdxLE_IdxGT_IdxLT_IdxGE = llexternal('impl_OP_IdxLE_IdxGT_IdxLT_IdxGE', [VDBEP, SQLITE3P, rffi.LONGP, VDBEOPP],
    rffi.LONG)
impl_OP_Seek = llexternal('impl_OP_Seek', [VDBEP, VDBEOPP],
    lltype.Void)
impl_OP_Once = llexternal('impl_OP_Once', [VDBEP, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_SCopy = llexternal('impl_OP_SCopy', [VDBEP, VDBEOPP],
    lltype.Void)
impl_OP_Affinity = llexternal('impl_OP_Affinity', [VDBEP, SQLITE3P, VDBEOPP],
    lltype.Void)
impl_OP_OpenAutoindex_OpenEphemeral = llexternal('impl_OP_OpenAutoindex_OpenEphemeral', [VDBEP, SQLITE3P, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_MakeRecord = llexternal('impl_OP_MakeRecord', [VDBEP, SQLITE3P, rffi.LONG, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_SorterInsert_IdxInsert = llexternal('impl_OP_SorterInsert_IdxInsert', [VDBEP, SQLITE3P, VDBEOPP],
    rffi.LONG)
impl_OP_NoConflict_NotFound_Found = llexternal('impl_OP_NoConflict_NotFound_Found', [VDBEP, SQLITE3P, rffi.LONGP, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_RowSetTest = llexternal('impl_OP_RowSetTest', [VDBEP, SQLITE3P, rffi.LONGP, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_Gosub = llexternal('impl_OP_Gosub', [VDBEP, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_Return = llexternal('impl_OP_Return', [VDBEP, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_SorterOpen = llexternal('impl_OP_SorterOpen', [VDBEP, SQLITE3P, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_NextIfOpen = llexternal('impl_OP_NextIfOpen', [VDBEP, SQLITE3P, rffi.LONGP, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_Sequence = llexternal('impl_OP_Sequence', [VDBEP, VDBEOPP],
    lltype.Void)
impl_OP_OpenPseudo = llexternal('impl_OP_OpenPseudo', [VDBEP, SQLITE3P, rffi.LONG, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_SorterSort_Sort = llexternal('impl_OP_SorterSort_Sort', [VDBEP, SQLITE3P, rffi.LONGP, VDBEOPP],
    rffi.LONG)
impl_OP_SorterData = llexternal('impl_OP_SorterData', [VDBEP, VDBEOPP],
    rffi.LONG)
impl_OP_SorterNext = llexternal('impl_OP_SorterNext', [VDBEP, SQLITE3P, rffi.LONGP, VDBEOPP],
    rffi.LONG)
impl_OP_Compare = llexternal('impl_OP_Compare', [VDBEP, VDBEOPP],
    lltype.Void)
impl_OP_Jump = llexternal('impl_OP_Jump', [VDBEOPP],
    rffi.LONG)
impl_OP_IfPos = llexternal('impl_OP_IfPos', [VDBEP, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_CollSeq = llexternal('impl_OP_CollSeq', [VDBEP, VDBEOPP],
    lltype.Void)
impl_OP_NotNull = llexternal('impl_OP_NotNull', [VDBEP, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_InitCoroutine = llexternal('impl_OP_InitCoroutine', [VDBEP, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_Yield = llexternal('impl_OP_Yield', [VDBEP, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_NullRow = llexternal('impl_OP_NullRow', [VDBEP, VDBEOPP],
    lltype.Void)
impl_OP_EndCoroutine = llexternal('impl_OP_EndCoroutine', [VDBEP, VDBEOPP],
    rffi.LONG)
impl_OP_ReadCookie = llexternal('impl_OP_ReadCookie', [VDBEP, SQLITE3P, VDBEOPP],
    lltype.Void)
impl_OP_NewRowid = llexternal('impl_OP_NewRowid', [VDBEP, SQLITE3P, rffi.LONG, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_Insert_InsertInt = llexternal('impl_OP_Insert_InsertInt', [VDBEP, SQLITE3P, VDBEOPP],
    rffi.LONG)
impl_OP_SetCookie = llexternal('impl_OP_SetCookie', [VDBEP, SQLITE3P, VDBEOPP],
    rffi.LONG)
impl_OP_ParseSchema = llexternal('impl_OP_ParseSchema', [VDBEP, SQLITE3P, rffi.LONG, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_RowSetAdd = llexternal('impl_OP_RowSetAdd', [VDBEP, SQLITE3P, rffi.LONG, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_RowSetRead = llexternal('impl_OP_RowSetRead', [VDBEP, SQLITE3P, rffi.LONGP, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_Delete = llexternal('impl_OP_Delete', [VDBEP, SQLITE3P, rffi.LONG, VDBEOPP],
    rffi.LONG)
impl_OP_DropTable = llexternal('impl_OP_DropTable', [SQLITE3P, VDBEOPP],
    lltype.Void)
impl_OP_RowKey_RowData = llexternal(
    'impl_OP_RowKey_RowData', [VDBEP, SQLITE3P, rffi.LONG, rffi.LONG, VDBEOPP], rffi.LONG)
impl_OP_Blob = llexternal('impl_OP_Blob', [VDBEP, SQLITE3P, VDBEOPP], lltype.Void)
impl_OP_Concat = llexternal('impl_OP_Concat', [VDBEP, SQLITE3P, rffi.LONG, rffi.LONG, VDBEOPP], rffi.LONG)
impl_OP_Variable = llexternal('impl_OP_Variable', [VDBEP, SQLITE3P, rffi.LONG, rffi.LONG, VDBEOPP], rffi.LONG)

sqlite3_reset = llexternal('sqlite3_reset', [VDBEP],
    rffi.INT)

sqlite3_column_type = llexternal('sqlite3_column_type', [VDBEP, rffi.INT],
    rffi.INT)
sqlite3_column_text = llexternal('sqlite3_column_text', [VDBEP, rffi.INT],
    rffi.CCHARP)
sqlite3_column_bytes = llexternal('sqlite3_column_bytes', [VDBEP, rffi.INT],
    rffi.INT)
sqlite3_column_int64 = llexternal('sqlite3_column_int64', [VDBEP, rffi.INT],
    rffi.LONG)
sqlite3_column_double = llexternal('sqlite3_column_double', [VDBEP, rffi.INT],
    rffi.DOUBLE)

sqlite3_sqlite3BtreeNext = llexternal('sqlite3BtreeNext', [BTCURSORP, rffi.INTP],
    rffi.INT)
sqlite3_sqlite3BtreePrevious = llexternal('sqlite3BtreePrevious', [BTCURSORP, rffi.INTP],
    rffi.INT)
sqlite3BtreeMovetoUnpacked = llexternal('sqlite3BtreeMovetoUnpacked', [BTCURSORP, rffi.VOIDP, CConfig.i64, rffi.INT, rffi.INTP],
    rffi.INT)

# XXX ugly hack, we need macro=True, but only when translating
if sys.argv[0].endswith("rpython"): # we're translating
    kwargs = dict(macro=True)
else:
    kwargs = {}
sqlite3_applyNumericAffinity = llexternal('applyNumericAffinity', [MEMP, rffi.INT],
    lltype.Void, **kwargs)

sqlite3AtoF = llexternal('sqlite3AtoF', [rffi.CCHARP, rffi.DOUBLEP, rffi.INT, CConfig.u8],
    rffi.INT)
sqlite3Atoi64 = llexternal('sqlite3Atoi64', [rffi.CCHARP, rffi.LONGLONGP, rffi.INT, CConfig.u8],
    rffi.INT)
sqlite3VdbeMemSetNull = llexternal('sqlite3VdbeMemSetNull', [MEMP],
    lltype.Void)
vdbeMemClearExternAndSetNull = llexternal('vdbeMemClearExternAndSetNull', [MEMP],
    lltype.Void)
sqlite3VdbeIdxRowid = llexternal('sqlite3VdbeIdxRowid', [SQLITE3P, BTCURSORP, rffi.LONGP],
    rffi.INT)
sqlite3DbFree = llexternal('sqlite3DbFree', [SQLITE3P, rffi.VOIDP],
    lltype.Void)
sqlite3ValueText = llexternal('sqlite3ValueText', [MEMP, CConfig.u8],
    rffi.VOIDP)
sqlite3VdbeIdxKeyCompare = llexternal('sqlite3VdbeIdxKeyCompare', [SQLITE3P, VDBECURSORP, UNPACKEDRECORDP, rffi.INTP],
    rffi.INT)
sqlite3VdbeMemFinalize = llexternal('sqlite3VdbeMemFinalize', [MEMP, FUNCDEFP],
    rffi.INT)
sqlite3VdbeSerialPut = llexternal('sqlite3VdbeSerialPut', [U8P, MEMP, CConfig.u32], CConfig.u32)
sqlite3VdbeChangeEncoding = llexternal('sqlite3VdbeChangeEncoding', [MEMP, rffi.INT],
    rffi.INT)
sqlite3VdbeMemTooBig = llexternal('sqlite3VdbeMemTooBig', [MEMP],
    rffi.INT)
sqlite3VdbeCheckFk = llexternal('sqlite3VdbeCheckFk', [VDBEP, rffi.INT], rffi.INT)
sqlite3VdbeCloseStatement = llexternal('sqlite3VdbeCloseStatement', [VDBEP, rffi.INT], rffi.INT)

# (char **, sqlite3*, const char*, ...);
sqlite3SetString1 = llexternal('sqlite3SetString', [rffi.CCHARPP, SQLITE3P, rffi.CCHARP, rffi.VOIDP],
    lltype.Void)

sqlite3_sqlite3MemCompare = llexternal('sqlite3MemCompare', [MEMP, MEMP, COLLSEQP],
    rffi.INT)
sqlite3_sqlite3VdbeMemShallowCopy = llexternal('sqlite3VdbeMemShallowCopy', [MEMP, MEMP, rffi.INT],
    lltype.Void)
sqlite3_sqlite3VdbeMemStringify = llexternal('sqlite3VdbeMemStringify', [MEMP, CConfig.u8, CConfig.u8],
    rffi.INT)
sqlite3_sqlite3VdbeMemNulTerminate = llexternal('sqlite3VdbeMemNulTerminate', [MEMP],
    rffi.INT)
sqlite3VdbeMemGrow = llexternal('sqlite3VdbeMemGrow', [MEMP, rffi.INT, rffi.INT], rffi.INT)
sqlite3_gotoAbortDueToInterrupt = llexternal('gotoAbortDueToInterrupt', [VDBEP, SQLITE3P, rffi.INT, rffi.INT],
    rffi.INT)
gotoAbortDueToError = llexternal('gotoAbortDueToError', [VDBEP, SQLITE3P, rffi.INT, rffi.INT],
    rffi.INT)
gotoNoMem = llexternal('gotoNoMem', [VDBEP, SQLITE3P, rffi.INT],
    rffi.INT)
gotoTooBig = llexternal('gotoTooBig', [VDBEP, SQLITE3P, rffi.INT],
    rffi.INT)

sqlite3PutVarint = llexternal('sqlite3PutVarint', [rffi.UCHARP, CConfig.u64], rffi.INT)
sqlite3VdbeMemExpandBlob = llexternal('sqlite3VdbeMemExpandBlob', [MEMP], rffi.INT)


sqlite3_create_function = llexternal(
    'sqlite3_create_function',
    [SQLITE3P, rffi.CCHARP, rffi.INT, rffi.INT, rffi.VOIDP,
        rffi.VOIDP, rffi.VOIDP, rffi.VOIDP],
    rffi.INT)

sqlite3_errmsg = llexternal('sqlite3_errmsg', [SQLITE3P], rffi.CCHARP)


sqlite3VdbeMemSetStr = llexternal('sqlite3VdbeMemSetStr', [MEMP, rffi.CCHARP, rffi.INT, CConfig.u8, rffi.VOIDP], rffi.INT)

sqlite3_bind_parameter_count = llexternal('sqlite3_bind_parameter_count', [VDBEP], rffi.INT)
sqlite3_bind_int64 = llexternal('sqlite3_bind_int64', [VDBEP, rffi.INT, lltype.Signed], rffi.INT)
sqlite3_bind_double = llexternal('sqlite3_bind_double', [VDBEP, rffi.INT, rffi.DOUBLE], rffi.INT)
sqlite3_bind_text = llexternal('sqlite3_bind_text', [VDBEP, rffi.INT, rffi.CCHARP, rffi.INT, rffi.VOIDP], rffi.INT)
sqlite3_bind_null = llexternal('sqlite3_bind_null', [VDBEP, rffi.INT], rffi.INT)


valueToText = llexternal('valueToText', [MEMP, CConfig.u8], rffi.VOIDP)
_sqpyte_get_lastRowid = llexternal('_sqpyte_get_lastRowid', [], CConfig.i64)
sqlite3VdbeLeave = llexternal('sqlite3VdbeLeave', [VDBEP], lltype.Void)
