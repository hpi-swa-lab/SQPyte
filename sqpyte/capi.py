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

DB = lltype.Struct("Db",			# src/sqliteInt.h: 845
	("zName", rffi.CCHARP),			# Name of this database
	("pBt", rffi.VOIDP),			#   Btree *pBt;          /* The B*Tree structure for this database file */
	("safety_level", CConfig.u8),	# How aggressive at syncing data to disk
	("pSchema", rffi.VOIDP)			#   Schema *pSchema;     /* Pointer to database schema (possibly shared) */
	)
DBP = lltype.Ptr(DB)


SQLITE3.become(lltype.Struct("sqlite3",			# src/sqliteInt.h: 960
	("pVfs", rffi.VOIDP),						#   sqlite3_vfs *pVfs;            /* OS Interface */
	("pVdbe", VDBEP),							# List of active virtual machines
	("pDfltColl", rffi.VOIDP),					#   CollSeq *pDfltColl;           /* The default collating sequence (BINARY) */
	("mutex", rffi.VOIDP),						#   sqlite3_mutex *mutex;         /* Connection mutex */
	("aDb", lltype.Ptr(lltype.Array(DBP,		# All backends
		hints={'nolength': True}))),
	("nDb", rffi.INT),							# Number of backends currently in use
	("flags", rffi.INT),						# Miscellaneous flags. See below
	("lastRowid", CConfig.i64),					# ROWID of most recent insert (see above)
	("szMmap", CConfig.i64),					# Default mmap_size setting
	("openFlags", rffi.UINT),					# Flags passed to sqlite3_vfs.xOpen()
	("errCode", rffi.INT),						# Most recent error code (SQLITE_*)
	("errMask", rffi.INT),						# & result codes with this before returning
	("dbOptFlags", CConfig.u16),				# Flags to enable/disable optimizations
	("autoCommit", CConfig.u8),					# The auto-commit flag.
	("temp_store", CConfig.u8),					# 1: file 2: memory 0: default
	("mallocFailed", CConfig.u8),				# True if we have seen a malloc failure
	("dfltLockMode", CConfig.u8),				# Default locking-mode for attached dbs
	("nextAutovac", rffi.CHAR),					# Autovac setting after VACUUM if >=0
	("suppressErr", CConfig.u8),				# Do not issue error messages if true
	("vtabOnConflict", CConfig.u8),				# Value to return for s3_vtab_on_conflict()
	("isTransactionSavepoint", CConfig.u8),		# True if the outermost savepoint is a TS
	("nextPagesize", rffi.INT),					# Pagesize after VACUUM if >0
	("magic", CConfig.u32),						# Magic number for detect library misuse
	("nChange", rffi.INT),						# Value returned by sqlite3_changes()
	("nTotalChange", rffi.INT),					# Value returned by sqlite3_total_changes()
	("aLimit", rffi.INT),						#   int aLimit[SQLITE_N_LIMIT];   /* Limits */
	("init", lltype.Struct("sqlite3InitInfo",	# Information used during initialization
		("newTnum", rffi.INT),					# Rootpage of table being initialized
		("iDb", CConfig.u8),					# Which db file is being initialized
		("busy", CConfig.u8),					# TRUE if currently initializing
		("orphanTrigger", CConfig.u8))),		# Last statement is orphaned TEMP trigger
	("nVdbeActive", rffi.INT),					# Number of VDBEs currently running
	("nVdbeRead", rffi.INT),					# Number of active VDBEs that read or write
	("nVdbeWrite", rffi.INT),					# Number of active VDBEs that read and write
	("nVdbeExec", rffi.INT),					# Number of nested calls to VdbeExec()
	("nExtension", rffi.INT),					# Number of loaded extensions
	("aExtension", rffi.VOIDPP),				# Array of shared library handles
	("xTrace", rffi.VOIDP),						#   void (*xTrace)(void*,const char*);        /* Trace function */
	("pTraceArg", rffi.VOIDP),					# Argument to the trace function
	("xProfile", rffi.VOIDP),					#   void (*xProfile)(void*,const char*,u64);  /* Profiling function */
	("pProfileArg", rffi.VOIDP),				# Argument to profile function
	("pCommitArg", rffi.VOIDP),					# Argument to xCommitCallback()
	("xCommitCallback", rffi.VOIDP),			#   int (*xCommitCallback)(void*);    /* Invoked at every commit. */
	("pRollbackArg", rffi.VOIDP),				# Argument to xRollbackCallback()
	("xRollbackCallback", rffi.VOIDP),			#   void (*xRollbackCallback)(void*); /* Invoked at every commit. */
	("pUpdateArg", rffi.VOIDP),					# Argument to xUpdateCallback()
	("xUpdateCallback", rffi.VOIDP),			#   void (*xUpdateCallback)(void*,int, const char*,const char*,sqlite_int64);
	# #ifndef SQLITE_OMIT_WAL
	#   int (*xWalCallback)(void *, sqlite3 *, const char *, int);
	#   void *pWalArg;
	# #endif
	("xCollNeeded", rffi.VOIDP),				#   void(*xCollNeeded)(void*,sqlite3*,int eTextRep,const char*);
	("xCollNeeded16", rffi.VOIDP),				#   void(*xCollNeeded16)(void*,sqlite3*,int eTextRep,const void*);
	("pCollNeededArg", rffi.VOIDP),
	("pErr", rffi.VOIDP),						#   sqlite3_value *pErr;          /* Most recent error message */
	("u1", lltype.Struct("u1",
		("isInterrupted", rffi.INT),			#     volatile int isInterrupted; /* True if sqlite3_interrupt has been called */
		("notUsed1", rffi.DOUBLE),				# Spacer
		hints={"union": True}))
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
	))


KEYINFO = lltype.Struct("KeyInfo",	# src/sqliteInt.h: 1616
	("nRef", CConfig.u32),			# Number of references to this KeyInfo object
	("enc", CConfig.u8),			# Text encoding - one of the SQLITE_UTF* values
	("nField", CConfig.u16),		# Number of key columns in the index
	("nXField", CConfig.u16),		# Number of columns beyond the key columns
	("db", SQLITE3P),				# The database connection
	("aSortOrder", CConfig.u8),		# Sort order for each column.
	("aColl", rffi.VOIDP)			#   CollSeq *aColl[1];  /* Collating sequence for each term of the key */
	)
KEYINFOP = lltype.Ptr(KEYINFO)


MEM = lltype.Struct("Mem",			# src/vdbeInt.h: 159
	("db", SQLITE3P),				# The associated database connection
	("z", rffi.CCHARP),				# String or BLOB value
	("r", rffi.DOUBLE),				# Real value
	("u", lltype.Struct("u",
		("i", CConfig.i64),			# Integer value used when MEM_Int is set in flags
		("nZero", rffi.INT),		# Used when bit MEM_Zero is set in flags
		("pDef", rffi.VOIDP),		#     FuncDef *pDef;      /* Used only when flags==MEM_Agg */
		("pRowSet", rffi.VOIDP),	#     RowSet *pRowSet;    /* Used only when flags==MEM_RowSet */
		("pFrame", rffi.VOIDP),		#     VdbeFrame *pFrame;  /* Used when flags==MEM_Frame */
		hints={"union": True})),
	("n", rffi.INT),				# Number of characters in string value, excluding '\0'
	("flags", CConfig.u16),			# Some combination of MEM_Null, MEM_Str, MEM_Dyn, etc.
	("enc", CConfig.u8),			# SQLITE_UTF8, SQLITE_UTF16BE, SQLITE_UTF16LE
	# #ifdef SQLITE_DEBUG
	#   Mem *pScopyFrom;    /* This Mem is a shallow copy of pScopyFrom */
	#   void *pFiller;      /* So that sizeof(Mem) is a multiple of 8 */
	# #endif
	("xDel", rffi.VOIDP),			#   void (*xDel)(void *);  /* If not null, call this function to delete Mem.z */
	("zMalloc", rffi.CCHARP)		# Dynamic buffer allocated by sqlite3_malloc()
	)
MEMP = lltype.Ptr(MEM)
MEMPP = rffi.CArrayPtr(MEMP)


VDBEOP = lltype.Struct("VdbeOp",				# src/vdbe.h: 41
	("opcode", CConfig.u8),						# What operation to perform
	("p4type", rffi.CHAR),						# One of the P4_xxx constants for p4
	("opflags", CConfig.u8),					# Mask of the OPFLG_* flags in opcodes.h
	("p5", CConfig.u8),							# Fifth parameter is an unsigned character
	("p1", rffi.INT),							# First operand
	("p2", rffi.INT),							# Second parameter (often the jump destination)
	("p3", rffi.INT),							# The third parameter
	("p4", lltype.Struct("p4",					# fourth parameter
		("i", rffi.INT),						# Integer value if p4type==P4_INT32
		("p", rffi.VOIDP),						# Generic pointer
		("z", rffi.CCHARP),						# Pointer to data for string (char array) types
		("pI64", CConfig.i64),					# Used when p4type is P4_INT64
		("pReal", rffi.DOUBLE),					# Used when p4type is P4_REAL
		("pFunc", rffi.VOIDP),					#     FuncDef *pFunc;        /* Used when p4type is P4_FUNCDEF */
		("pColl", rffi.VOIDP),					#     CollSeq *pColl;        /* Used when p4type is P4_COLLSEQ */
		("pMem", MEMP),							# Used when p4type is P4_MEM
		("pVtab", rffi.VOIDP),					#     VTable *pVtab;         /* Used when p4type is P4_VTAB */
		("pKeyInfo", KEYINFOP),					# Used when p4type is P4_KEYINFO
		("ai", rffi.VOIDP),						#     int *ai;               /* Used when p4type is P4_INTARRAY */
		("pProgram", rffi.VOIDP),				#     SubProgram *pProgram;  /* Used when p4type is P4_SUBPROGRAM */
		("xAdvance", rffi.VOIDP),				#     int (*xAdvance)(BtCursor *, int *);
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


VDBE.become(lltype.Struct("Vdbe",				# src/vdbeInt.h: 308
	("db", SQLITE3P),							# The database connection that owns this statement
	("aOp", lltype.Ptr(lltype.Array(VDBEOP,		# Space to hold the virtual machine's program
		hints={'nolength': True}))),
	("aMem", rffi.CArrayPtr(MEM)),				# The memory locations
	("apArg", MEMPP),							# Arguments to currently executing user function
	("aColName", rffi.CArrayPtr(MEM)),			# Column names to return
	("pResultSet", rffi.CArrayPtr(MEM)),		# Pointer to an array of results
	("pParse", rffi.VOIDP),						#   Parse *pParse;          /* Parsing context used to create this Vdbe */
	("nMen", rffi.INT),							# Number of memory locations currently allocated
	("nOp", rffi.INT),							# Number of instructions in the program
	("nCursor", rffi.INT),						# Number of slots in apCsr[]
	("magic", CConfig.u32),						# Magic number for sanity checking
	("zErrMsg", rffi.CCHARP),					# Error message written here
	("pPrev", VDBEP),							# Linked list of VDBEs with the same Vdbe.db
	("pNext", VDBEP),							# Linked list of VDBEs with the same Vdbe.db
	("apCsr", VDBECURSORPP),					# One element of this array for each open cursor
	("aVar", rffi.CArrayPtr(MEM)),				# Values for the OP_Variable opcode.
	("azVar", rffi.CCHARPP),					# Name of variables
	("nVar", rffi.INT),							#   ynVar nVar;             /* Number of entries in aVar[] */
	("nzVar", rffi.INT),						#   ynVar nzVar;            /* Number of entries in azVar[] */
	("cacheCtr", CConfig.u32),					# VdbeCursor row cache generation counter
	("pc", rffi.INT),							# The program counter
	("rc", rffi.INT),							# Value to return
	("nResColumn", CConfig.u16),				# Number of columns in one row of the result set
	("errorAction", CConfig.u8),				# Recovery action to do in case of an error
	("minWriteFileFormat", CConfig.u8),			# Minimum file format for writable database files
	("explain", rffi.UINT),						#   bft explain:2;          /* True if EXPLAIN present on SQL command */
	("inVtabMethod", rffi.UINT),				#   bft inVtabMethod:2;     /* See comments above */		
	("changeCntOn", rffi.UINT),					#   bft changeCntOn:1;      /* True to update the change-counter */
	("expired", rffi.UINT),						#   bft expired:1;          /* True if the VM needs to be recompiled */
	("runOnlyOnce", rffi.UINT),					#   bft runOnlyOnce:1;      /* Automatically expire on reset */
	("usesStmtJournal", rffi.UINT),				#   bft usesStmtJournal:1;  /* True if uses a statement journal */
	("readOnly", rffi.UINT),					#   bft readOnly:1;         /* True for statements that do not write */
	("bIsReader", rffi.UINT),					#   bft bIsReader:1;        /* True for statements that read */
	("isPrepareV2", rffi.UINT),					#   bft isPrepareV2:1;      /* True if prepared with prepare_v2() */
	("doingRerun", rffi.UINT),					#   bft doingRerun:1;       /* True if rerunning after an auto-reprepare */
	("nChange", rffi.INT),						# Number of db changes made since last reset
	("btreeMask", rffi.UINT),					#   yDbMask btreeMask;      /* Bitmask of db->aDb[] entries referenced */
	("lockMask", rffi.UINT),					#   yDbMask lockMask;       /* Subset of btreeMask that requires a lock */
	("iStatement", rffi.UINT),					# Statement number (or 0 if has not opened stmt)
	("aCounter", CConfig.u32),					#   u32 aCounter[5];        /* Counters used by sqlite3_stmt_status() */
	# #ifndef SQLITE_OMIT_TRACE
	#   i64 startTime;          /* Time when query started - used for profiling */
	# #endif
	("iCurrentTime", CConfig.i64),				# Value of julianday('now') for this statement
	("nFkConstraint", CConfig.i64),				# Number of imm. FK constraints this VM
	("nStmtDefCons", CConfig.i64),				# Number of def. constraints when stmt started
	("nStmtDefImmCons", CConfig.i64),			# Number of def. imm constraints when stmt started
	("zSql", rffi.CCHARP),						# Text of the SQL statement that generated this
	("pFree", rffi.VOIDP),						# Free this when deleting the vdbe
	# #ifdef SQLITE_ENABLE_TREE_EXPLAIN
	#   Explain *pExplain;      /* The explainer */
	#   char *zExplain;         /* Explanation of data structures */
	# #endif
	("pFrame", rffi.VOIDP),								#   VdbeFrame *pFrame;      /* Parent frame */
	("pDelFrame", rffi.VOIDP),							#   VdbeFrame *pDelFrame;   /* List of frame objects to free on VM reset */
	("nFrame", rffi.INT),								# Number of frames in pFrame list
	("expmask", CConfig.u32),							# Binding to these vars invalidates VM
	("pProgram", rffi.VOIDP),							#   SubProgram *pProgram;   /* Linked list of all sub-programs used by VM */
	("nOnceFlag", rffi.INT),							# Size of array aOnceFlag[]
	("aOnceFlag", lltype.Ptr(lltype.Array(CConfig.u8,	# Flags for OP_Once
		hints={'nolength': True}))),
	("pAuxData", rffi.VOIDP)							#   AuxData *pAuxData;      /* Linked list of auxdata allocations */
	))


VDBECURSOR.become(lltype.Struct("VdbeCursor",	# src/vdbeInt.h: 63
	("pCursor", rffi.VOIDP),					#   BtCursor *pCursor;    /* The cursor structure of the backend */
	("pBt", rffi.VOIDP),						#   Btree *pBt;           /* Separate file holding temporary table */
	("pKeyInfo", KEYINFOP),						# Info about index keys needed by index cursors
	("seekResult", rffi.INT),					# Result of previous sqlite3BtreeMoveto()
	("pseudoTableReg", rffi.INT),				# Register holding pseudotable content.
	("nField", CConfig.i16),					# Number of fields in the header
	("nHdrParsed", CConfig.i16),				# Number of header fields parsed so far
	("iDb", CConfig.i8),						# Index of cursor database in db->aDb[] (or -1)
	("nullRow", CConfig.u8),					# True if pointing to a row with no data
	("rowidIsValid", CConfig.u8),				# True if lastRowid is valid
	("deferredMoveto", CConfig.u8),				# A call to sqlite3BtreeMoveto() is needed
	("isEphemeral", lltype.Bool),				#   Bool isEphemeral:1;   /* True for an ephemeral table */
	("useRandomRowid", lltype.Bool),			#   Bool useRandomRowid:1;/* Generate new record numbers semi-randomly */
	("isTable", lltype.Bool),					#   Bool isTable:1;       /* True if a table requiring integer keys */
	("isOrdered", lltype.Bool),					#   Bool isOrdered:1;     /* True if the underlying table is BTREE_UNORDERED */
	("pVtabCursor", rffi.VOIDP),				#   sqlite3_vtab_cursor *pVtabCursor;  /* The cursor for a virtual table */
	("seqCount", CConfig.i64),					# Sequence counter
	("movetoTarget", CConfig.i64),				# Argument to the deferred sqlite3BtreeMoveto()
	("lastRowid", CConfig.i64),					# Rowid being deleted by OP_Delete
	("pSorter", rffi.VOIDP),					#   VdbeSorter *pSorter;  /* Sorter object for OP_SorterOpen cursors */
	#   /* Cached information about the header for the data record that the
	#   ** cursor is currently pointing to.  Only valid if cacheStatus matches
	#   ** Vdbe.cacheCtr.  Vdbe.cacheCtr will never take on the value of
	#   ** CACHE_STALE and so setting cacheStatus=CACHE_STALE guarantees that
	#   ** the cache is out of date.
	#   **
	#   ** aRow might point to (ephemeral) data for the current row, or it might
	#   ** be NULL.
	#   */
	("cacheStatus", CConfig.u32),				# Cache is valid if this matches Vdbe.cacheCtr
	("payloadSize", CConfig.u32),				# Total number of bytes in the record
	("szRow", CConfig.u32),						# Byte available in aRow
	("iHdrOffset", CConfig.u32),				# Offset to next unparsed byte of the header
	("aRow", rffi.VOIDP),						#   const u8 *aRow;       /* Data for the current row, if all on one page */
	("aType", rffi.VOIDP)						#   u32 aType[1];         /* Type values for all entries in the record */
	#   /* 2*nField extra array elements allocated for aType[], beyond the one
	#   ** static element declared in the structure.  nField total array slots for
	#   ** aType[] and nField+1 array slots for aOffset[] */
	))


sqlite3_open = rffi.llexternal('sqlite3_open', [rffi.CCHARP, SQLITE3PP],
							   rffi.INT, compilation_info=CConfig._compilation_info_)
sqlite3_prepare = rffi.llexternal('sqlite3_prepare', [rffi.VOIDP, rffi.CCHARP, rffi.INT, rffi.VOIDPP, rffi.CCHARPP],
								  rffi.INT, compilation_info=CConfig._compilation_info_)
sqlite3_allocateCursor = rffi.llexternal('allocateCursor', [lltype.Ptr(VDBE), rffi.INT, rffi.INT, rffi.INT, rffi.INT],
	VDBECURSORP, compilation_info=CConfig._compilation_info_)
sqlite3_sqlite3VdbeMemIntegerify = rffi.llexternal('sqlite3VdbeMemIntegerify', [lltype.Ptr(MEM)],
	rffi.INT, compilation_info=CConfig._compilation_info_)
sqlite3_sqlite3BtreeCursor = rffi.llexternal('sqlite3BtreeCursor', [rffi.VOIDP, rffi.INT, rffi.INT, rffi.VOIDP, rffi.VOIDP],
	rffi.INT, compilation_info=CConfig._compilation_info_)

