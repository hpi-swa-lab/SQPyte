from rpython.rtyper.tool import rffi_platform as platform
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.translator.tool.cbuild import ExternalCompilationInfo
import sys

class CConfig:
    _compilation_info_ = ExternalCompilationInfo(
        includes = ['sqlite3.h'],
        libraries = ['sqlite3']
    )


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


VDBEP = lltype.Ptr(VDBE)

sqlite3_open = rffi.llexternal('sqlite3_open', [rffi.CCHARP, rffi.VOIDPP],
							   rffi.INT, compilation_info=CConfig._compilation_info_)
sqlite3_prepare = rffi.llexternal('sqlite3_prepare', [rffi.VOIDP, rffi.CCHARP, rffi.INT, rffi.VOIDPP, rffi.CCHARPP],
								  rffi.INT, compilation_info=CConfig._compilation_info_)

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


	

