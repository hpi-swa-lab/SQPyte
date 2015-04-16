#ifndef _SQPYTE_H_
#define _SQPYTE_H_

#include "vdbeInt.h"

long impl_OP_AggFinal(Vdbe *p, sqlite3 *db, long pc, long rc, Op *pOp);
long impl_OP_AggStep(Vdbe *p, sqlite3 *db, long pc, long rc, Op *pOp);
void impl_OP_Blob(Vdbe *p, sqlite3 *db, Op *pOp);
long impl_OP_Column(Vdbe *p, sqlite3 *db, long pc, Op *pOp);
long impl_OP_Concat(Vdbe* p, sqlite3 *db, long pc, long rc, Op *pOp);
long impl_OP_Copy(Vdbe *p, sqlite3 *db, long pc, long rc, Op *pOp);
long impl_OP_CreateIndex_CreateTable(Vdbe* p, sqlite3 *db, long rc, Op *pOp);
long impl_OP_Delete(Vdbe *p, sqlite3 *db, long pc, Op *pOp);
long impl_OP_EndCoroutine(Vdbe *p, Op *pOp);
long impl_OP_Function(Vdbe *p, sqlite3 *db, long pc, long rc, Op *pOp);
long impl_OP_Gosub(Vdbe *p, long pc, Op *pOp);
long impl_OP_Goto(Vdbe *p, sqlite3 *db, long pc, long rc, Op *pOp);
long impl_OP_Halt(Vdbe *p, sqlite3 *db, long *pc, Op *pOp);
long impl_OP_IdxLE_IdxGT_IdxLT_IdxGE(Vdbe *p, sqlite3 *db, long *pc, Op *pOp);
long impl_OP_IdxRowid(Vdbe *p, sqlite3 *db, long pc, long rc, Op *pOp);
long impl_OP_IfPos(Vdbe *p, long pc, Op *pOp);
long impl_OP_IfZero(Vdbe *p, long pc, Op *pOp);
long impl_OP_If_IfNot(Vdbe *p, long pc, Op *pOp);
long impl_OP_InitCoroutine(Vdbe *p, long pc, Op *pOp);
long impl_OP_Insert_InsertInt(Vdbe *p, sqlite3 *db, Op *pOp);
long impl_OP_IsNull(Vdbe *p, long pc, Op *pOp);
long impl_OP_Jump(Op *pOp);
long impl_OP_MakeRecord(Vdbe *p, sqlite3 *db, long pc, long rc, Op *pOp);
long impl_OP_MustBeInt(Vdbe *p, sqlite3 *db, long *pc, long rc, Op *pOp);
long impl_OP_Ne_Eq_Gt_Le_Lt_Ge(Vdbe *p, sqlite3 *db, long *pc, long rc, Op *pOp);
long impl_OP_NewRowid(Vdbe *p, sqlite3 *db, long pc, long rcIn, Op *pOp);
long impl_OP_Next(Vdbe *p, sqlite3 *db, long *pc, Op *pOp);
long impl_OP_NextIfOpen(Vdbe *p, sqlite3 *db, long *pc, long rc, Op *pOp);
long impl_OP_NoConflict_NotFound_Found(Vdbe *p, sqlite3 *db, long *pc, long rc, Op *pOp);
long impl_OP_NotExists(Vdbe *p, sqlite3 *db, long *pc, Op *pOp);
long impl_OP_NotNull(Vdbe *p, long pc, Op *pOp);
long impl_OP_Once(Vdbe *p, long pc, Op *pOp);
long impl_OP_OpenAutoindex_OpenEphemeral(Vdbe *p, sqlite3 *db, long pc, Op *pOp);
long impl_OP_OpenPseudo(Vdbe *p, sqlite3 *db, long pc, long rc, Op *pOp);
long impl_OP_OpenRead_OpenWrite(Vdbe *p, sqlite3 *db, long pc, Op *pOp);
long impl_OP_ParseSchema(Vdbe *p, sqlite3 *db, long pc, long rcIn, Op *pOp);
long impl_OP_ResultRow(Vdbe *p, sqlite3 *db, long pc, Op *pOp);
long impl_OP_Return(Vdbe *p, long pc, Op *pOp);
long impl_OP_Rewind(Vdbe *p, sqlite3 *db, long *pc, Op *pOp);
long impl_OP_RowKey_RowData(Vdbe* p, sqlite3 *db, long pc, long rc, Op *pOp);
long impl_OP_RowSetAdd(Vdbe *p, sqlite3 *db, long pc, long rc, Op *pOp);
long impl_OP_RowSetRead(Vdbe *p, sqlite3 *db, long *pc, long rc, Op *pOp);
long impl_OP_RowSetTest(Vdbe *p, sqlite3 *db, long *pc, long rc, Op *pOp);
long impl_OP_Rowid(Vdbe *p, sqlite3 *db, long pc, long rc, Op *pOp);
long impl_OP_SeekLT_SeekLE_SeekGE_SeekGT(Vdbe *p, sqlite3 *db, long *pc, long rc, Op *pOp);
long impl_OP_SetCookie(Vdbe *p, sqlite3 *db, Op *pOp);
long impl_OP_SorterData(Vdbe *p, Op *pOp);
long impl_OP_SorterInsert_IdxInsert(Vdbe *p, sqlite3 *db, Op *pOp);
long impl_OP_SorterNext(Vdbe *p, sqlite3 *db, long *pc, Op *pOp);
long impl_OP_SorterOpen(Vdbe *p, sqlite3 *db, long pc, Op *pOp);
long impl_OP_SorterSort_Sort(Vdbe *p, sqlite3 *db, long *pc, Op *pOp);
long impl_OP_String8(Vdbe *p, sqlite3 *db, long pc, long rc, Op *pOp);
long impl_OP_TableLock(Vdbe *p, sqlite3 *db, long rc, Op *pOp);
long impl_OP_Transaction(Vdbe *p, sqlite3 *db, long pc, Op *pOp);
long impl_OP_Yield(Vdbe *p, long pc, Op *pOp);
void impl_OP_Add_Subtract_Multiply_Divide_Remainder(Vdbe *p, Op *pOp);
void impl_OP_Affinity(Vdbe *p, sqlite3 *db, Op *pOp);
void impl_OP_Close(Vdbe *p, Op *pOp);
void impl_OP_CollSeq(Vdbe *p, Op *pOp);
void impl_OP_Compare(Vdbe *p, Op *pOp);
void impl_OP_DropTable(sqlite3 *db, Op *pOp);
void impl_OP_Integer(Vdbe *p, Op *pOp);
void impl_OP_Move(Vdbe *p, Op *pOp);
void impl_OP_Null(Vdbe *p, Op *pOp);
void impl_OP_NullRow(Vdbe *p, Op *pOp);
void impl_OP_ReadCookie(Vdbe *p, sqlite3 *db, Op *pOp);
void impl_OP_Real(Vdbe *p, Op *pOp);
void impl_OP_RealAffinity(Vdbe *p, Op *pOp);
void impl_OP_SCopy(Vdbe *p, Op *pOp);
void impl_OP_Seek(Vdbe *p, Op *pOp);
void impl_OP_Sequence(Vdbe *p, Op *pOp);
void impl_OP_String(Vdbe *p, sqlite3 *db, Op *pOp);
long impl_OP_Variable(Vdbe* p, sqlite3 *db, long pc, long rc, Op *pOp);

int gotoAbortDueToError(Vdbe *p, sqlite3 *db, int pc, int rc);
int gotoAbortDueToInterrupt(Vdbe *p, sqlite3 *db, int pc, int rc);
int gotoNoMem(Vdbe *p, sqlite3 *db, int pc);
int gotoTooBig(Vdbe *p, sqlite3 *db, int pc);
int gotoVdbeErrorHalt(Vdbe *p, sqlite3 *db, int pc, int rc);

i64 _sqpyte_get_lastRowid();

const void *valueToText(sqlite3_value* pVal, u8 enc);

#endif
