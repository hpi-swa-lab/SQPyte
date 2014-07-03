#ifndef _SQPYTE_H_
#define _SQPYTE_H_

#include "vdbeInt.h"

int impl_OP_Transaction(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
int impl_OP_TableLock(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
int impl_OP_Goto(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
void impl_OP_OpenWrite(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
void impl_OP_OpenRead(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
int impl_OP_Rewind(Vdbe *p, sqlite3 *db, int *pc, Op *pOp);
int impl_OP_Column(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
int impl_OP_ResultRow(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
int impl_OP_Next(Vdbe *p, sqlite3 *db, int *pc, Op *pOp);
void impl_OP_Close(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
int impl_OP_Halt(Vdbe *p, sqlite3 *db, int *pc, Op *pOp);
int impl_OP_Ne_Eq_Gt_Le_Lt_Ge(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
void impl_OP_Integer(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
void impl_OP_Null(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
int impl_OP_AggStep(Vdbe *p, sqlite3 *db, int rc, Op *pOp);
void impl_OP_AggFinal(Vdbe *p, sqlite3 *db, int rc, Op *pOp);
void impl_OP_Copy(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
int impl_OP_MustBeInt(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
int impl_OP_NotExists(Vdbe *p, sqlite3 *db, int *pc, Op *pOp);
void impl_OP_String(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
int impl_OP_String8(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
int impl_OP_Function(Vdbe *p, sqlite3 *db, int pc, int rc, Op *pOp);
void impl_OP_Real(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
void impl_OP_RealAffinity(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
void impl_OP_Add_Subtract_Multiply_Divide_Remainder(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
int impl_OP_If_IfNot(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
int impl_OP_Rowid(Vdbe *p, sqlite3 *db, int rc, Op *pOp);
int impl_OP_IsNull(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
int impl_OP_SeekLT_SeekLE_SeekGE_SeekGT(Vdbe *p, sqlite3 *db, int *pc, Op *pOp);
void impl_OP_Move(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
int impl_OP_IfZero(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
int impl_OP_IdxRowid(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
int impl_OP_IdxLE_IdxGT_IdxLT_IdxGE(Vdbe *p, sqlite3 *db, int *pc, Op *pOp);

#endif