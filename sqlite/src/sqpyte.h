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
int impl_OP_Compare(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
void impl_OP_Integer(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
void impl_OP_Null(Vdbe *p, sqlite3 *db, int pc, Op *pOp);
int impl_OP_AggStep(Vdbe *p, sqlite3 *db, int rc, Op *pOp);
void impl_OP_AggFinal(Vdbe *p, sqlite3 *db, int rc, Op *pOp);

#endif