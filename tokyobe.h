/*
    Copyright (C) 2013 Sridhar Valaguru <sridharnitt@gmail.com>

    This file is part of eXTendDB.

    eXTendDB is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    eXTendDB is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with eXTendDB.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef __TOKYO_BE_H__
#define __TOKYO_BE_H__

#include <tcbdb.h>
#include "stringutil.h"
#include "dbbe.h"
#include "extenddberror.h"

typedef struct TokyoBackEnd {
   String dbFileName;
   TCBDB  *dbHandle; 
} TokyoBackEnd;


#define BSTR_FMT "%p:%u"
#define BSTR_ARGS(bStr) (bStr)->data, (bStr)->len
#define MAX_OBJ_LEN 4*1024
#define MAX_KEY_LEN 4*1024

#if 0
TokyoBackEnd* TokyoBEInit(const char* name,uint32_t mode);

void TokyoBEFree(TokyoBackEnd* tc);
int TokyoBEListLen(TCLIST* list);
void* TokyoBEGetList(TokyoBackEnd* tcbe,const BinaryStr* key);

uint8_t TokyoBEGet(TokyoBackEnd* tcbe,const BinaryStr* key,BinaryStr* outVal);

uint8_t TokyoBEDelete(TokyoBackEnd* tcbe,const BinaryStr* key);

uint64_t TokyoBECount(TokyoBackEnd* tcbe,const BinaryStr* key);


uint8_t TokyoBESet(TokyoBackEnd* tcbe,const BinaryStr* key,BinaryStr* newVal,uint8_t allowDup);

uint8_t TokyoBEOpen(TokyoBackEnd* tcbe,int mode);

int TokyoBEClose(TokyoBackEnd* tcbe);

int TokyoBESync(TokyoBackEnd* tcbe);


void TokyoBEIterFree(BDBCUR* bcur);
uint8_t TokyoBEIterNext(BDBCUR* bcur);
uint8_t TokyoBEIterPrev(BDBCUR* bcur);
uint8_t TokyoBEIterCurDelete(BDBCUR* bcur);

uint8_t TokyoBEIterCur(BDBCUR* bcur,BinaryStr* outKey,BinaryStr* outVal);
uint8_t TokyoBEIterJump(BDBCUR* cursor,BinaryStr* key);
BDBCUR* TokyoBEIter(TokyoBackEnd* tcbe);

int TokyoBEListVal(TCLIST* list,int index,BinaryStr* out);
XTDBError TokyoBEGetLastError();
#endif

#endif
