
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

#ifndef __DB_BE_H__
#define __DB_BE_H__

#include "stringutil.h"
#include "tokyobe.h"
#include "extenddberror.h"

typedef void* (*DBBEInit)(const char* name,uint32_t mode);

typedef void (*DBBEFree)(void* be);
typedef int (*DBBEListLen)(void* list);
typedef void* (*DBBEGetList)(void* tcbe,const BinaryStr* key);

typedef uint8_t (*DBBEGet)(void* tcbe,const BinaryStr* key,BinaryStr* outVal);

typedef uint8_t (*DBBEDelete)(void* tcbe,const BinaryStr* key);

typedef uint64_t (*DBBECount)(void* tcbe,const BinaryStr* key);

typedef uint8_t (*DBBESet)(void* tcbe,const BinaryStr* key,BinaryStr* newVal,uint8_t allowDup);

typedef uint8_t (*DBBEOpen)(void* tcbe,int mode);

typedef int (*DBBEClose)(void* tcbe);

typedef int (*DBBESync)(void* tcbe,uint8_t diskSync);

typedef void (*DBBEIterFree)(void* bcur);
typedef uint8_t (*DBBEIterNext)(void* bcur);
typedef uint8_t (*DBBEIterPrev)(void* bcur);
typedef uint8_t (*DBBEIterCurDelete)(void* bcur);

typedef uint8_t (*DBBEIterCur)(void* bcur,BinaryStr* outKey,BinaryStr* outVal);
typedef uint8_t (*DBBEIterJump)(void* cursor,BinaryStr* key);
typedef void* (*DBBEIter)(void* tcbe);

typedef int (*DBBEListVal)(void* list,int index,BinaryStr* out);
typedef void (*DBBEListFree)(void* list);
typedef XTDBError (*DBBEGetLastError)(void* be);

typedef struct DBBEOps {
   DBBEInit          init;
   DBBEFree          free;
   DBBEOpen          open;
   DBBEClose         close;
   DBBESync          sync;

   DBBEGet           get;
   DBBEGetList       getList;
   DBBEListLen       listLen;
   DBBEListVal       listVal;
   DBBEListFree      listFree;
   DBBESet           set;
   DBBEDelete        del;
   DBBECount         count;

   DBBEIter          iter;
   DBBEIterFree      iterFree;
   DBBEIterNext      iterNext;
   DBBEIterPrev      iterPrev;
   DBBEIterCurDelete iterCurDelete;
   DBBEIterCur       iterCurrent;
   DBBEIterJump      iterJump;

   DBBEGetLastError  getLastError;

} DBBEOps;

typedef struct DataBaseBE {
   String name;
   void* privData;
   XTDBError error;
   DBBEOps ops;
} DataBaseBE;

DataBaseBE* DBInit(const char* name ,uint32_t mode);
void DBFree(DataBaseBE* be);
const char* DBName(DataBaseBE* be);

uint8_t DBOpen(DataBaseBE* db,int mode);
uint8_t DBClose(DataBaseBE* db);

uint8_t DBGet(DataBaseBE* db,const BinaryStr* key,BinaryStr* outVal);
uint8_t DBSet(DataBaseBE* db,const BinaryStr* key,BinaryStr* newVal,uint8_t allowDup);
uint8_t DBGetList(DataBaseBE* db,const BinaryStr* key,void** outList);
int DBListVal(DataBaseBE* db,void* list,int index,BinaryStr* out);
int DBListLen(DataBaseBE* db,void* list);
void DBListFree(DataBaseBE* db,void* list);

uint8_t DBDelete(DataBaseBE* db,BinaryStr* key);
uint64_t DBCount(DataBaseBE* db,BinaryStr* key);

void* DBIter(DataBaseBE* db);
uint8_t DBIterNext(DataBaseBE* db,void* cursor);
uint8_t DBIterPrev(DataBaseBE* db,void* cursor);

uint8_t DBIterCur(DataBaseBE* db,void* cursor,
           BinaryStr* outKey,
           BinaryStr* outVal);
uint8_t DBIterCurDelete(DataBaseBE* db,void* cursor);
void DBIterFree(DataBaseBE* db,void* iter);
uint8_t DBIterJump(DataBaseBE* db,void* cursor,BinaryStr* key);

uint8_t DBSync(DataBaseBE* db,uint8_t diskSync);
uint8_t DBDeleteKeyVal(DataBaseBE* db,BinaryStr* key, BinaryStr* value);
XTDBError DBErrorToXTDBError(int rv);

XTDBError DBGetLastError(DataBaseBE* be);
#endif
