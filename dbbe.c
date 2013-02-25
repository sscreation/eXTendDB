
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

#include "dbbe.h"
#include <assert.h>

extern DBBEOps tokyoBEOps;

#define OP_INIT(db) (db->ops.init) 

static uint8_t DBGetLastBEError(DataBaseBE* db) {
   //db->error = TokyoBEGetLastError(db->privData);  
   db->error = db->ops.getLastError(db->privData);  
   
   return db->error== XTDB_OK ? True:False;
}

int DBListVal(DataBaseBE* db,void* list,int index,BinaryStr* out) {
   //return TokyoBEListVal(list,index,out); 
   return db->ops.listVal(list,index,out); 
}

int
DBListLen(DataBaseBE* db,void* list) {
   //return TokyoBEListLen(list);
   return db->ops.listLen(list);
}

DataBaseBE*
DBInit(const char* name ,uint32_t mode) {
   DataBaseBE * be = malloc(sizeof(DataBaseBE));

   be->ops = tokyoBEOps;
   if (!be) {
      return be;
   }
   StrInit(&be->name);
   if (StrAppend(&be->name,name)) {
      free(be);
      return NULL;
   }
   //be->privData = TokyoBEInit(name,mode);
   be->privData = be->ops.init(name,mode);
   if (!be->privData) {
      free(be);
      return NULL;
   }

   return be;
}

void
DBFree(DataBaseBE* be) {
   //TokyoBEFree(be->privData);
   be->ops.free(be->privData);
   be->privData = NULL;
   StrFree(&be->name);
   free(be);
}

const char*
DBName(DataBaseBE* be) {
   return STR(&be->name);
}

uint8_t
DBGet(DataBaseBE* db,const BinaryStr* key,BinaryStr* outVal){
   db->error = XTDB_OK;
   /*if(!TokyoBEGet(db->privData,key,outVal)) {
      return DBGetLastBEError(db);
   }*/
   if(!db->ops.get(db->privData,key,outVal)) {
      return DBGetLastBEError(db);
   }
   return True;
}

uint8_t
DBSet(DataBaseBE* db,const BinaryStr* key,BinaryStr* newVal,uint8_t allowDup){
   db->error = XTDB_OK;
   /*if(!TokyoBESet(db->privData,key,newVal,allowDup)) {
      return DBGetLastBEError(db);
   }*/
   if(!db->ops.set(db->privData,key,newVal,allowDup)) {
      return DBGetLastBEError(db);
   }
   return True;
}

uint8_t
DBGetList(DataBaseBE* db,const BinaryStr* key,void** outList){
   //*outList =  TokyoBEGetList(db->privData,key);
   *outList =  db->ops.getList(db->privData,key);
   if (!*outList) {
      db->error = XTDB_NO_ENTRY;
      return False;
   }
   return True;
}

void
DBListFree(DataBaseBE* db,void* list) {
   db->ops.listFree(list);
   //tclistdel(list);
}

uint8_t
DBOpen(DataBaseBE* db,int mode) {
   db->error = XTDB_OK;
   //printf("Opening file %s\n",db->name.ptr);
   /*if (!TokyoBEOpen(db->privData,mode)) {
      return DBGetLastBEError(db);
   }*/
   if (!db->ops.open(db->privData,mode)) {
      return DBGetLastBEError(db);
   }
   return True;
}

uint8_t
DBClose(DataBaseBE* db) {
   db->error = XTDB_OK;
   DBSync(db,True);
  // printf("Closing file %s\n",db->name.ptr);
   /*if (!TokyoBEClose(db->privData)) {
      return DBGetLastBEError(db);
   }*/
   if (!db->ops.close(db->privData)) {
      return DBGetLastBEError(db);
   }
   return True;
}

uint8_t
DBDelete(DataBaseBE* db,BinaryStr* key) {
   db->error = XTDB_OK;
   /*if (!TokyoBEDelete(db->privData,key)) {
      return DBGetLastBEError(db);
   }*/
   if (!db->ops.del(db->privData,key)) {
      return DBGetLastBEError(db);
   }
   return True;
}

uint64_t
DBCount(DataBaseBE* db,BinaryStr* key) {
   //return TokyoBECount(db->privData,key);
   return db->ops.count(db->privData,key);
}

void*
DBIter(DataBaseBE* db) {

   void* cur;
   db->error = XTDB_OK;
   /*if (!(cur=TokyoBEIter(db->privData))) {
      db->error = XTDB_NO_MEM;
      return NULL;
   }*/
   if (!(cur=db->ops.iter(db->privData))) {
      db->error = XTDB_NO_MEM;
      return NULL;
   }
   return cur;
}

uint8_t
DBIterNext(DataBaseBE* db,void* cursor){
   //db->error = XTDB_OK;
   //return TokyoBEIterNext(cursor);
   return db->ops.iterNext(cursor);
}

uint8_t
DBIterPrev(DataBaseBE* db,void* cursor){
   //db->error = XTDB_OK;
   //return TokyoBEIterPrev(cursor);
   return db->ops.iterPrev(cursor);
}

uint8_t
DBIterCur(DataBaseBE* db, void* cursor,
           BinaryStr* outKey,
           BinaryStr* outVal){
   //db->error = XTDB_OK;
   /*if (!TokyoBEIterCur(cursor,outKey,outVal)) {
      //db->error = XTDB_INVALID_CURSOR;
      return False;
   }*/
   if (!db->ops.iterCurrent(cursor,outKey,outVal)) {
      //db->error = XTDB_INVALID_CURSOR;
      return False;
   }
   return True;
}

uint8_t
DBIterCurDelete(DataBaseBE* db,void* cursor) {
   //db->error = XTDB_OK;
   /*if (!TokyoBEIterCurDelete(cursor)) {
      // what is the likely reason for delete to fail invalid cursor or io error ?
      //db->error = XTDB_IO_ERR;
      return False;
   }*/
   if (!db->ops.iterCurDelete(cursor)) {
      // what is the likely reason for delete to fail invalid cursor or io error ?
      //db->error = XTDB_IO_ERR;
      return False;
   }
   return True;
}

uint8_t
DBSync(DataBaseBE* db,uint8_t diskSync) {
   db->error = XTDB_OK;
   /*if(!TokyoBESync(db->privData)){
      return DBGetLastBEError(db);
   }*/
   if(!db->ops.sync(db->privData,diskSync)){
      return DBGetLastBEError(db);
   }
   return True;
}

void
DBIterFree(DataBaseBE* db,void* iter) {
   //return TokyoBEIterFree(iter);
   return db->ops.iterFree(iter);
}

uint8_t
DBIterJump(DataBaseBE* db,void* cursor,BinaryStr* key) {
   //db->error = XTDB_OK;
   
/*   if(!TokyoBEIterJump(cursor,key)) {
      //db->error = XTDB_NO_ENTRY;
      return False;
   }*/
   if(!db->ops.iterJump(cursor,key)) {
      //db->error = XTDB_NO_ENTRY;
      return False;
   }
   return True;
}

// Can return false for the following conditions
// 1. no memory
// 2. entry cant be deleted from db

uint8_t
DBDeleteKeyVal(DataBaseBE* db,BinaryStr* key, BinaryStr* value){
   void *cursor;
   BinaryStr outKey,outVal;
   uint8_t ret;

   cursor = DBIter(db);
   if (!cursor) {
      db->error = XTDB_NO_MEM;
      return False;
   }
   ret = DBIterJump(db,cursor,key);
   if (!ret) {
      return True;
   }
   while (1) {
      if (!DBIterCur(db,cursor,&outKey,&outVal)) {
         DBIterFree(db,cursor);
         db->error = XTDB_INVALID_CURSOR;
         return False;
      }

      if (!BStrEqual(&outKey,key)){
         BinaryStrFree(&outKey);
         BinaryStrFree(&outVal);
         break;
      } 
       
      if (!value || BStrEqual(&outVal,value)){
         if (!DBIterCurDelete(db,cursor)) {
            BinaryStrFree(&outKey);
            BinaryStrFree(&outVal);
            DBIterFree(db,cursor);
            db->error = XTDB_INVALID_CURSOR;
            return False;
         }

      }
      BinaryStrFree(&outKey);
      BinaryStrFree(&outVal);
      if (!DBIterNext(db,cursor)) {
         break;
      }
   }
   DBIterFree(db,cursor);
   return True;
}


XTDBError DBGetLastError(DataBaseBE* be) {
   return be->error;
}
