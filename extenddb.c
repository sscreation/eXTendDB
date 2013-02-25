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


#include <tcbdb.h>
#include <errno.h>
#include <execinfo.h>
#include <stdio.h>
#include <stdlib.h>

#include "bson.h"
#include "extenddb.h"
#include "list.h"
#include "objmod.h"

typedef struct FuncStat {
   double totalTime;
   uint64_t numCalls;
}OpStat;

typedef struct XTDBProfile {
   OpStat sync;
   OpStat insert;
   OpStat update;
   OpStat find; 
   OpStat curNext;
   OpStat queryMatch;
   OpStat remove;
   OpStat indexRemove;
   OpStat indexInsert;
   OpStat indexUpdate;
} XTDBProfile;

typedef enum XTDBOp {
   XTDB_OP_INSERT,
   XTDB_OP_UPDATE,
   XTDB_OP_REMOVE,
   XTDB_OP_FIND,
   XTDB_OP_COUNT,
   XTDB_CREATE_INDEX,
   XTDB_OP_DROP_INDEX,
   XTDB_OP_DROP,
}XTDBOp;

typedef struct XTDBUpdateStatus {
   int nUpdated;
   uint8_t upserted;
   bson_oid_t newOid;
} XTDBUpdateStatus;

typedef struct XTDBInsertStatus {
   bson_oid_t newOid;
}XTDBInsertStatus;

typedef union XTDBStatus {
   XTDBUpdateStatus updateStatus;
   XTDBInsertStatus insertStatus;
}XTDBStatus;

typedef struct XTDBHandle {
   String dbName;
   String descName; 
   String mainDbFileName;
   TCMAP *indexes;  
   //HashTable *indexes;
   DataBaseBE* mainDB;
   DataBaseBE* descDB;
   String dataDir;
   uint64_t gen;
   XTDBProfile prof;
   XTDBError error; 
   XTDBOp lastOp;
   char errorString[256];
   XTDBStatus status;
} XTDBHandle;

#define STAT(handle,op) ((handle)->prof. op )
#if XDB_PROF
#define _S_FN(op) double __a = UTime();
#define _E_FN(op) STAT(handle,op).totalTime += (UTime() - __a); STAT(handle,op).numCalls++; 
#else
#define _S_FN(op)
#define _E_FN(op)
#endif

/* Obtain a backtrace and print it to stdout. */
void
print_trace (void)
{
   void *array[10];
   size_t size;
   char **strings;
   size_t i;

   size = backtrace (array, 10);
   strings = backtrace_symbols (array, size);

   printf ("Obtained %zd stack frames.\n", size);

   for (i = 0; i < size; i++)
      printf ("%s\n", strings[i]);

   free (strings);
}

static inline double UTime(){
   struct timeval tim;
   double ret;
   gettimeofday(&tim, NULL);
   ret = tim.tv_sec+(tim.tv_usec/1000000.0);
   return ret;
}

int
GetIndexDBName(XTDBHandle* h,
                 const char* field,String* out) {
   StrInit(out);
   return StrAppendFmt(out,"%s/%s.%s.idx",h->dataDir.ptr,h->dbName.ptr,field);
}

// descriptor key names
#define IDXFIELDNAME "index.names"


DataBaseBE*
XTDBGetIndex(XTDBHandle* handle,const char* fieldName) {
   BinaryStr field;
   StrToBStr(fieldName,&field);
   
   DataBaseBE *outBase = NULL;
   
   int len;
   
   const void *pPtr = tcmapget(handle->indexes,fieldName,strlen(fieldName),&len);
   if (!pPtr) {
      return NULL;
   }
   memcpy(&outBase,pPtr,len);
   //free(pPtr);
   /*outBase = hash_table_lookup(handle->indexes,(char*)fieldName);*/

   return outBase;
}

static uint8_t
XTDBRemoveIndex(XTDBHandle* handle,char* fieldName) {
   //hash_table_remove(h->indexes,fieldName);
   if(!tcmapout(handle->indexes,fieldName,strlen(fieldName))) {
      handle->error  = XTDB_NO_ENTRY;
      return False;
   }
   return True;
}

//
// Checked

static uint8_t
XTDBAddIndex(XTDBHandle* h,char* fieldName) {
   String idxDBName;
   BinaryStr out;
   uint8_t success=False;
   DataBaseBE* db;

   DataBaseBE* be = XTDBGetIndex(h,fieldName);
   if (be) {
      // XXX Set error
      // Index already exists
      assert(0);
      return False;
   }
   StrToBStr(fieldName,&out);
   GetIndexDBName(h,fieldName,&idxDBName);
   db = DBInit(STR(&idxDBName),BDBOWRITER);
   //print_trace();

   if (db) {
      uint8_t rv = DBOpen(db,BDBOWRITER);
      //printf("Return value %d,%s\n",rv,tcbdberrmsg(rv));
      if (!rv) {
         //h->error = DBErrorToXTDBError(rv);
         h->error = DBGetLastError(db);
         DBFree(db);
         assert(0);
         return False;
      }
      // No error for this function 
      tcmapput(h->indexes,out.data,out.len,&db,sizeof(db));
      int a;
      assert(tcmapget(h->indexes,out.data,out.len,&a));

      /*char* newStr = strdup(fieldName);
      if(hash_table_insert(h->indexes,newStr,db)) {
      }
      assert(hash_table_lookup(h->indexes,newStr));*/
   } else {
      h->error = XTDB_NO_MEM;
      return False;
   }
   StrFree(&idxDBName);
   assert(tcmaprnum(h->indexes));
   return True;
}

// checked
uint8_t
XTDBLoadIndexes(XTDBHandle* h) {
   void* idxList = NULL;
   BinaryStr bStr,out;
   uint8_t rv;

   h->descDB = DBInit(STR(&h->descName),BDBOCREAT|BDBOWRITER);
   if (!h->descDB) {
      h->error = XTDB_NO_MEM;
      return False;
   }
   rv = DBOpen(h->descDB,BDBOCREAT|BDBOWRITER|BDBOTSYNC);
   if (!rv) {
      // XXX revisit
      h->error = DBGetLastError(h->descDB);
      if (h->error == XTDB_FILE_NOT_FOUND) {
         h->error = XTDB_DESC_NOT_FOUND;
      }
      return False;
   }

   StrToBStr(IDXFIELDNAME,&bStr);

   if (DBGetList(h->descDB,&bStr,&idxList)) {
      int idxLen = DBListLen(h->descDB,idxList);
      int i;
      for (i=0;i<idxLen;i++) {
         DBListVal(h->descDB,idxList,i,&out);
         char* idxName = BStrData(&out); 
         //printf("IDX in descriptor %s\n",idxName);
         if (!XTDBAddIndex(h,idxName)) {
            return False;
         }
         assert(XTDBGetIndex(h,idxName));

      }
      DBListFree(h->descDB,idxList);
   }
   return True;
}

/**
 * Compare two strings to determine if they are equal.
 *
 * @param string1         The first string.
 * @param string2         The second string.
 * @return                Non-zero if the strings are equal, zero if they are
 *                        not equal.
 */
/*
int strequal(void *string1, void *string2) {
   int equal = strcmp(string1,string2);
   return !equal;
}

unsigned long stringhash(void* string){
   unsigned long h = string_hash(string);
   return h;
}*/

// return values checked

XTDBError
XTDBInitHandle(const char* dbName,const char* dir,XTDBHandle** out) {
   XTDBError rv;
   XTDBHandle* h = malloc(sizeof(XTDBHandle));
   int rc;
   //InitQueryModule();
   if (!h) {
      return XTDB_NO_MEM;
   }
   memset(h,0,sizeof(*h));
   StrInit(&h->dbName);
   if (StrAppend(&h->dbName,dbName)) {
      rv = XTDB_NO_MEM;
      goto error;
   }
   StrInit(&h->dataDir);
   if (StrAppend(&h->dataDir,dir)) {
      rv = XTDB_NO_MEM;
      goto error;
   }
   StrInit(&h->descName);
   if (StrAppendFmt(&h->descName,"%s/%s.desc",dir,dbName)) {
      rv = XTDB_NO_MEM;
      goto error;
   }
   StrInit(&h->mainDbFileName);
   if (StrAppendFmt(&h->mainDbFileName,"%s/%s.main.data",dir,dbName)) {
      rv = XTDB_NO_MEM;
      goto error;
   }

   h->indexes = tcmapnew2(23); 
   if (!h->indexes) {
      rv = XTDB_NO_MEM;
      goto error;
   }
   //h->indexes = hash_table_new(stringhash,string_equal); 
   //hash_table_register_free_functions(h->indexes,free,NULL);
   if (!(h->mainDB = DBInit(h->mainDbFileName.ptr,BDBOCREAT|BDBOWRITER))) {
      rv = XTDB_NO_MEM;
      goto error;
   }
   
   if (!(rc=DBOpen(h->mainDB,BDBOCREAT|BDBOWRITER))) {
      rv = DBGetLastError(h->mainDB);
      goto error;
   }
   
   if (!XTDBLoadIndexes(h)) {
      rv = h->error;
      goto error;
   }
   *out = h;
   return XTDB_OK;

error:
   XTDBFreeHandle(h);
   *out = NULL;
   return rv;
}

#define OP_STAT_FMT "[%lu calls in %lf seconds]"
#define OP_STAT_ARGS(opStat) (opStat).numCalls,(opStat).totalTime
 
void
PrintProfile(XTDBHandle* handle){
   printf("SYNC TIMES \t"OP_STAT_FMT"\n",OP_STAT_ARGS(handle->prof.sync));
   printf("INSERT TIMES \t"OP_STAT_FMT"\n",OP_STAT_ARGS(handle->prof.insert));
   printf("UPDATE TIMES \t"OP_STAT_FMT"\n",OP_STAT_ARGS(handle->prof.update));
   printf("FIND TIMES \t"OP_STAT_FMT"\n",OP_STAT_ARGS(handle->prof.find));
   printf("NEXT CURSOR \t"OP_STAT_FMT"\n",OP_STAT_ARGS(handle->prof.curNext));
   printf("QUERY MATCH \t"OP_STAT_FMT"\n",OP_STAT_ARGS(handle->prof.queryMatch));
   printf("REMOVE \t"OP_STAT_FMT"\n",OP_STAT_ARGS(handle->prof.remove));
   printf("INDEX RM \t"OP_STAT_FMT"\n",OP_STAT_ARGS(handle->prof.indexRemove));
   printf("INDEX IN \t"OP_STAT_FMT"\n",OP_STAT_ARGS(handle->prof.indexInsert));
   printf("INDEX UP \t"OP_STAT_FMT"\n",OP_STAT_ARGS(handle->prof.indexUpdate));
}

typedef uint8_t (*IndexFullFn)(XTDBHandle* handle,const char* indexName,DataBaseBE* db,void* args);
typedef uint8_t (*IndexDBFn)(DataBaseBE* db);
typedef void (*IndexDBNoResFn)(DataBaseBE* db);

uint8_t 
XTDBForAllIDX(XTDBHandle* handle,IndexFullFn fullFn,IndexDBFn dbFn,IndexDBNoResFn noResFn ,void* args) {
   uint8_t rv = True;
   
/*   HashTableIterator iter;
   hash_table_iterate(handle->indexes,&iter);
   while (hash_table_iter_has_more(&iter)) {
      DataBaseBE* be = hash_table_iter_next(&iter);
      rv = dbFn(NULL,db,args);
      if (!rv) {
         return rv;
      }
      //DBClose(be);
      //DBFree(be);
   }*/
   tcmapiterinit(handle->indexes);
   while (1) {
      int len,dataLen;
      DataBaseBE *outBase = NULL;
      const void* key = tcmapiternext(handle->indexes,&len);
      if (!key) {
         break;
      }
      const void *pPtr = tcmapget(handle->indexes,key,len,&dataLen);
      if (!pPtr) {
         continue;
      }
      memcpy(&outBase,pPtr,dataLen);
      if (fullFn) {
         rv = fullFn(handle,key,outBase,args);
      }
      if (dbFn) {
         rv = dbFn(outBase);
         if (rv) {
            // XXX proper error
            handle->error = XTDB_IO_ERR;
         }
      }
      if (noResFn) {
         noResFn(outBase);
         rv = True;
      }
      //free(key);
      //free(pPtr);
      if (!rv){
         return rv;
      }
   }
   return rv;
}

// Checked

void
XTDBFreeHandle(XTDBHandle* handle){
   //PrintProfile(handle);
   if (handle->indexes) {
      XTDBForAllIDX(handle,NULL,DBClose,DBFree,NULL);
      tcmapdel(handle->indexes);
   }
/*   HashTableIterator iter;
   hash_table_iterate(handle->indexes,&iter);
   while (hash_table_iter_has_more(&iter)) {
      DataBaseBE* be = hash_table_iter_next(&iter);
      printf("Closing db %p \n",be);
      DBClose(be);
      DBFree(be);
   }*/
   if (handle->mainDB) {
      DBClose(handle->mainDB);
      DBFree(handle->mainDB);
   }
   if (handle->descDB) {
      DBClose(handle->descDB);
      DBFree(handle->descDB);
   }
   //hash_table_free(handle->indexes);
   StrFree(&handle->dbName);
   StrFree(&handle->dataDir);
   StrFree(&handle->descName);
   StrFree(&handle->mainDbFileName);
   memset(handle,0,sizeof(*handle));
   free(handle);
}

/* 
 * This function could be slow because of DBDeleteKeyVal
 * it creates iterator and jumps to the value and compares 
 * and deletes the value.
 * One optimization is possible here if the key being 
 * deleted is part of the query then the entire index key-values
 * have to be deleted instead of the value deletion.
 * Or index deletion can be skipped altogether lazily
 * delete during find.
 */

uint8_t
XTDBRemoveFromIndex(XTDBHandle* handle,BinaryStr* key,BinaryStr* data) {
   bson_oid_t oid;
   bson_iterator q;
   bson_type qType;
   uint8_t ret = True;
   _S_FN(indexRemove);
   
   bson_iterator_from_buffer(&q, data->data);

   while((qType = bson_iterator_next(&q))) { 
      const char* keyVal = bson_iterator_key(&q);
      //printf("%s\n",keyVal);
      DataBaseBE* db;
      BSONElem qVal;
      BSONElemInitFromItr(&qVal,&q);
      db = XTDBGetIndex(handle,keyVal);
      if (db) {
         bson outBson;
         BinaryStr out;
         ConvertToBStr(&qVal,&outBson);
         BsonToBStr(&outBson,&out);
         ret = DBDeleteKeyVal(db,&out,key);
         if (!ret) {
            handle->error = XTDB_NO_ENTRY;
            bson_destroy(&outBson);
            break;
         }
         bson_destroy(&outBson);
      }
   }
   _E_FN(indexRemove);
   return ret;
   //return DBSet(handle->mainDB,&key,&val,False);
}

// Checked
uint8_t
XTDBInsertToIndex(XTDBHandle* handle,BinaryStr* key,BinaryStr* data) {
   bson_oid_t oid;
   bson_iterator q;
   bson_type qType;
   uint8_t ret = True;
   _S_FN(indexInsert);
   
   if (!tcmaprnum(handle->indexes)) {
      _E_FN(indexInsert);
      return True;
   }
   
   bson_iterator_from_buffer(&q, data->data);
   while((qType = bson_iterator_next(&q))) { 
      const char* keyVal = bson_iterator_key(&q);
      //printf("keyval %s\n",keyVal);
      DataBaseBE* db;
      BSONElem qVal;
      BSONElemInitFromItr(&qVal,&q);
      db = XTDBGetIndex(handle,keyVal);
      if (db) {
         bson outBson;
         BinaryStr out;
         if (qType == BSON_ARRAY) {
            assert(0);
            bson_iterator si;
            bson_type t;
            bson_iterator_subiterator(&q,&si);
            while ((t=bson_iterator_next(&si))){
               if (ConvertToBStr(&qVal,&outBson)) {
                  handle->error = XTDB_NO_MEM;
                  return False;
               }
               BsonToBStr(&outBson,&out);
               ret = DBSet(db,&out,key,True);
               bson_destroy(&outBson);
               if (ret) {
                  handle->error = XTDB_IO_ERR;
                  goto error;
               }
            }
         }else {

            if (ConvertToBStr(&qVal,&outBson)) {
               handle->error = XTDB_NO_MEM;
               return False;
            }
            BsonToBStr(&outBson,&out);
            ret = DBSet(db,&out,key,True);
            bson_destroy(&outBson);
            if (!ret) {
               handle->error = XTDB_IO_ERR;
               goto error;
            }
         }
      }

   }
   _E_FN(indexInsert);

   return ret;
error:
   // roll back is required?
   // can simply return error unnecessarily inserted elements will be eventually corrected when index is accessed
   return ret;
   //return DBSet(handle->mainDB,&key,&val,False);
}

typedef struct XTDBUpdateIndexArgs {
   bson_iterator oldItr,newItr;    
   bson oldBson,newBson;
   BinaryStr key;
} XTDBUpdateIndexArgs;

// XXX revisit after db return values
uint8_t
XTDBUpdateIndexIter(XTDBHandle* handle,const char* indexName,DataBaseBE* db,void* voidArgs) {
   XTDBUpdateIndexArgs * args = voidArgs;
   bson_type oldType, newType;
   bson oldBsonVal,newBsonVal;
   BinaryStr out1,out2;
   BSONElem qVal;
   uint8_t ret;
   handle->error = XTDB_OK;
   oldType = bson_find(&args->oldItr,&args->oldBson,indexName);
   newType = bson_find(&args->newItr,&args->newBson,indexName);
   if (!oldType && !newType) {
      // nothing to update
      return True;
   }
   if (!newType && oldType) {
      // new bson value does not have indexed field remove and return

      BSONElemInitFromItr(&qVal,&args->newItr);
      if (ConvertToBStr(&qVal,&newBsonVal)) {
         handle->error = XTDB_NO_MEM;
      }
      BsonToBStr(&newBsonVal,&out1);
      // XXX what are the reasons db delete can fail ?
      ret = DBDeleteKeyVal(db,&out1,&args->key);
      bson_destroy(&newBsonVal);
      if (ret) {
         return True;
      }
      handle->error = DBGetLastError(db);
      return False;
   }
   // Both old and new values have this field

   BSONElemInitFromItr(&qVal,&args->oldItr);
   if (ConvertToBStr(&qVal,&oldBsonVal)) {
      handle->error = XTDB_NO_MEM;
   }
   BsonToBStr(&oldBsonVal,&out1);

   BSONElemInitFromItr(&qVal,&args->newItr);
   if (ConvertToBStr(&qVal,&newBsonVal)) {
      handle->error = XTDB_NO_MEM;
   }
   BsonToBStr(&newBsonVal,&out2);

   if (BStrEqual(&out1,&out2)) {
      ret = True;
      goto freeBson;
   }
   ret = DBDeleteKeyVal(db,&out1,&args->key);
   if (!ret) {
      handle->error = DBGetLastError(db);
      goto freeBson;
   }
   ret = DBSet(db,&out2,&args->key,True);
   if (!ret) {
      handle->error = DBGetLastError(db);
      goto freeBson;
   }
freeBson:
   bson_destroy(&oldBsonVal);
   bson_destroy(&newBsonVal);
   return ret;
}

// Checked

uint8_t
XTDBUpdateIndex(XTDBHandle* handle,BinaryStr* key,BinaryStr* newData,BinaryStr* oldData) {
   XTDBUpdateIndexArgs args;
   _S_FN(indexUpdate);
   bson_iterator_from_buffer(&args.oldItr,oldData->data);
   bson_iterator_from_buffer(&args.newItr,newData->data);
   bson_init_finished_data(&args.oldBson,(char*)oldData->data);
   bson_init_finished_data(&args.newBson,(char*)newData->data);
   args.key = *key;
   uint8_t ret = XTDBForAllIDX(handle,XTDBUpdateIndexIter,NULL,NULL,&args);
   _E_FN(indexUpdate);
   return ret;

}


// XXX revisit after DB eror
uint8_t
XTDBCreateIndex(XTDBHandle* handle,char* fieldName) {
   String idxDBName;
   String descKey;
   bson idxDesc; 
   BinaryStr key,value,data;
   DataBaseBE* db;
   int rc;

   StrInit(&descKey);
   if(StrAppendFmt(&descKey,"index.value.%s",fieldName)) {
      handle->error = XTDB_NO_MEM;
      return False;
   }
   StrToBStr(descKey.ptr,&key);
   if (DBGet(handle->descDB,&key,&value)){
      // Index already exists
      printf("Index already exists.\n");
      StrFree(&descKey);
      BinaryStrFree(&value);
      handle->error= XTDB_INDEX_EXISTS;
      return False;
   }
   bson_init(&idxDesc);
   if (bson_append_string(&idxDesc,"name",fieldName)) {
      handle->error = XTDB_NO_MEM;
      bson_destroy(&idxDesc);
      StrFree(&descKey);
      return False;
   }
   if (bson_finish(&idxDesc)) {
      handle->error = XTDB_NO_MEM;
      bson_destroy(&idxDesc);
      StrFree(&descKey);
      return False;
   }

   BsonToBStr(&idxDesc,&value);
   if (!DBSet(handle->descDB,&key,&value,False)) {
      printf("Error Adding to index names list\n");
      handle->error = DBGetLastError(handle->descDB);
      bson_destroy(&idxDesc);
      StrFree(&descKey);
      return False;
   }
   StrFree(&descKey);
   StrToBStr(IDXFIELDNAME,&key);
   StrToBStr(fieldName,&value);
   if (!DBSet(handle->descDB,&key,&value,True)) {
      //printf("Error Adding to index names list\n");
      handle->error = DBGetLastError(handle->descDB);
      //handle->error = XTDB_IO_ERR;
      bson_destroy(&idxDesc);
      return False;
   }
   bson_destroy(&idxDesc);

   if (GetIndexDBName(handle,fieldName,&idxDBName)) {
      handle->error = XTDB_NO_MEM;
      return False;
   }

   db = DBInit(STR(&idxDBName),BDBOWRITER | BDBOCREAT);
   StrFree(&idxDBName);
   if (!db) {
      handle->error = XTDB_NO_MEM;
      return False;          
   }

   rc = DBOpen(db,BDBOCREAT|BDBOWRITER);
   if (!rc) {
      handle->error = DBGetLastError(db);
      DBFree(db);
      return False;
   }
   DBClose(db);
   DBFree(db);
   if (!XTDBAddIndex(handle,fieldName)) {
      // AddIndex failed return
      printf("Error Adding to index names list\n");
      return False;
   }
   assert(XTDBGetIndex(handle,fieldName));
   void* iter = DBIter(handle->mainDB);
   while (DBIterCur(handle->mainDB,iter,&key,&data)) {
      //printf("Adding value to index\n");
      bson obj;
      BStrToBson(&data,&obj);
      if (!XTDBInsertToIndex(handle,&key,&data)) {
         return False;
      }
      BinaryStrFree(&key);
      BinaryStrFree(&data);
      DBIterNext(handle->mainDB,iter);
   }
   DBIterFree(handle->mainDB,iter);
   return True;
}

// Checked

uint8_t
XTDBDropIndex(XTDBHandle* handle,char* fieldName){
   //void * iter = DBIter(handle->descDB); 
   uint8_t deleted=False;
   String descKey,idxDBName;
   BinaryStr key,value;
   DataBaseBE* idxDB;

   idxDB = XTDBGetIndex(handle,fieldName);
   if (!idxDB) {
      handle->error = XTDB_NO_ENTRY;
      return False;
   }
   StrInit(&descKey);
   if (StrAppendFmt(&descKey,"index.value.%s",fieldName)){
      handle->error = XTDB_NO_MEM;
      return False;
   }
   StrToBStr(IDXFIELDNAME,&key);
   StrToBStr(fieldName,&value);
   DBDeleteKeyVal(handle->descDB,&key,&value);
   DBClose(idxDB);

   if (!XTDBRemoveIndex(handle,fieldName)) {
      return False;
   }

   DBFree(idxDB);
   StrToBStr(descKey.ptr,&key);
   DBDelete(handle->descDB,&key);
   if (GetIndexDBName(handle,fieldName,&idxDBName)) {
      handle->error = XTDB_NO_MEM;
      return False;
   }
   unlink(STR(&idxDBName));
   StrFree(&descKey);
   StrFree(&idxDBName);
   return True;      
}

// checked

uint8_t
XTDBInsert(XTDBHandle* handle,bson* newVal) {
   _S_FN(insert);
   handle->gen++;
   bson_oid_t oid;
   bson_oid_gen(&oid);
   BinaryStr key,val;
   uint8_t ret;
   bson copyObj;

   handle->lastOp = XTDB_OP_INSERT;
   key.data = oid.bytes;
   key.len  = sizeof(oid.bytes);
   if (bson_copy(&copyObj,newVal)) {
      handle->error = XTDB_INVALID_BSON;
      return False;
   }
   bson_unfinish_object(&copyObj);
   if (bson_append_oid(&copyObj,"_id",&oid)) {
      handle->error = XTDB_NO_MEM;
      bson_destroy(&copyObj);
      return False;
   }
   if (bson_finish(&copyObj)) {
      handle->error = XTDB_NO_MEM;
      bson_destroy(&copyObj);
      return False;
   }
   //bson_print(&copyObj);
   BsonToBStr(&copyObj,&val);
   
   if (XTDBInsertToIndex(handle,&key,&val) == False) {
      assert(0);
      return False;
   }

   ret = DBSet(handle->mainDB,&key,&val,False);
   _E_FN(insert);
   if (!ret) {
      handle->error = DBGetLastError(handle->mainDB);
   } else {
      memcpy(&handle->status.insertStatus.newOid,&oid,sizeof(oid));
   }
   return ret;
}

// Checked
static inline uint8_t 
XTDBCursorNextBStr(XTDBCursor* cursor,
              BinaryStr* outKey,
              BinaryStr* outVal){
   XTDBHandle* handle = cursor->mdbHandle;
   _S_FN(curNext);
   if (cursor->usingIndex) {
      if (!cursor->dbIter) {
         return False;
      }
      if (cursor->first) {
         cursor->first = False;
      } else{
         //
         cursor->idx++;
      }
      int64_t listLen = DBListLen(cursor->curDB,cursor->dbIter);
      while (cursor->idx < listLen ) {
         BinaryStr oid,bsonVal;
         DBListVal(cursor->curDB,cursor->dbIter,cursor->idx,&oid);
         if (cursor->usingIndex == OTHER_INDEX) {
            DataBaseBE* mainDB = cursor->mdbHandle->mainDB;
            if (!DBGet(mainDB,&oid,&bsonVal)) {
               // value not in main db ideally we should delete from the index
               cursor->idx++;
               continue;
            }
         } else {
            // using main db as index so we directly get oid
            bsonVal = oid;
         }
         
         _S_FN(queryMatch);
         if (QueryMatch(&cursor->query,&bsonVal)) {
            if (!(outKey->data = malloc(oid.len))) {
               cursor->error = XTDB_NO_MEM;
               return False;
            }
            outKey->len = oid.len;
            memcpy((char*)outKey->data,oid.data,oid.len);
            /*
            assert(outVal->data = malloc(newVal.len));
            memcpy((void*)outVal->data,newVal.data,newVal.len);*/
            *outVal = bsonVal;
            //cursor->idx++;
            _E_FN(curNext);
            _E_FN(queryMatch);
            return True;
         } else {
            _E_FN(queryMatch);
            BinaryStrFree(&bsonVal);
         }
         cursor->idx++;
      }
      _E_FN(curNext);
      cursor->error = XTDB_OK;
      return False;
   } else {
      if (cursor->first) {
         cursor->first = False;
      } else{
         //
         cursor->error = XTDB_OK;
         if (handle->gen != cursor->gen) {
            if (!cursor->nextKeyStatus) {
               return False;
            }
            DBIterFree(cursor->curDB,cursor->dbIter);
            if (!(cursor->dbIter = DBIter(handle->mainDB)) ){
               cursor->error = XTDB_NO_MEM;
               return False;
            }
            assert(cursor->dbIter);
            if (!DBIterJump(cursor->curDB,cursor->dbIter,&cursor->lastKey)) {
               return False;
            }
            BinaryStrFree(&cursor->lastKey);
            cursor->nextKeyStatus = False;
            cursor->gen = handle->gen;
         } 
      }
      while (DBIterCur(cursor->curDB,cursor->dbIter,outKey,outVal)){
         //return True;
         if (QueryMatch(&cursor->query,outVal)) {
            //DBIterNext(cursor->dbIter);
            if (!(cursor->lastKey.data = malloc(outKey->len))) {
               cursor->error = XTDB_NO_MEM;
               return False;
            }
            cursor->lastKey.len =outKey->len;
            memcpy((void*)cursor->lastKey.data,outKey->data,outKey->len);
            if (DBIterNext(cursor->curDB,cursor->dbIter)) {
               // save the next key before returning the value used in next call of this function
               cursor->nextKeyStatus = DBIterCur(cursor->curDB,cursor->dbIter,&cursor->lastKey,NULL);
            }
            _E_FN(curNext);
            return True;
         }
         //printf(" **** Query not matched ****\n");
         BinaryStrFree(outKey);
         BinaryStrFree(outVal);
         DBIterNext(cursor->curDB,cursor->dbIter);
      }
      _E_FN(curNext);
      return False;
   }
   return False;
}

// Checked
uint8_t 
XTDBCursorNext(XTDBCursor* cursor,
              BinaryStr* outKey,
              bson* outVal){
   BinaryStr val;
   uint8_t rv;
   rv = XTDBCursorNextBStr(cursor,outKey,&val);
   if (rv) {
      bson_init_finished_data(outVal,(char*)val.data);
   }
   return rv;   
}

void
XTDBCursorFree(XTDBCursor* cursor) {
   if (!cursor->usingIndex && cursor->dbIter){
      DBIterFree(cursor->curDB,cursor->dbIter);
   } else{
      if (cursor->dbIter) {
         DBListFree(cursor->curDB,cursor->dbIter);
      }
   }
   BinaryStrFree(&cursor->lastKey);
   BinaryStrFree(&cursor->query);
   free(cursor);
}

uint8_t
XTDBFindById(XTDBHandle* handle,BinaryStr* oid,bson* outVal){
   BinaryStr outBStr;
   uint8_t rv;
   rv = DBGet(handle->mainDB,oid,&outBStr);
   if (rv) {
      bson_init_finished_data(outVal,(char*)outBStr.data);
   }
   return rv;
}

// Checked

/*
 * On success returns a cursor for the query specified by bson object query.
 *
 * Incase of errors NULL is returned and the error field is set in the handle.
 * possible error codes
 *    -- XTDB_NO_MEM
 *
 * Handle will be used by the corresponding XTDBCursorNext calls so the handle must be valid till XTDBCursorFree is called.
 *
 */

XTDBCursor*
XTDBFind(XTDBHandle* handle,bson* query) {
   BinaryStr key,data,keyData;
   bson_iterator q;
   bson_type qType;
   XTDBCursor * cur ;
   ListNode idxNameList;
   char * minKey = NULL;
   uint64_t minWeight = -1,weight;
   DataBaseBE* db = NULL,*minDB;
   BinaryStr idxVal,minIndexVal;
   BinaryStr origQuery;
   bson outBson;

   _S_FN(find);
   cur= malloc(sizeof(XTDBCursor));
   if (!cur) {
      handle->error = XTDB_NO_MEM;
      return NULL;
   }
   memset(cur,0,sizeof(*cur));
   cur->first = True;
   BsonToBStr(query,&origQuery);
   if (BinaryStrDup(&cur->query,&origQuery)) {

      handle->error = XTDB_NO_MEM;
      goto error;
   }
   bson_iterator_from_buffer(&q,query->data);
   QueryGetIndexesToUse(&cur->query,&idxNameList);

   minIndexVal.data = NULL;
   minIndexVal.len = 0;

   while (!LIST_ISEMPTY(&idxNameList)) {
      ListNode* node = LIST_FIRST(&idxNameList);
      char* key = node->data;
      LIST_REMOVE(&idxNameList,LIST_FIRST(&idxNameList));
      qType = bson_find(&q,query,key);
      assert(qType); 
      BSONElem qVal;
      BSONElemInitFromItr(&qVal,&q);
      db = XTDBGetIndex(handle,key);
      if (db) {
         if (ConvertToBStr(&qVal,&outBson)) {
            handle->error = XTDB_NO_MEM;
            goto error;
         }
         BsonToBStr(&outBson,&idxVal);
         weight = DBCount(db,&idxVal); 
         if (minWeight >  weight) {
            //BinaryStrFree(&minIndexVal);
            BStrToBson(&minIndexVal,&outBson);
            bson_destroy(&outBson);
            minIndexVal = idxVal;
            free(minKey);
            minWeight = weight;
            minKey = strdup(key); 
            
            minDB = db;
         } else {
            bson_destroy(&outBson);
         }
      }
      free(node);
      free(key);
   }

   cur->mdbHandle = handle;
   if (minWeight == -1) {
      cur->dbIter = DBIter(handle->mainDB);
      if (!cur->dbIter) {
         handle->error = XTDB_NO_MEM;
         goto error;
      }
      cur->usingIndex = NO_INDEX;
      cur->curDB = handle->mainDB;
   } else {
      assert(minDB);
      cur->curDB = minDB;
      //printf("using index %s %d \n",minKey,minWeight);
      cur->usingIndex = OTHER_INDEX;
      if (!DBGetList(minDB,&minIndexVal,&cur->dbIter)) {
         handle->error = DBGetLastError(minDB);
         if (handle->error == XTDB_NO_ENTRY) {
            handle->error = XTDB_OK;
         } else {
            goto error;
         }
      }
      BinaryStrFree(&minIndexVal);

      if (!cur->dbIter) {
         //printf("Warning Value not found in index '%s'\n",idxVal);
      } else {
         uint32_t count = DBListLen(minDB,cur->dbIter);
         if (count >10) {
         }

      }
      //assert(cur->dbIter);
      cur->idx = 0;
      free(minKey);
   }

   _E_FN(find);
   return cur;
error:
   _E_FN(find);
   XTDBCursorFree(cur);
   return NULL;
}

uint8_t XTDBBsonAppendOid(XTDBHandle* handle,bson* in , bson* out,bson_oid_t * oid) {

   if (bson_copy(out,in)) {
      handle->error = XTDB_INVALID_BSON;
      return False;
   }
   bson_unfinish_object(out);
   if (bson_append_oid(out,"_id",oid)) {
      handle->error = XTDB_NO_MEM;
      bson_destroy(out);
      return False;
   }
   if (bson_finish(out)) {
      handle->error = XTDB_NO_MEM;
      bson_destroy(out);
      return False;
   }
   return True;
}

/* 
 * Sets all values matched by the particular query to the value specified in "newVal".
 * If no entry is found and upsert flag is True then new entry will be inserted.
 *
 * Return value:
 * On success:
 *     -- True
 * On Failure:
 *     -- False and error field will be set in handle
 *     possible error codes
 *         -- XTDB_IO_ERR
 *         -- XTDB_NO_MEM
 */

uint8_t
XTDBUpdate(XTDBHandle* handle,bson* query,bson* newVal,uint8_t upsert) {

   XTDBCursor* cur ; 
   BinaryStr key,value,newValueStr;
   uint8_t updated = False,ret=True;
   int nUpdated = 0;
   bson_oid_t oid;
   bson withOid;
   
   int isMod;
   XTDBUpdateStatus status; 

   status.upserted = False;
   _S_FN(update);
   handle->lastOp = XTDB_OP_UPDATE;
   if (!(cur = XTDBFind(handle,query))) {
      return False;
   }
   BsonToBStr(newVal,&newValueStr);
   if ((handle->error = ObjModValidateValBson(newVal,&isMod))) {
      return False;
   }

   while (XTDBCursorNextBStr(cur,&key,&value)) {
      handle->gen++;
      //updated = True;
      // retain the oid with new object
      if (!isMod) {

         memcpy(oid.bytes,key.data,sizeof(oid.bytes));
         if(!XTDBBsonAppendOid(handle,newVal,&withOid,&oid)) {
            return False;
         }
      } else {
         bson obj;
         bson error;
         bson outObj;
         BStrToBson(&value,&obj);
         handle->error = ObjModifyBson(&obj,newVal,&withOid,&error);
         if (handle->error) {
            bson_iterator itr;
            bson_iterator_init(&itr,&error);
            bson_find(&itr,&error,"error");
            strncpy(handle->errorString,bson_iterator_string(&itr),sizeof(handle->errorString));
            bson_destroy(&error);
            return False;
         }
      }
      
      BsonToBStr(&withOid,&newValueStr);
      // Remove entry from index and re-insert
      if (!XTDBUpdateIndex(handle,&key,&newValueStr,&value)) {
         XTDBCursorFree(cur);
         BinaryStrFree(&key);
         BinaryStrFree(&value);
         bson_destroy(&withOid);

         return False;
      }
      //XTDBInsertToIndex(handle,&key,&newValueStr);
      //XTDBRemoveFromIndex(handle,&key,&value);
      if (!DBSet(handle->mainDB,&key,&newValueStr,False)) {
         XTDBCursorFree(cur);
         BinaryStrFree(&key);
         BinaryStrFree(&value);
         bson_destroy(&withOid);
         handle->error = DBGetLastError(handle->mainDB);
         return False;
      }

      BinaryStrFree(&key);
      BinaryStrFree(&value);
      bson_destroy(&withOid);
      nUpdated++;
   }

   if (cur->error != XTDB_OK) {
      handle->error = cur->error;
      return False;
   }

   if ( !nUpdated && upsert) {
      ret = XTDBInsert(handle,newVal);
      memcpy(&status.newOid,&handle->status.insertStatus.newOid,sizeof(status.newOid));
      status.upserted = True;
      nUpdated = 1;
   }
   status.nUpdated = nUpdated;
   handle->lastOp = XTDB_OP_UPDATE;
   handle->status.updateStatus = status;
   XTDBCursorFree(cur);
   _E_FN(update);
   return ret;
}

uint8_t
XTDBSyncIndexIter(XTDBHandle* handle,const char* indexName,DataBaseBE* db,void* voidArgs) {
   return DBSync(db,*((uint8_t*)voidArgs));
}

// Checked
uint8_t
XTDBSync(XTDBHandle* handle,uint8_t diskSync){
   _S_FN(sync);
   if (!DBSync(handle->mainDB,diskSync)) {
      handle->error = XTDB_IO_ERR;
      return False;
   }
   /*DBClose(handle->mainDB);
   DBOpen(handle->mainDB,BDBOCREAT|BDBOWRITER);*/
   /*HashTableIterator iter;
   hash_table_iterate(handle->indexes,&iter);
   while (hash_table_iter_has_more(&iter)) {
      DataBaseBE* be = hash_table_iter_next(&iter);

      DBSync(be);

   }*/
   
   if (!XTDBForAllIDX(handle,XTDBSyncIndexIter,NULL,NULL,&diskSync)) {
      return False;
   }
   if (!DBSync(handle->descDB,diskSync)) {
      handle->error = XTDB_IO_ERR;
      return False;
   }
   _E_FN(sync);
}

// Checked
uint8_t
XTDBUnlinkIndexIter(XTDBHandle* handle,const char* indexName,DataBaseBE* db,void* voidArgs) {
   // XXX not checking unlink errors cant do anything
   handle->error = XTDB_OK;
   //DBClose(db);
   if (!unlink(DBName(db))) {
      if (errno == ENOENT) {
         return True;
      }
      handle->error = XTDB_IO_ERR;
      return False;   
   }
   DBFree(db);
   return True;
}

//Checked
XTDBError
XTDBDrop(XTDBHandle* handle){
   if (!XTDBForAllIDX(handle,XTDBUnlinkIndexIter,NULL,NULL,NULL)) {
      return handle->error;
   }
   
   
   //unlink();
   /*HashTableIterator iter;
   hash_table_iterate(handle->indexes,&iter);
   while (hash_table_iter_has_more(&iter)) {
      DataBaseBE* be = hash_table_iter_next(&iter);
      
      unlink(DBName(be));
      //DBClose(be);
      //DBFree(be);

      /*DBClose(be);
      DBOpen(be,BDBOWRITER);*
   }*/
   unlink(DBName(handle->descDB));
   unlink(DBName(handle->mainDB));

   XTDBFreeHandle(handle);
   return XTDB_OK;
}

/*
 * Removes all entries matching the query from db and its indexes.
 *
 * Return value:
 *    -- True - On success
 *    -- False - On failure with error value set in handle
 *       Possible error codes
 *       -- XTDB_NO_MEM
 *       --
 *
 */ 

uint8_t
XTDBRemove(XTDBHandle* handle,bson* query){
   BinaryStr key,value,newValueStr;
   uint8_t updated = False,ret=True;
   XTDBCursor* cur = XTDBFind(handle,query);

   if (!cur) {
      return False;
   }

   while (XTDBCursorNextBStr(cur,&key,&value)) {
      handle->gen++;
      _S_FN(remove);
      if (!DBDelete(handle->mainDB,&key)) {
         handle->error = DBGetLastError(handle->mainDB);
         BinaryStrFree(&key);
         BinaryStrFree(&value);
         goto error;
      }
      // Remove from indexes
      /*if (!XTDBRemoveFromIndex(handle,&key,&value)) {
         BinaryStrFree(&key);
         BinaryStrFree(&value);
         goto error;
      }*/
      BinaryStrFree(&key);
      BinaryStrFree(&value);
      _E_FN(remove);
   }
   if (cur->error != XTDB_OK) {
      handle->error = cur->error;
   }
   XTDBCursorFree(cur);
   return True;
error:
   XTDBCursorFree(cur);
   return False;
}

int64_t
XTDBCount(XTDBHandle* handle, bson* query){

   XTDBCursor* cur;
   BinaryStr key,value;
   int64_t i=0;
   
   handle->error = XTDB_OK;
   if (!query) {
      return DBCount(handle->mainDB,NULL);
   }

   cur= XTDBFind(handle,query);
   if (!cur) {
      if (handle->error == XTDB_NO_ENTRY) {
         handle->error = XTDB_OK;
      }
      return 0;
   }
   while (XTDBCursorNextBStr(cur,&key,&value)) {
      i++;
      BinaryStrFree(&key);
      BinaryStrFree(&value);
   }
   XTDBCursorFree(cur);
   return i; 
}

XTDBError XTDBGetLastError(XTDBHandle* handle){
   return handle->error;
}

void XTDBGetLastErrorBson(XTDBHandle* handle,bson* error) {
   bson_init(error);
   bson_append_int(error,"errno",handle->error);
   bson_append_string(error,"errstr", handle->errorString);
   if (handle->error) {
      bson_finish(error);
   }
   if (handle->lastOp == XTDB_OP_INSERT ) {
      bson_append_oid(error,"inserted",&handle->status.insertStatus.newOid);
   }
   if (handle->lastOp == XTDB_OP_UPDATE ) {
      bson_append_int(error,"nupdated",handle->status.updateStatus.nUpdated);
      if (handle->status.updateStatus.upserted) {
         bson_append_oid(error,"upserted",&handle->status.updateStatus.newOid);
      }
   }
   bson_finish(error);
}
