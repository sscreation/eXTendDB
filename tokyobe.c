
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

#include "tokyobe.h"
#include <assert.h>
#include <fcntl.h>
#include <sys/types.h>

XTDBError TokyoBEErrorToXTDBError(int error) {
 switch(error) {
   case  TCESUCCESS:
      return XTDB_OK;
   case TCETHREAD:
   case TCEINVALID:
      return XTDB_OTHER_ERR;
   case TCENOFILE:
      return XTDB_FILE_NOT_FOUND;
   case TCENOPERM:
   case TCELOCK:
      return XTDB_PERM_DENIED;
   case TCEMETA:
   case TCERHEAD:
      return XTDB_CORRUPT_DB;
   case TCEOPEN:
      return XTDB_DB_OPEN_FAILED;
   case TCECLOSE:
   case TCETRUNC:
   case TCESEEK:
   case TCEREAD:
   case TCESYNC:
   case TCEWRITE:
   case TCEMMAP:
   case TCEUNLINK:
   case TCERENAME:
   case TCEMKDIR:
   case TCERMDIR:
      return XTDB_IO_ERR; 
   case TCEKEEP:
      return XTDB_ENTRY_EXISTS;
   case TCENOREC:
      return XTDB_NO_ENTRY;
   default:
      return XTDB_OTHER_ERR;
};

}

XTDBError TokyoBEGetLastError(void* arg) {
   TokyoBackEnd* tcbe = arg;
   TCBDB* bdb = tcbe->dbHandle;
   return TokyoBEErrorToXTDBError(tcbdbecode(bdb)); 
}

void*
TokyoBEInit(const char* name,uint32_t mode) {
   TokyoBackEnd* tc = malloc(sizeof(TokyoBackEnd));
   if (!tc) {
      return NULL;
   }
   StrInit(&tc->dbFileName);
   StrAppend(&tc->dbFileName,name);
   if (!(tc->dbHandle = tcbdbnew())) {
      StrFree(&tc->dbFileName);
      free(tc);
      return NULL;
   }
   tcbdbsetmutex(tc->dbHandle);
   return tc;
}

void
TokyoBEFree(void* arg){
   TokyoBackEnd* tc = arg;
   assert(tc);
   tcbdbdel(tc->dbHandle);
   StrFree(&tc->dbFileName);
   free(tc);
}

int
TokyoBEListLen(void* arg) {
   TCLIST* list = arg;
   return tclistnum(list);
}

void*
TokyoBEGetList(void* arg,const BinaryStr* key) {
   TokyoBackEnd* tcbe = arg;
   TCBDB* bdb = tcbe->dbHandle;
   return tcbdbget4(bdb,key->data,key->len);
}

uint8_t
TokyoBEGet(void* arg,const BinaryStr* key,BinaryStr* outVal) {
   TokyoBackEnd* tcbe = arg;
   TCBDB* bdb = tcbe->dbHandle;
   outVal->data =  tcbdbget(bdb,key->data,key->len,&outVal->len);
   if (outVal->data) {
      return True;
   }
   
   return False;
}

uint8_t
TokyoBEDelete(void* arg,const BinaryStr* key) {
   TokyoBackEnd* tcbe = arg;
   TCBDB* bdb = tcbe->dbHandle;
   return tcbdbout3(bdb,key->data,key->len);
}

uint64_t
TokyoBECount(void* arg,const BinaryStr* key) {
   TokyoBackEnd* tcbe = arg;
   TCBDB* bdb = tcbe->dbHandle;
   if (key) {
      return tcbdbvnum(bdb,key->data,key->len);
   }
   return tcbdbrnum(bdb);
}

#define BSTR_FMT "%p:%u"
#define BSTR_ARGS(bStr) (bStr)->data, (bStr)->len
#define MAX_OBJ_LEN 4*1024
#define MAX_KEY_LEN 4*1024

uint8_t
TokyoBESet(void* arg,const BinaryStr* key,BinaryStr* newVal,uint8_t allowDup) {
   TokyoBackEnd* tcbe = arg;
   TCBDB* bdb = tcbe->dbHandle;
   int rv;
   if (key->len > MAX_KEY_LEN) {
      assert(0);
   }
   if (newVal->len > MAX_OBJ_LEN) {
      assert(0);
   }

   if (!allowDup) {
      rv = tcbdbput(bdb,key->data,key->len,newVal->data,newVal->len);
   } else {
      rv =tcbdbputdup(bdb,key->data,key->len,newVal->data,newVal->len);
   }
   //tcbdbsync(bdb);
   if (!rv) {
      rv = tcbdbecode(bdb);
      printf("Return value %d,%s\n",rv,tcbdberrmsg(rv));
   }
   return rv;
}

uint8_t
TokyoBEOpen(void* arg,int mode) {
   TokyoBackEnd* tcbe = arg;
   TCBDB* bdb = tcbe->dbHandle;
   //tcbdbsetcache(bdb,102400,102400);
   uint8_t rs = tcbdbopen(bdb,tcbe->dbFileName.ptr,mode);
#ifdef DEBUGFD
   char f[1024];
   snprintf(f,sizeof(f),"%s-debug.out",tcbe->dbFileName.ptr);
   int fd = open(f,O_RDWR|O_CREAT); 
   tcbdbsetdbgfd(bdb,fd);
#endif
   if (!rs) {
      int rv = tcbdbecode(bdb);
      if (rv && rv!=22) {
         printf("Opening file %s\n",tcbe->dbFileName.ptr);
         printf("Return value %d,%s\n",rv,tcbdberrmsg(rv));
      }
   }
   return rs;
}

int
TokyoBEClose(void* arg) {
   TokyoBackEnd* tcbe = arg;
   TCBDB* bdb = tcbe->dbHandle;
   return tcbdbclose(bdb);
}

int
TokyoBESync(void* arg,uint8_t diskSync) {
   TokyoBackEnd* tcbe = arg;
   TCBDB* bdb = tcbe->dbHandle;
   return tcbdbmemsync(bdb,diskSync);
}


void
TokyoBEIterFree(void* arg) {
   BDBCUR* bcur= arg;
   tcbdbcurdel(bcur);
}

int i__j=0;
#define CE(rv,bdb)\
   if (!rv){\
   int __rv = tcbdbecode(bdb);\
   if (__rv) {\
      printf("%s %d\n",tcbdberrmsg(__rv),i__j++);\
      /*printf("%s:%s %s %s\n" __FILE__, __LINE__, __func__,tcbdberrmsg(__rv));\*/\
   }}

uint8_t
TokyoBEIterNext(void* arg){
   BDBCUR* bcur= arg;
   TCBDB *bdb = bcur->bdb;
   uint8_t rv = tcbdbcurnext(bcur);
   //CE(rv,bdb);
   return rv;
}

uint8_t
TokyoBEIterPrev(void* arg){
   BDBCUR* bcur= arg;
   TCBDB *bdb = bcur->bdb;
   uint8_t rv = tcbdbcurprev(bcur);
   //CE(rv,bdb);
   return rv;
}

uint8_t
TokyoBEIterCurDelete(void* arg){
   BDBCUR* bcur= arg;
   uint8_t rv;// = tcbdbcurprev(bcur);
   rv = tcbdbcurout(bcur);
   tcbdbcurprev(bcur);
   return rv;

}

uint8_t
TokyoBEIterCur(void* arg,BinaryStr* outKey,BinaryStr* outVal){
   BDBCUR* bcur= arg;
   outKey->data = tcbdbcurkey(bcur,&outKey->len);
   if (!outKey->data) {
      return False;
   }   
   if (outVal){
      outVal->data = tcbdbcurval(bcur,&outVal->len);
   }

   return True;
}

uint8_t
TokyoBEIterJump(void* arg,BinaryStr* key) {
   BDBCUR* cursor= arg;
   return tcbdbcurjump(cursor,key->data,key->len);
}

void*
TokyoBEIter(void* arg) {
   TokyoBackEnd* tcbe =arg;
   TCBDB* bdb = tcbe->dbHandle;
   BDBCUR* cur = tcbdbcurnew(bdb);
   if (!cur) {
      return NULL;
   }
   tcbdbcurfirst(cur);
   return cur; 
}

int
TokyoBEListVal(void* arg,int index,BinaryStr* out){
   TCLIST *list = arg;
   out->data = (char*)tclistval(list,index,&out->len);
   return 0;  
}

void TokyoBEListFree(void* arg) {
   TCLIST* list=arg;
   tclistdel(list);
}

DBBEOps tokyoBEOps = {
   .init          = TokyoBEInit,
   .free          = TokyoBEFree,
   .open          = TokyoBEOpen,
   .close         = TokyoBEClose,
   .sync          = TokyoBESync,

   .get           = TokyoBEGet, 
   .getList       = TokyoBEGetList,
   .listLen       = TokyoBEListLen,
   .listVal       = TokyoBEListVal,
   .listFree      = TokyoBEListFree,
   .set           = TokyoBESet,
   .del           = TokyoBEDelete,
   .count         = TokyoBECount,

   .iter          = TokyoBEIter, 
   .iterFree      = TokyoBEIterFree,
   .iterCurrent   = TokyoBEIterCur,
   .iterNext      = TokyoBEIterNext,
   .iterPrev      = TokyoBEIterPrev,
   .iterCurDelete = TokyoBEIterCurDelete,
   .iterJump      = TokyoBEIterJump,
   
   .getLastError = TokyoBEGetLastError,

};
