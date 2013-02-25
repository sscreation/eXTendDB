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

#include <stdio.h>
#define MONGO_HAVE_STDINT
#include "bson.h"
#include <assert.h>
#include "query.h"
#include "list.h"
#include "hash.h"

HashTable* opTable;

#define INTCMP(i1,i2) ((i1) > (i2) ? 1 : ((i1) == (i2) ? 0:-1))

typedef struct BSONElemConverter{
   BSONElemConverterFn cFuncs[BSON_TYPE_LAST];
} BSONElemConverter;

uint8_t
QueryObjMatch(bson* obj,bson* query);

BSONElemOps elemOps[BSON_TYPE_LAST];

int 
RegisterBSONCmp(bson_type type,BSONElemCompareFn fn) {
   if (type > BSON_TYPE_LAST) {
      assert(0);
      return -1;
   }
   elemOps[type].compare = fn;
   return 0;
}

int 
RegisterBSONToBStr(bson_type type,BSONElemToBStr fn) {
   if (type > BSON_TYPE_LAST) {
      assert(0);
      return -1;
   }
   elemOps[type].datatobstr = fn;
   return 0;
}

#define DEFINE_CONV(fnName) BSONElem* fnName(BSONElem* const from,BSONElem* const to)

static inline int ElemToDouble(BSONElem* val2,double* outVal){

   int32_t int32;
   double d2;
   int64_t lng;
   double doVal;

   switch(val2->type) {
      case BSON_INT:
         bson_little_endian32(&int32,val2->value);
         d2 = int32;
         break;
      case BSON_LONG:
      case BSON_DATE:
         bson_little_endian64(&lng,val2->value);
         d2 = lng;
         break;
      case BSON_DOUBLE:
         bson_little_endian64(&doVal,val2->value);
         d2 = doVal;
         break;
      default:
         return -1;
   }
   *outVal = d2;
   return 0;
}

static inline int ElemToLong(BSONElem* val2,int64_t* outVal){

   int32_t int32;
   int64_t d2;
   int64_t lng;
   double doVal;

   switch(val2->type) {
      case BSON_INT:
         bson_little_endian32(&int32,val2->value);
         d2 = int32;
         break;
      case BSON_LONG:
      case BSON_DATE:
         bson_little_endian64(&lng,val2->value);
         d2 = lng;
         break;
      case BSON_DOUBLE:
         bson_little_endian64(&doVal,val2->value);
         d2 = doVal;
         break;
      default:
         return -1;
   }
   *outVal = d2;
   return 0;
}
static inline int ElemToInt(BSONElem* val2,int32_t* intVal) {
   int32_t d1,d2;
   int64_t lng;
   double doVal;

   switch(val2->type) {
      case BSON_INT:
         bson_little_endian32(&d2,val2->value);
         break;
      case BSON_DATE:
      case BSON_LONG:
         bson_little_endian64(&lng,val2->value);
         d2 = lng;
         break;
      case BSON_DOUBLE:
         bson_little_endian64(&doVal,val2->value);
         d2 = doVal;
         break;
      default:
         return -1;
   }
   *intVal = d2;
   return 0;
}

DEFINE_CONV(DoubleToInt) {
   assert(from->type == BSON_DOUBLE);
   assert(to->type   == BSON_INT);
   double d;
   uint64_t v;
   //to->value = to->buf;
   bson_little_endian64(&d,from->value);
   v = d;
   bson_little_endian64(to->value,(void*)&v);
}

DEFINE_CONV(DoubleToLong){
   assert(from->type == BSON_DOUBLE);
   assert(to->type   == BSON_LONG);
   double d;
   uint64_t v;
   //to->value = to->buf;
   bson_little_endian64(&d,from->value);
   v = d;
   bson_little_endian64(to->value,(void*)&v);
}

DEFINE_CMP(DoubleCmp) {
   double d1, d2;
   if (val1->type != BSON_DOUBLE) {
      return -1;
   }
   bson_little_endian64(&d1,val1->value);
   if (ElemToDouble(val2,&d2)) {
      return -1;
   }
   if (d1 == d2) {
      *cmpOut = 0;
   } else if (d1 < d2) {
      *cmpOut = -1;
   } else if (d1 > d2) {
      *cmpOut = 1;
   }
   return 0;
}

DEFINE_CMP(StringCmp) {
   *cmpOut = strcmp(val1->value+4,val2->value+4);
   //printf("\tComparing string '%s','%s': %d \n",val1->value+4,val2->value+4,*cmpOut);
   return 0;
}

DEFINE_CMP(ObjectCmp) {
   return -1;
}

DEFINE_CMP(ArrayCmp) {
   BSONElemItr it1,it2;
   BSONElem query,val;
   int cVal;
   if (val1->type != BSON_ARRAY || val2->type != BSON_ARRAY) {
      return -1;
   }
   BSONElemItrInit(&it1,val1);
   BSONElemItrInit(&it2,val2);
   while (1) {
      uint8_t t1,t2;
      t1 = BSONElemItrNext(&it1,&val);
      t2 = BSONElemItrNext(&it2,&query);

      if (!t1) {
         if (t1 != t2) {
            return -1;
         }
         break;
      }
      /*if (t1 != t2) {
         return -1;
      }*/
      // if CompareValue is used here instead of CompareElem then operators inside array elements can be supported.
      // as of now mongo does not do this so leaving it this way.
      if (CompareElem(&val,&query,&cVal) || cVal != 0){
         return -1;
      }
   }
   *cmpOut = 0;
   return 0;
}

DEFINE_CMP(BindataCmp) {
   int cVal;
   int t1,t2;
   const char *d1,*d2;
   if (val1->type != BSON_BINDATA || val2->type != BSON_BINDATA) {
      return -1;
   }
   t1 = bson_iterator_bin_type(val1->itr); 
   t2 = bson_iterator_bin_type(val2->itr);
   if (t1!=t2) {
      *cmpOut = INTCMP(t1,t2);
      return 0;
   }
   t1 = bson_iterator_bin_len(val1->itr);
   t2 = bson_iterator_bin_len(val2->itr);
   d1 = bson_iterator_bin_data(val1->itr);
   d2 = bson_iterator_bin_data(val2->itr);
   while (t1 && t2) {
      uint8_t v1,v2;
      if (d1[t1] == d2[t2]) {
         t1--;
         t2--;
         continue;
      }
      v1 = d1[t1];
      v2 = d2[t2];
      if (v1 > v2) {
         *cmpOut = 1;
      } else {
         *cmpOut = -1;
      }
      return 0;
   }
   *cmpOut = INTCMP(t1,t2);
   return 0;
}

DEFINE_CMP(OidCmp) {
   bson_oid_t *oid1;
   bson_oid_t *oid2;
   int i;

   if (val1->type != BSON_OID || val2->type != BSON_OID) {
      return -1;
   }
   oid1 = bson_iterator_oid(val1->itr);
   oid2 = bson_iterator_oid(val2->itr);

   for (i=0;i<sizeof(oid1->bytes);i++) {
      if (oid1->bytes[i] == oid2->bytes[i]) {
         continue;
      }
      if (oid1->bytes[i] > oid2->bytes[i]){
         *cmpOut = 1;
      } else {
         *cmpOut = -1;
      }
      return 0;
   }
   *cmpOut = 0;
   return 0;
}

DEFINE_CMP(BoolCmp) {
   return -1;
}

DEFINE_CMP(RegexCmp) {
   return -1;
}

DEFINE_CMP(CodeCmp) {
   return StringCmp(val1,val2,cmpOut);
}

DEFINE_CMP(SymbolCmp) {
   return StringCmp(val1,val2,cmpOut);
}

DEFINE_CMP(CodewscopeCmp) {
   return StringCmp(val1,val2,cmpOut);
}

DEFINE_CMP(TimestampCmp) {
   return -1;
}

DEFINE_CMP(IntCmp) {
   int d1,d2;
   int64_t lng;
   double doVal;

   if (val1->type != BSON_INT) {
      return -1;
   }
   bson_little_endian32(&d1,val1->value);
   if(ElemToInt(val2,&d2)) {
      return -1;
   }
   
   if (d1 == d2) {
      *cmpOut = 0;
   } else if (d1 < d2) {
      *cmpOut = -1;
   } else if (d1 > d2) {
      *cmpOut = 1;
   }
   //printf("Integer compare %d %d : %d\n",d1,d2,*cmpOut);
   return 0;
}


DEFINE_CMP(LongCmp) {
   int64_t d1,d2;
   double doVal;
   int rVal;
   bson_little_endian64(&d1,val1->value);
   rVal = ElemToLong(val2,&d2); 
   if (rVal) {
      return -1;
   }

   if (d1 == d2) {
      *cmpOut = 0;
   } else if (d1 < d2) {
      *cmpOut = -1;
   } else if (d1 > d2) {
      *cmpOut = 1;
   }
   return 0;
}

DEFINE_CMP(DateCmp) {
   return LongCmp(val1,val2,cmpOut);
}

DEFINE_CMP(UnsupportedCmp) {
   return -1;
}


void InitComparators() {
   RegisterBSONCmp(BSON_EOO,UnsupportedCmp);
   RegisterBSONCmp(BSON_DOUBLE,DoubleCmp);
   RegisterBSONCmp(BSON_STRING,StringCmp);
   RegisterBSONCmp(BSON_OBJECT,ObjectCmp);
   RegisterBSONCmp(BSON_ARRAY,ArrayCmp);
   RegisterBSONCmp(BSON_BINDATA,BindataCmp);
   RegisterBSONCmp(BSON_UNDEFINED,UnsupportedCmp);
   RegisterBSONCmp(BSON_OID,OidCmp);
   RegisterBSONCmp(BSON_BOOL,BoolCmp);
   RegisterBSONCmp(BSON_DATE,DateCmp);
   RegisterBSONCmp(BSON_NULL,UnsupportedCmp);
   RegisterBSONCmp(BSON_REGEX,RegexCmp);
   RegisterBSONCmp(BSON_DBREF,UnsupportedCmp);
   RegisterBSONCmp(BSON_CODE,CodeCmp);
   RegisterBSONCmp(BSON_SYMBOL,SymbolCmp);
   RegisterBSONCmp(BSON_CODEWSCOPE,CodewscopeCmp);
   RegisterBSONCmp(BSON_INT,IntCmp);
   RegisterBSONCmp(BSON_TIMESTAMP,TimestampCmp);
   RegisterBSONCmp(BSON_LONG,LongCmp);
}

void BSONElemInit(BSONElem* elem,bson_type type,void* value,uint8_t toFree,bson_iterator* itr) {
   elem->value = value;
   elem->type  = type;
   elem->freeVal = toFree;
   elem->itr = itr;
   //memset(elem->buf,0xab,sizeof(elem->buf));
}

void BSONElemInitFromItr(BSONElem* elem,bson_iterator* itr) {
   elem->value = (char*) bson_iterator_value(itr);
   elem->type  = bson_iterator_type(itr);
   elem->freeVal = False; 
   elem->itr = itr;
}

int CompareElem(BSONElem* val,BSONElem* query,int* cVal) {
   return elemOps[val->type].compare(val,query,cVal); 
}

void BSONElemItrInit(BSONElemItr * it,BSONElem* val) {
   it->val = val;
   it->useItr = False;
   if (it->val->type == BSON_ARRAY) {
      it->useItr = True;
      bson_iterator_subiterator(val->itr,&it->it);
   }
   it->more = True;
   
}

uint8_t BSONElemItrNext(BSONElemItr* it,BSONElem * outVal) {
   if (!it->more) {
      return False;
   }
   if (!it->useItr) {
      *outVal = *it->val;
      it->more = False;
   } else {
      bson_type t;
      t = bson_iterator_next(&it->it);
      if (!t) {
         it->more = False;
         return False;
      }
      BSONElemInitFromItr(outVal,&it->it);
   }
   return True;
}

uint8_t QueryOpLTInt(BSONElem* val,BSONElem* qVal,int * err) {
   int r,cVal;
   r = CompareElem(val,qVal,&cVal);
   if (r) {
      return False;
   }
   return (cVal < 0);
}

uint8_t QueryOpLT(BSONElem* val,BSONElem* qVal,int * err) {
   BSONElem tmpVal;
   BSONElemItr it;
   if (!val->value) {
      return False;
   }
   BSONElemItrInit(&it,val);
   while (BSONElemItrNext(&it,&tmpVal)) {
      if (QueryOpLTInt(&tmpVal,qVal,err)) {
         return True;
      }
   }
   return False;
}

uint8_t QueryOpLTEInt(BSONElem* val,BSONElem* qVal,int * err) {
   int r,cVal;
   if (!val->value) {
      return False;
   }
   r = CompareElem(val,qVal,&cVal);
   if (r) {
      return False;
   }
   return (cVal <= 0);

}

uint8_t QueryOpLTE(BSONElem* val,BSONElem* qVal,int * err) {
   BSONElem tmpVal;
   BSONElemItr it;
   if (!val->value) {
      return False;
   }
   BSONElemItrInit(&it,val);
   while (BSONElemItrNext(&it,&tmpVal)) {
      if (QueryOpLTEInt(&tmpVal,qVal,err)) {
         return True;
      }
   }
   return False;

}

uint8_t QueryOpGTEInt(BSONElem* val,BSONElem* qVal,int * err) {
   int r,cVal;
   if (!val->value) {
      return False;
   }
   r = CompareElem(val,qVal,&cVal);
   if (r) {
      return False;
   }
   return (cVal >= 0);
}

uint8_t QueryOpGTE(BSONElem* val,BSONElem* qVal,int * err) {
   BSONElem tmpVal;
   BSONElemItr it;
   if (!val->value) {
      return False;
   }
   BSONElemItrInit(&it,val);
   while (BSONElemItrNext(&it,&tmpVal)) {
      if (QueryOpGTEInt(&tmpVal,qVal,err)) {
         return True;
      }
   }
   return False;

}

uint8_t QueryOpGTInt(BSONElem* val,BSONElem* qVal,int * err) {
   int r,cVal;
   if (!val->value) {
      return False;
   }
   r = CompareElem(val,qVal,&cVal);
   if (r) {
      return False;
   }
   return (cVal > 0);

}

uint8_t QueryOpGT(BSONElem* val,BSONElem* qVal,int * err) {
   BSONElem tmpVal;
   BSONElemItr it;
   if (!val->value) {
      return False;
   }
   BSONElemItrInit(&it,val);
   while (BSONElemItrNext(&it,&tmpVal)) {
      if (QueryOpGTInt(&tmpVal,qVal,err)) {
         return True;
      }
   }
   return False;

}

uint8_t QueryOpEXISTS(BSONElem* val,BSONElem* qVal,int * err) {
   uint8_t eVal = bson_iterator_bool(qVal->itr);

   if (val->value) {
      return eVal ? eVal:0;   
   } 
   return eVal ? eVal:1;
}

uint8_t QueryOpNEInt(BSONElem* val,BSONElem* qVal,int * err) {
   int cVal;
   if (!val->value) {
      return True;
   } else {
      if (!CompareElem(val,qVal,&cVal) && cVal != 0) {
         return True;
      }
   }
   return False;
}

uint8_t QueryOpNE(BSONElem* val,BSONElem* qVal,int * err) {
   BSONElem tmpVal;
   BSONElemItr it;

   if (!val->value) {
      return False;
   }

   BSONElemItrInit(&it,val);
   while (BSONElemItrNext(&it,&tmpVal)) {
      if (!QueryOpNEInt(&tmpVal,qVal,err)) {
         return False;
      }
   }
   return True;

}

uint8_t QueryOpNINInt(BSONElem* val,BSONElem* qVal,int * err) {
   int cVal;
   bson_iterator si;    
   bson_type t;
   
   if (!val->value) {
      return True;
   } 

   bson_iterator_subiterator(qVal->itr,&si);
   while ((t=bson_iterator_next(&si))) {
      BSONElemInitFromItr(qVal,&si);
      if((!CompareElem(val,qVal,&cVal) )&& (cVal == 0) ) {
         return False;

      }
   } 
   return True;
}

uint8_t QueryOpNIN(BSONElem* val,BSONElem* qVal,int * err) {
   BSONElem tmpVal;
   BSONElemItr it;
   if (!val->value) {
      return False;
   }

   BSONElemItrInit(&it,val);
   while (BSONElemItrNext(&it,&tmpVal)) {
      if (!QueryOpNINInt(&tmpVal,qVal,err)) {
         return False;
      }
   }
   return True;
}

uint8_t QueryOpINInt(BSONElem* val,BSONElem* qVal,int * err) {
   int cVal;
   bson_iterator si;    
   bson_type t;
   
   if (!val->value) {
      return False;
   } 

   bson_iterator_subiterator(qVal->itr,&si);
   while ((t=bson_iterator_next(&si))) {
      BSONElemInitFromItr(qVal,&si);
      if((!CompareElem(val,qVal,&cVal) )&& (cVal == 0) ) {
         return True;
      }
   } 
   return False;

}

uint8_t QueryOpIN(BSONElem* val,BSONElem* qVal,int * err) {
   BSONElem tmpVal;
   BSONElemItr it;
   if (!val->value) {
      return False;
   }

   BSONElemItrInit(&it,val);
   while (BSONElemItrNext(&it,&tmpVal)) {
      if (QueryOpINInt(&tmpVal,qVal,err)) {
         return True;
      }
   }
   return False;
}

uint8_t QueryOpALL(BSONElem* val,BSONElem* qVal,int * err) {
   BSONElem tmpQVal;
   BSONElemItr qit;
   if (!val->value) {
      return False;
   }

   BSONElemItrInit(&qit,qVal);
   int cVal;
   uint8_t found = False;
   while (BSONElemItrNext(&qit,&tmpQVal)) {
      BSONElem tmpVal;
      BSONElemItr it;
      BSONElemItrInit(&it,val);
      found = False;
      while (BSONElemItrNext(&it,&tmpVal)) {
         if (!CompareElem(&tmpVal,&tmpQVal,&cVal) && cVal == 0) {
            found = True;
            break;
         }
      }
      if (!found) {
         return False;
      }
   }
   return True;
}


int 
ComputeOperator(BSONElem* val,BSONElem* query){
   bson_iterator q,o;
   bson_type qType,tType;

   bson_iterator_from_buffer(&q, query->value);
   bson_iterator_from_buffer(&o, val->value);
   bson qBson,oBson;
   bson_init_finished_data(&qBson,(char*)query->value);
   bson_init_finished_data(&oBson,(char*)val->value);
   while((qType = bson_iterator_next(&q))) { 
      const char* key = bson_iterator_key(&q);
      BSONElem qVal;
      BSONElemInitFromItr(&qVal,&q);
      int cVal = 0,r;
      uint8_t cont = False;
      int error;
      QueryOpDesc* desc = HTFind(opTable,key,strlen(key)+1);
      if (desc) {
         cont = desc->fn(val,&qVal,&error);
         if (!cont) {
            return False;
         }
      } else {
         // no such operator
         return False;
      }
   }
   //printf("Returning true\n");
   return True;
}

int CompareValue(BSONElem* val,BSONElem* query) {
   bson_type qType = query->type;
   int r,cVal;
   if (qType == BSON_OBJECT) {
      r =  ComputeOperator(val,query);
      if (!r && val->type == BSON_OBJECT) {
         bson o,q;
         bson_iterator_subobject(val->itr,&o);
         bson_iterator_subobject(query->itr,&q);

         return QueryObjMatch(&o,&q); 
      }
      return r;
         // do individual element comparision with object
   }
   r = CompareElem(val,query,&cVal);
   if (r) {
      return False;
   }
   if (!cVal) {
      return True;
   }
   return False;
}

int
QueryGetIndexesToUse(BinaryStr* query,ListNode* head){
   LIST_INIT(head);
   bson_iterator q,o;
   bson_type qType,tType;

   bson_iterator_from_buffer(&q, query->data);
   while((qType =bson_iterator_next(&q))) {
      const char* key = bson_iterator_key(&q);
      if (qType != BSON_ARRAY && qType != BSON_OBJECT ) {
         ListNode* node2 = (ListNode*) malloc(sizeof(*node2));
         node2->data = strdup(key);
         LIST_ADD_AFTER(node2,LIST_LAST(head));
      } 
   }
   return 0;
}

int
QueryElemMatch(char* key,BSONElem* objElem,BSONElem* query) {
   bson_iterator oItr;
   char* k = key;
   bson obj;
   bson so; 
   BSONElem tmpVal;
   BSONElem valElem;
   BSONElemItr it1;
   bson_iterator_init(&oItr,&obj);

   if (!objElem) {
      BSONElemInit(&tmpVal,BSON_EOO,NULL,0,NULL);
      return CompareValue(&tmpVal,query);
   }
   BSONElemItrInit(&it1,objElem);
   if (*k) {
      BSONElem soElem;
      BSONElem* elemPtr = NULL;
      bson_iterator sItr;
      bson b;

      char* keyEnd = strchr(key,'.');
      //printf("Key in new elem '%s','%s'\n",key,keyEnd);
      if (keyEnd) {
         *keyEnd = '\0';
         keyEnd ++;
      } else {
         //printf("No dot \n");
         keyEnd = key+strlen(key);
      }
      while (BSONElemItrNext(&it1,&tmpVal)) {
         if (tmpVal.type != BSON_OBJECT) {
            continue;
         }
         bson_iterator_subobject(tmpVal.itr,&b);
         bson_iterator_init(&sItr,&b);
         if (bson_find(&sItr,&b,key)) {
            BSONElemInitFromItr(&soElem,&sItr);
            elemPtr = &soElem;
         }
         if (QueryElemMatch(keyEnd,elemPtr,query)) {
            return True;
         }
      }
   } else {
      // end of the string
      if (query->type == BSON_ARRAY || query->type == BSON_OBJECT) {
         return CompareValue(objElem,query);
      }
      while (BSONElemItrNext(&it1,&tmpVal)) {
         if (CompareValue(&tmpVal,query)) {
            return True;
         }
      }
      return False;
   }

   return False;
}

uint8_t
QueryObjMatch(bson* obj,bson* query) {
   bson_iterator oItr,qItr; 
   bson_type o,q;

   bson_iterator_init(&oItr,obj);
   bson_iterator_init(&qItr,query);

   while ((o=bson_iterator_next(&oItr))) {
      BSONElem oVal,qVal;
      int cVal;
      q = bson_find(&qItr,query,bson_iterator_key(&oItr));
      if (!q) {
         return False;
      }
      if ( (o == BSON_OBJECT && q != BSON_OBJECT ) ||
           (q == BSON_OBJECT && o != BSON_OBJECT ) ) {
         return False;
      }
      if ( o == q && o == BSON_OBJECT) {
         bson o1,q1;
         bson_iterator_subobject(&oItr,&o1);
         bson_iterator_subobject(&qItr,&q1);
         return QueryObjMatch(&o1,&q1);
      }
      BSONElemInitFromItr(&oVal,&oItr);
      BSONElemInitFromItr(&qVal,&qItr);
      if (CompareElem(&oVal,&qVal,&cVal)) {
         return False;
      }
      if (cVal) {
         return False;
      }
   }
   return True;
}

int 
QueryMatch(BinaryStr* query, BinaryStr* obj ) {
   bson_iterator q,o;
   bson_type qType,tType;
   bson qBson,oBson;
   BSONElem objElem;

   bson_iterator_from_buffer(&q, query->data);
   bson_iterator_from_buffer(&o, obj->data);
   bson_init_finished_data(&qBson,(char*)query->data);
   bson_init_finished_data(&oBson,(char*)obj->data);


   while((qType = bson_iterator_next(&q))) {
      // for all keys in query
      char *newKey;
      const char* key = bson_iterator_key(&q);
      char* keyEnd;
      BSONElem qVal,oVal;
      newKey = strdup(key);
      if (!newKey) {
         return False;
      }
      keyEnd = strchr(newKey,'.');
      if (keyEnd) {
         *keyEnd = '\0';
         keyEnd ++;
      } else {
         //printf("No dot \n");
         keyEnd = newKey+strlen(newKey);
      }
      BSONElemInitFromItr(&qVal,&q);
      tType = bson_find(&o,&oBson,newKey) ;
      if (!tType) {
         return False;
      }
      BSONElemInitFromItr(&oVal,&o);
      if (!QueryElemMatch(keyEnd,&oVal,&qVal)) {
         free(newKey);
         return False;
      }
      free(newKey);
   }
   return True;
}
// Returns True if object matches the query

int 
QueryMatch2(BinaryStr* query, BinaryStr* obj ) {
   bson_iterator q,o;
   bson_type qType,tType;
   bson qBson,oBson;

   bson_iterator_from_buffer(&q, query->data);
   bson_iterator_from_buffer(&o, obj->data);
   bson_init_finished_data(&qBson,(char*)query->data);
   bson_init_finished_data(&oBson,(char*)obj->data);

   while((qType = bson_iterator_next(&q))) {
      // for all keys in query
      const char* key = bson_iterator_key(&q);
      //printf("Key %s \n",key);
      BSONElem val1,val2;

      BSONElemInit(&val1,bson_iterator_type(&q),(char*) bson_iterator_value(&q),0,&q);
      tType = bson_find(&o,&oBson,key); 
      if (!tType) {

         BSONElemInit(&val2,BSON_EOO,NULL,0,NULL);
         if (!CompareValue(&val2,&val1)) {
            return False;
         }
         continue;
      } else if (tType == BSON_OBJECT && qType == BSON_OBJECT) {
         BinaryStr qData,oData;
         qData.data = (char*) bson_iterator_value(&q);
         oData.data = (char*) bson_iterator_value(&o);
         if (!QueryMatch(&qData,&oData)) {
            return False;
         }
      } else if (tType == BSON_ARRAY && qType != BSON_OBJECT && qType != BSON_ARRAY) {
         bson_iterator si;
         bson_type t;
         bson_iterator_subiterator(&o,&si);
         uint8_t found = False; 
         while ((t=bson_iterator_next(&si))){
            //BSONElemInit(&val2,bson_iterator_type(&si),(char*) bson_iterator_value(&si),0,&si);
            BSONElemInitFromItr(&val2,&si);
            //BSONElemInit(&val2,bson_iterator_type(&si),(char*) bson_iterator_value(&si),0,&si);
            if (CompareValue(&val2,&val1)) {
               found = True;
               break;
            }
         }
         if (!found) {
            return False;
         } 
      } else {

         //BSONElemInit(&val2,bson_iterator_type(&o),(char*) bson_iterator_value(&o),0,&o);
         BSONElemInitFromItr(&val2,&o);
         if (!CompareValue(&val2,&val1)) {
            return False;
         }
      }
   }

   return True;
}


int
QueryMatchBson(bson* query,bson* obj) {
   BinaryStr q,o;
   BsonToBStr(obj,&o);
   BsonToBStr(query,&q);
   return QueryMatch(&q,&o);
}

uint8_t QueryOpCmpFn(void* fullData,void* searchData) {
   QueryOpDesc * desc = fullData;
   return strcmp(desc->opName,searchData) == 0;
}

int
RegisterQueryOp(const char* op, QueryOpFn fn) {
   void* exist;
   int len = strlen(op);
   int rv;
   QueryOpDesc *desc = malloc(sizeof(QueryOpDesc));
   if (!desc) {
      return ENOMEM;
   }
   if (!fn) {
      free(desc);
      return EINVAL;
   }
   if (len > (sizeof(desc->opName)-1)){
      free(desc);
      return ENAMETOOLONG;
   }
   strcpy(desc->opName,op);
   desc->fn = fn;
   if ((rv = HTInsert(opTable,desc,op,len+1,&exist))) {
      free(desc);
      return rv;
   }
   return 0; 
}

void InitQueryOperators(){
   void *exist;
   opTable = HTInit(DJB2StrHash,QueryOpCmpFn,23);
   assert(opTable);
   RegisterQueryOp("$lt",QueryOpLT);
   RegisterQueryOp("$lte",QueryOpLTE);
   RegisterQueryOp("$gt",QueryOpGT);
   RegisterQueryOp("$gte",QueryOpGTE);
   RegisterQueryOp("$exists",QueryOpEXISTS);
   RegisterQueryOp("$nin",QueryOpNIN);
   RegisterQueryOp("$ne",QueryOpNE);
   RegisterQueryOp("$in",QueryOpIN);
   RegisterQueryOp("$all",QueryOpALL);
}

__attribute__((constructor)) void
InitQueryModule(void) {
   InitComparators();
   InitQueryOperators();
}

