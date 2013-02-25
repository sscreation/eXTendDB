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

#ifndef __QUERY_H__
#define __QUERY_H__
#include "bson.h"
#include "stringutil.h"

#define BsonToBStr(bsonVal,binaryStr)\
   (binaryStr)->data = (bsonVal)->data;\
   (binaryStr)->len = bson_size((bsonVal));
   //(binaryStr)->len = bson_size((bsonVal));

#define BStrToBson(binaryStr,bsonVal)\
   bson_init_finished_data((bsonVal),(char*)(binaryStr)->data);
#define BSON_TYPE_LAST (BSON_LONG+1)
typedef struct BSONElem {
   bson_type type;
   //`uint8_t buf[32];
   void* value;
   uint32_t len;
   uint8_t freeVal;
   bson_iterator* itr;
} BSONElem;

typedef void (*BSONElemConverterFn)(BSONElem* const from,BSONElem* const to);
typedef int (*BSONElemCompareFn)(BSONElem* const val1,
                                 BSONElem* const val2,int* cmpOut);

typedef int (*BSONElemToBStr)(const BSONElem* const from,bson* out);

#define DEFINE_CMP(fnName) int fnName(BSONElem* const val1, BSONElem* const val2,int* cmpOut)

#define DEFINE_TOBSTR(fnName) int fnName(const BSONElem* elem,bson* out)

typedef struct BSONElemOps{
   //BSONElemConverter converter;
   // Compare any 2 types of BSONElements used in querying
   BSONElemCompareFn compare;
   // Convert BSONElem to binary data used for indexing
   BSONElemToBStr datatobstr;
} BSONElemOps;

void InitQueryModule(void);
int QueryMatch(BinaryStr* query,BinaryStr* obj);

int QueryMatchBson(bson* query,bson* obj);

extern BSONElemOps elemOps[BSON_TYPE_LAST];
static int
ConvertToBStr(BSONElem* val,bson* out) {
   assert(val);
   assert(out);
   bson_init(out);
   if (bson_append_element(out,"",val->itr)) {
      return ENOMEM;
   }
   bson_finish(out);
   return 0;
   //return elemOps[val->type].datatobstr(val,out); 
}


typedef uint8_t (*QueryOpFn)(BSONElem* val, BSONElem* qVal, int* err);

typedef struct QueryOpDesc {
   char opName[64];
   QueryOpFn fn;
} QueryOpDesc;

int RegisterQueryOp(const char* op, QueryOpFn fn);
int RegisterBSONCmp(bson_type type,BSONElemCompareFn fn);

typedef struct BSONElemItr {
   BSONElem* val;
   bson_iterator it;
   uint8_t more;
   uint8_t useItr;
} BSONElemItr;

void BSONElemItrInit(BSONElemItr * it,BSONElem* val);
uint8_t BSONElemItrNext(BSONElemItr* it,BSONElem * outVal);

#endif
