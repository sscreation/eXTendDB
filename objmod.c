
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

#include "objmod.h"
#include "extenddberror.h"
#include "query.h"
#include "hash.h"

static HashTable* modObjTable;

int ObjModOpSet(BSONElem* val,BSONElem* objModVal,bson* outBson,bson* error);
int ObjModOpUnset(BSONElem* val,BSONElem* objModVal,bson* outBson,bson* error);
int ObjModOpInc(BSONElem* val,BSONElem* objModVal,bson* outBson,bson* error);
int ObjModOpArrSet(BSONElem* val,BSONElem* objModVal,bson* outBson,bson* error);
int ObjModOpPop(BSONElem* val,BSONElem* objModVal,bson* outBson,bson* error);

int ObjModOpPush(BSONElem* val,BSONElem* objModVal,bson* outBson,bson* error);

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

XTDBError ObjModValidateVal(BinaryStr* obj,int* outIsMod) {
   bson_iterator q,o;
   bson_type qType,tType;
   bson objBson;
   int isMod = 0;

   bson_iterator_from_buffer(&q, obj->data);
   bson_init_finished_data(&objBson,(char*)obj->data);

   while ((qType = bson_iterator_next(&q))) {
      const char* key = bson_iterator_key(&q);
      if (*key != '$') {
         if (isMod == 1) {
            return XTDB_MIX_MOD_NONMOD_OP;
         }
         isMod = 2;
      } else {
         if (isMod == 2) {
            return XTDB_MIX_MOD_NONMOD_OP;
         }
         isMod =1;
      }
   }
   if (isMod == 2 || isMod == 0) {
      *outIsMod = 0;
   } else {
      *outIsMod = 1;
   }

   return XTDB_OK;
}

XTDBError ObjModValidateValBson(bson* newObj,int* isMod) {
   BinaryStr o;
   BsonToBStr(newObj,&o);
   return ObjModValidateVal(&o,isMod);
}

int ObjModReplaceField(bson* oldObj,bson* newField,bson* outObj,char* key) {
   bson_iterator fItr,oItr;
   bson_type t;
   int found=0;

   bson_init(outObj);

   bson_iterator_init(&fItr,newField);
   if (oldObj) {
      bson_append_start_object(outObj,"");
   }
   t = bson_iterator_next(&fItr);
   if (oldObj) {
      bson_iterator_init(&oItr,oldObj);
      while (bson_iterator_next(&oItr)) {
         if (strcmp(bson_iterator_key(&oItr),key)) {
            if (bson_append_element(outObj,NULL,&oItr)) {
               return ENOMEM;
            }
         } else {
            if (t) {
               if (bson_append_element(outObj,key,&fItr)) {
                  return ENOMEM;
               }
            }
            found = 1;
         }
      }
   }

   if (!found) {
      if (t) {
         if (!oldObj) {
            bson_append_start_object(outObj,"");
         }
         if (bson_append_element(outObj,key,&fItr)) {
            return ENOMEM;
         }
         if (!oldObj) {
            bson_append_finish_object(outObj);
         }
      }
   }
   if (oldObj) {
      bson_append_finish_object(outObj);
   }

   bson_finish(outObj);
   return 0;
}

int 
ObjModGetNewElem(char* key,BSONElem* obj,ObjModOpFn cb,BSONElem* objModVal,bson* outObj,bson* error) {
   char* newVal = strchr(key,'.');   
   bson newElem;
   int rv;
   BSONElem subObjElem;
   if (*key) {
      BSONElem *subElem = NULL;
      bson_iterator itr;
      bson oldObj;
      
      char* keyEnd = strchr(key,'.');
      //printf("Key in new elem '%s','%s'\n",key,keyEnd);
      if (keyEnd) {
         *keyEnd = '\0';
         keyEnd ++;
      } else {
         //printf("No dot \n");
         keyEnd = key+strlen(key);
      }
      if (obj) {
         bson_iterator_subiterator(obj->itr,&itr);
         while (bson_iterator_next(&itr)) {
            if (strcmp(key,bson_iterator_key(&itr)) == 0) {
               BSONElemInitFromItr(&subObjElem,&itr);
               subElem = &subObjElem;
               break;
            }

         }
      }

      if ( (rv = ObjModGetNewElem(keyEnd,subElem,cb,objModVal,&newElem,error))) {
         return rv;
      }
      if (obj) {
         bson_iterator_subobject(obj->itr,&oldObj);
         if (ObjModReplaceField(&oldObj,&newElem,outObj,key))  {
            bson_destroy(&newElem);
            return XTDB_NO_MEM;
         }   
      } else {
         if (ObjModReplaceField(NULL,&newElem,outObj,key))  {
            bson_destroy(&newElem);
            return XTDB_NO_MEM;
         }   
         bson_init(&oldObj);
         bson_append_start_object(&oldObj,key);
         bson_append_finish_object(&oldObj);
         bson_finish(&oldObj);
      }

   } else {
      //bson error;
      if ((rv = cb(obj,objModVal,outObj,error))) {
         //bson_print(error);
         return rv;
      }
      return rv;
   }

   return 0;
}


typedef struct ElemDesc{
   char* key;
   bson obj;
}ElemDesc;

uint8_t ObjModElemCmp(void* fullData,void* searchData) {
   ElemDesc* desc = fullData;
   return strcmp(desc->key,searchData) == 0;
}


ObjModOpFn GetObjModOpFn(const char* opName) {
   ObjModOpDesc * desc = HTFind(modObjTable,opName,strlen(opName)+1);
   if (desc) {
      return desc->fn;
   }
   return NULL;
}

void ElemDescFree(void* d) {
   ElemDesc* desc = d;
   if (d) {
      free(desc->key);
      bson_destroy(&desc->obj);
      free(d);
   }
}


int AppendElemToBSON(void* data,void* args) {
   ElemDesc* desc = data;
   bson* outBson = args;
   bson_iterator eItr;

   bson_iterator_init(&eItr,&desc->obj);
   bson_iterator_next(&eItr);
   bson_append_element(outBson,desc->key,&eItr);
   return 0;
}


int SetBsonError(bson* error, XTDBError err) {
   bson_init(error);
   bson_append_string(error,"error",XTDBGetErrString(err));
   bson_finish(error);
}

int SetBsonErrorString(bson* error,char* str) {
   bson_init(error);
   bson_append_string(error,"error",str);
   bson_finish(error);
}

int ObjModifyBson(bson* obj,bson* modObj,bson* outObj,bson* error) {
   bson_iterator oItr,mItr;
   HashTable* delList;
   HashTable* newList;
   
   XTDBError err = XTDB_OK;

   bson_init(outObj);
   newList = HTInit(DJB2StrHash,ObjModElemCmp,13);
   if (!newList) {
      err = XTDB_NO_MEM;
      SetBsonError(error,err);
      return err;
   }
   delList = HTInit(DJB2StrHash,ObjModElemCmp,13);
   if (!delList) {
      err =  XTDB_NO_MEM;
      HTDestroy(newList,ElemDescFree);
      SetBsonError(error,err);
      return err;
   }
   bson_iterator_init(&oItr,obj);
   bson_iterator_init(&mItr,modObj);
   while (bson_iterator_next(&mItr)){
      bson_iterator itr;
      ObjModOpFn cb;
      cb = GetObjModOpFn(bson_iterator_key(&mItr));
      if (!cb) {
         err = XTDB_INVALID_OBJ_MOD;
         SetBsonError(error,err);
         goto cleanup;
      }
      if (bson_iterator_type(&mItr) !=BSON_OBJECT) {
         err = XTDB_MOD_SHOULD_BE_OBJ;
         SetBsonError(error,err);
         goto cleanup;
      }
      bson_iterator_subiterator(&mItr,&itr);
      while (bson_iterator_next(&itr)) {
         bson_type t;
         BSONElem elem,objModVal;
         ElemDesc *desc;
         bson_iterator rItr;
         BSONElem * elemPtr = NULL;
         bson_iterator exItr;
         char* key = strdup(bson_iterator_key(&itr));
         char* keyEnd = strchr(key,'.');
         void *exist;
         ElemDesc* tmp;
         desc = malloc(sizeof(*desc));
         if (!desc) {
            free(key);
         //   free(desc);
            err = XTDB_NO_MEM;
            goto cleanup;
         }
         desc->key = key;
         if (strcmp(desc->key , "_id") == 0) {
            err = XTDB_CANNOT_MODIFY_ID;
            SetBsonError(error,err);
            free(key);
            free(desc);
            goto cleanup;
         }
         if (keyEnd){
            *keyEnd = '\0';
            keyEnd++;
         } else {
            keyEnd = key+strlen(key);
         }
         BSONElemInitFromItr(&objModVal,&itr);
         if (!(tmp = HTFind(newList,key,strlen(key)))){
            t = bson_find(&oItr,obj,key);
            if (t) {
               BSONElemInitFromItr(&elem,&oItr);
               elemPtr = &elem;
            } 
         } else {
            bson_iterator_init(&exItr,&tmp->obj);
            if (bson_iterator_next(&exItr)) {
               BSONElemInitFromItr(&elem,&exItr);
               elemPtr = &elem;
            }
         }
         if ((err = ObjModGetNewElem(keyEnd,elemPtr,cb,&objModVal,&desc->obj,error))) {
            free(key);
            free(desc);
            goto cleanup;
         }
         tmp = HTRemove(newList,desc->key,strlen(desc->key));
         ElemDescFree(tmp);
         bson_iterator_init(&rItr,&desc->obj);
         if (!bson_iterator_next(&rItr)) {
            HTInsert(delList,desc,desc->key,strlen(desc->key),&exist);
         } else {
            HTInsert(newList,desc,desc->key,strlen(desc->key),&exist);
         }

      }
   }
   //bson_init(outObj);
   bson_iterator_init(&oItr,obj);
   while (bson_iterator_next(&oItr)){
      ElemDesc* desc;
      bson_iterator eItr;
      if (HTFind(delList,bson_iterator_key(&oItr),strlen(bson_iterator_key(&oItr)))) {
         continue;
      }
      if ((desc = HTFind(newList,bson_iterator_key(&oItr),strlen(bson_iterator_key(&oItr))))){
         bson_iterator_init(&eItr,&desc->obj);
         bson_iterator_next(&eItr);
         bson_append_element(outObj,desc->key,&eItr);
         desc = HTRemove(newList,(char*)bson_iterator_key(&oItr),strlen(bson_iterator_key(&oItr)));
         ElemDescFree(desc);   
      } else {
         bson_append_element(outObj,NULL,&oItr);
      }
   }

   HTForAll(newList,AppendElemToBSON,outObj);
cleanup:
   bson_finish(outObj);
   if (err != XTDB_OK) {
      bson_destroy(outObj);
   }
   HTDestroy(newList,ElemDescFree);
   HTDestroy(delList,ElemDescFree);
   return err;
}

XTDBError 
RegisterObjModOp(char* modName,ObjModOpFn cb) {
   ObjModOpDesc* desc = malloc(sizeof(*desc));   
   void* exist;
   int rv;
   if (!desc) {
      return XTDB_NO_MEM;
   }
   if (strlen(modName) > sizeof(desc->key)-1 || *modName != '$') {
      free(desc);
      return XTDB_INVALID_INPUT;
   } 
   strcpy(desc->key,modName);
   desc->fn = cb;
   rv = HTInsert(modObjTable,desc,modName,strlen(modName),&exist);
   if (rv == EEXIST) {
      free(desc);
      return XTDB_ENTRY_EXISTS;
   }
   if (rv == ENOMEM) {
      return XTDB_NO_MEM;
   }
   return XTDB_OK;
}

uint8_t ObjModDescCmpFn(void* fullData,void* searchData) {
   ObjModOpDesc* desc= fullData;
   return strcmp(desc->key,searchData) == 0;
}

__attribute__ ((constructor)) void 
InitObjMod() {
   modObjTable = HTInit(DJB2StrHash,ObjModDescCmpFn,13);
   assert(modObjTable);
   assert(!RegisterObjModOp("$set",ObjModOpSet));
   assert(!RegisterObjModOp("$unset",ObjModOpUnset));
   assert(!RegisterObjModOp("$inc",ObjModOpInc));
   assert(!RegisterObjModOp("$arrSet",ObjModOpArrSet));
   assert(!RegisterObjModOp("$pop",ObjModOpPop));
   assert(!RegisterObjModOp("$push",ObjModOpPush));
}


int ObjModOpSet(BSONElem* val,BSONElem* objModVal,bson* outBson,bson* error) {
   bson newVal;
   bson oldVal;
   bson_init(outBson);
   bson_append_element(outBson,"",objModVal->itr);
   bson_finish(outBson);
   return 0;   
}

int ObjModOpUnset(BSONElem* val,BSONElem* objModVal,bson* outBson,bson* error) {
   bson newVal;
   bson oldVal;
   bson_init(outBson);
   bson_finish(outBson);
   return 0;   
}

uint8_t BSONIsTypeNumber(bson_type type) {
   return (type == BSON_INT || type == BSON_LONG || type == BSON_DOUBLE);

}

int ObjModOpInc(BSONElem* val,BSONElem* objModVal,bson* outBson,bson* error) {
   bson newVal;
   bson oldVal;
   bson_init(outBson);
   if (!BSONIsTypeNumber(objModVal->type)) {
      bson_init(error);
      bson_append_string(error,"error","Only numbers allowed in $inc modifier.");
      bson_finish(error);
      bson_finish(outBson);
      bson_destroy(outBson);
      return XTDB_MOD_ERR;
   }
   if (val && !BSONIsTypeNumber(val->type)) {
      bson_init(error);
      bson_append_string(error,"error","$inc cannot be applied for non-numbers.");
      bson_finish(error);
      bson_finish(outBson);
      bson_destroy(outBson);
      return XTDB_MOD_ERR;
   }
   if (!val) {
      bson_append_element(outBson,"",objModVal->itr);
   } else {
      if (val->type == BSON_INT) {
         int v = bson_iterator_int(objModVal->itr);
         int i = bson_iterator_int(val->itr);
         i+=v;
         bson_append_int(outBson,"",i);
      }
      if (val->type == BSON_LONG) {
         int64_t v = bson_iterator_long(objModVal->itr);
         int64_t i = bson_iterator_long(val->itr);
         i+=v;
         bson_append_long(outBson,"",i);
      }
      if (val->type == BSON_DOUBLE) {
         double v = bson_iterator_double(objModVal->itr);
         double i = bson_iterator_double(val->itr);
         i+=v;
         bson_append_double(outBson,"",i);
      }
   }
   bson_finish(outBson);
   return 0;   
}

int 
ObjModOpPop(BSONElem* val,BSONElem* objModVal,bson* outBson,bson* error) {
   bson_iterator sItr,aItr;
   bson_type t;
   bson query,mod;
   bson tObj;
   int sIdx;
   int rv = 0;
   uint8_t toInsert = True;
   char comError[2048];
   int idx=0;
   

   if (!BSONIsTypeNumber(objModVal->type)) {
      SetBsonErrorString(error,"Invalid argument to $pop number expected.");
      return XTDB_MOD_ERR;  
   }
   if (!val) {
      bson_init(outBson);
      bson_finish(outBson);
      return XTDB_OK;
   }
   if (val && val->type != BSON_ARRAY) {
      SetBsonErrorString(error,"Cannot apply $pop to non-array element.");
      return XTDB_MOD_ERR;
   }
   ElemToInt(objModVal,&sIdx);
   bson_init(outBson);
   bson_append_start_array(outBson,"");
   bson_iterator_subiterator(val->itr,&aItr);
   while ((t=bson_iterator_next(&aItr))) {
      if (idx != sIdx) {
         bson_append_element(outBson,"",&aItr);
      }
      idx++;
   }
   bson_append_finish_object(outBson);
   bson_finish(outBson);
   if (rv) {
      bson_destroy(outBson);
   }
   return rv;
}

int 
ObjModOpPush(BSONElem* val,BSONElem* objModVal,bson* outBson,bson* error) {
   bson_iterator sItr,aItr;
   bson_type t;
   bson query,mod;
   bson tObj;
   int sIdx;
   int rv = 0;
   uint8_t toInsert = True;
   char comError[2048];
   int idx=0;

   if (val && val->type != BSON_ARRAY) {
      SetBsonErrorString(error,"Cannot apply $pop to non-array element.");
      return XTDB_MOD_ERR;
   }
   ElemToInt(objModVal,&sIdx);
   bson_init(outBson);
   bson_append_start_array(outBson,"");
   bson_iterator_subiterator(val->itr,&aItr);
   while ((t=bson_iterator_next(&aItr))) {

   }
   bson_append_element(outBson,"",objModVal->itr);
   bson_append_finish_object(outBson);
   bson_finish(outBson);
   if (rv) {
      bson_destroy(outBson);
   }
   return rv;
}

int 
ObjModOpArrSet(BSONElem* val,BSONElem* objModVal,bson* outBson,bson* error) {
   bson_iterator sItr,aItr;
   bson_type t;
   bson query,mod;
   bson tObj;
   int rv = 0;
   uint8_t toInsert = True;
   char comError[2048];

   if (objModVal->type != BSON_OBJECT) {
      bson_init(error);
      bson_append_string(error,"error","Invalid argument for $arrSet operator , operator value must be an object.");
      bson_finish(error);
      return XTDB_MOD_ERR;
   }
   
   bson_iterator_subiterator(objModVal->itr,&sItr);
   bson_iterator_subobject(objModVal->itr,&tObj);
   t =  bson_find(&sItr,&tObj,"query");
   if (t != BSON_OBJECT) {
      bson_init(error);
      bson_append_string(error,"error","Invalid argument for $arrSet operator , 'query' value must be an object.");
      bson_finish(error);
      return XTDB_MOD_ERR;
   } else {
      bson_iterator_subobject(&sItr,&query);
   }

   t =  bson_find(&sItr,&tObj,"value");
   if (t != BSON_OBJECT) {
      bson_init(error);
      bson_append_string(error,"error","Invalid argument for $arrSet operator , 'value' value must be an object.");
      bson_finish(error);
      return XTDB_MOD_ERR;
   } else {
      bson_iterator_subobject(&sItr,&mod);
   }

   if (val->type != BSON_ARRAY) {
      bson_init(error);
      bson_append_string(error,"error","Field must be an array for $arrSet operator.");
      bson_finish(error);
      return XTDB_MOD_ERR;
   }
   bson_init(outBson);
   bson_append_start_array(outBson,"");
   bson_iterator_subiterator(val->itr,&aItr);
   while ((t=bson_iterator_next(&aItr))) {
      bson tmpVal;
      bson_init(&tmpVal);
      bson_append_element(&tmpVal,"",&aItr);
      bson_finish(&tmpVal);
      if (QueryMatchBson(&query,&tmpVal)) {
         bson newVal;
         bson_iterator i;
         bson tmpError;
         toInsert = False;
         rv = ObjModifyBson(&tmpVal,&mod,&newVal,&tmpError);
         if (rv) {
            bson_iterator it;
            bson_iterator_init(&it,&tmpError);
            bson_find(&it,&tmpError,"error");
            snprintf(comError,sizeof(comError),"%s, '%s'","Error applying modifications to array element",bson_iterator_string(&it));
            bson_init(error);
            bson_append_string(error,"error","Error applying modifications to array element.");
            bson_finish(error);
            bson_destroy(&tmpVal);
            bson_destroy(&tmpError);
            goto cleanup;
         }
         bson_iterator_init(&i,&newVal);
         if (bson_iterator_next(&i)) {
            bson_append_element(outBson,"",&i);
            bson_destroy(&newVal);
         }
      } else {
         bson_append_element(outBson,"",&aItr);
      }
      bson_destroy(&tmpVal);
   }
   t =  bson_find(&sItr,&tObj,"upsert");
   if (t) {
      bson tmpVal;
      bson newVal;
      bson tmpError;
      bson_iterator i;

      bson_init(&tmpVal);
      bson_finish(&tmpVal);
      rv = ObjModifyBson(&tmpVal,&mod,&newVal,&tmpError);
      if (rv) {
         bson_iterator it;
         bson_iterator_init(&it,&tmpError);
         bson_find(&it,&tmpError,"error");
         snprintf(comError,sizeof(comError),"%s, '%s'","Error applying modifications to array element",bson_iterator_string(&it));
         bson_init(error);
         bson_append_string(error,"error",comError);
         bson_finish(error);
         bson_destroy(&tmpVal);
         bson_destroy(&tmpError);
         goto cleanup;
      }
      bson_iterator_init(&i,&newVal);
      if (bson_iterator_next(&i)) {
         bson_append_element(outBson,"",&i);
         bson_destroy(&newVal);
      }
   }
cleanup:
   bson_append_finish_object(outBson);
   bson_finish(outBson);
   if (rv) {
      bson_destroy(outBson);
   }
   return rv;
}
