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

#ifndef __OBJMOD_H__
#define __OBJMOD_H__
#include "bson.h"
#include "query.h"
#include "extenddberror.h"

typedef int (*ObjModOpFn)(BSONElem* val,BSONElem* objModVal,bson* outBson,bson* error);
typedef struct ObjModOpDesc{
   char key[64];
   ObjModOpFn fn;
}ObjModOpDesc;

int ObjModifyBson(bson* obj,bson* modObj,bson* outObj,bson* error);

XTDBError RegisterObjModOp(char* modName,ObjModOpFn cb);
XTDBError ObjModValidateValBson(bson* newObj,int* isMod);

#endif
