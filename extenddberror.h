
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
#ifndef __XTDB_ERROR_H__
#define __XTDB_ERROR_H__

typedef enum XTDBError {
   XTDB_OK                 ,
   XTDB_NO_MEM             ,
   XTDB_INVALID_BSON       ,
   XTDB_NIN_REQUIRES_ARRAY ,
   XTDB_IN_REQUIRES_ARRAY  ,
   XTDB_ALL_REQUIRES_ARRAY ,
   XTDB_IO_ERR             ,
   XTDB_FILE_NOT_FOUND     ,
   XTDB_MAIN_DB_NOT_FOUND  ,
   XTDB_DESC_NOT_FOUND     ,
   XTDB_INDEX_NOT_FOUND    ,
   XTDB_DB_OPEN_FAILED     ,
   XTDB_INDEX_EXISTS       ,
   XTDB_NO_ENTRY           ,
   XTDB_ENTRY_EXISTS       ,
   XTDB_CORRUPT_DB         ,
   XTDB_NO_SPACE           ,
   XTDB_PERM_DENIED        ,
   XTDB_INVALID_CURSOR     ,
   XTDB_MIX_MOD_NONMOD_OP  ,
   XTDB_INVALID_OBJ_MOD    ,
   XTDB_MOD_SHOULD_BE_OBJ  ,
   XTDB_INVALID_INPUT      ,
   XTDB_CANNOT_MODIFY_ID   ,
   XTDB_MOD_ERR            ,
   XTDB_OTHER_ERR          ,
}XTDBError;

typedef struct XTDBErrDesc {
   XTDBError errnum;
   char* errString;
}XTDBErrDesc;

const char* XTDBGetErrString(XTDBError err);

#endif
