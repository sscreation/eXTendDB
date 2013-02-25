#include "extenddberror.h"

static XTDBErrDesc errDesc[]= {

   { XTDB_OK                 ,"Success."},
   { XTDB_NO_MEM             ,"No memory."} ,
   { XTDB_INVALID_BSON       ,"Invalid bson."},
   { XTDB_NIN_REQUIRES_ARRAY ,"$nin requires array as argument."},
   { XTDB_IN_REQUIRES_ARRAY , "$in requires array as argument."},
   { XTDB_ALL_REQUIRES_ARRAY ,"$all requres array as argument."},
   { XTDB_IO_ERR             ,"IO Error."},
   { XTDB_FILE_NOT_FOUND     ,"File not found."},
   { XTDB_MAIN_DB_NOT_FOUND  ,"Main DB not found."},
   { XTDB_DESC_NOT_FOUND     ,"Descriptr not found."},
   { XTDB_INDEX_NOT_FOUND    ,"Index not found."},
   { XTDB_DB_OPEN_FAILED     ,"Open failed."},
   { XTDB_INDEX_EXISTS       ,"Index already exists."},
   { XTDB_NO_ENTRY           ,"No such entry."},
   { XTDB_ENTRY_EXISTS       ,"Entry already exists."},
   { XTDB_CORRUPT_DB         ,"Database is corrupt."},
   { XTDB_NO_SPACE           ,"No disk space."},
   { XTDB_PERM_DENIED        ,"Permission denied."},
   { XTDB_INVALID_CURSOR     ,"Invalid cursor."},
   { XTDB_MIX_MOD_NONMOD_OP  ,"Mixing of modifier and non modifier operators."},
   { XTDB_INVALID_OBJ_MOD    ,"Invalid object modifier."},
   { XTDB_MOD_SHOULD_BE_OBJ  ,"Modifier argument should be of type BSON_OBJECT."},
   { XTDB_INVALID_INPUT      ,"Invalid input."},
   { XTDB_CANNOT_MODIFY_ID   ,"Cannot modify _id field."},
   { XTDB_MOD_ERR            ,"Error during object modifier."},
   { XTDB_OTHER_ERR          ,"Other error."},
};

const char* XTDBGetErrString(XTDBError err) {
   return errDesc[err].errString;
}
