%module extenddbint
%include "stdint.i"
%{
#define SWIG_FILE_WITH_INIT
#include "extenddb.h"
#include "bson.h"
%}
struct XTDBHandle;
typedef struct XTDBHandle* XTDBHandlePtr; 
#ifdef SWIGPYTHON
%typemap(in) (BinaryStr bStr)
{
  if (!PyString_Check($input)) {
    PyErr_SetString(PyExc_ValueError,"Expected a string");
    return NULL;
  }
  $1.data = PyString_AsString($input);
  $1.len  = PyString_Size($input);
}


%typemap(out) BinaryStr {
    $result = PyString_FromStringAndSize($1.data,$1.len);
}

%typemap(out) bson* {
    $result = PyString_FromStringAndSize($1->data,bson_size($1));
    //bson_finish($1);
    free($1);
}

%typemap(in) XTDBHandlePtr* out (XTDBHandlePtr ptr) {
   $1 = &ptr;
}

%typemap(argout) XTDBHandlePtr* out {
   if (result != XTDB_OK) {
      PyErr_SetString(PyExc_RuntimeError, XTDBGetErrString(result));
      return NULL;
   }
   $result = SWIG_NewPointerObj(SWIG_as_voidptr(*$1), SWIGTYPE_p_XTDBHandle, 0 |  0 );
}

%typemap(in) bson* INPUT {
   bson *val = malloc(sizeof(bson));
   bson_init_finished_data(val,(char*) PyString_AsString($input));
   $1=val;
}

%typemap(in) bson* INPUT2 {
   bson *val = malloc(sizeof(bson));
   bson_init_finished_data(val,(char*) PyString_AsString($input));
   $1=val;
}
%typemap(freeargs) bson* INPUT {
   free($1)
}

%typemap(freeargs) bson* INPUT2 {
   free($1)
}

%typemap(in) (XTDBHandle* handle) {
    $1=$input;
}

%typemap(in) bson* OUTPUT {
   bson out;
   bson_init(&out);
   bson_finish(&out);
   $1=&out;
}

%typemap(in) BinaryStr* outKey {
   BinaryStr out;
   $1=&out;
}

%typemap(argout) BinaryStr* outKey {
   if (result) {
      free($1->data);
   }
}

%typemap(argout) bson* OUTPUT {
   PyObject* o = PyString_FromStringAndSize($1->data,bson_size($1));
   $result = SWIG_Python_AppendOutput($result,o);
}

#endif
%include "extenddberror.h"
XTDBError XTDBInitHandle(const char* dbName,const char* dataDir,XTDBHandlePtr* out);
void XTDBFreeHandle(XTDBHandlePtr handle);

uint8_t XTDBCreateIndex(XTDBHandlePtr handle,char* fieldName);
uint8_t XTDBDropIndex(XTDBHandlePtr handle,char* fieldName);
XTDBError XTDBDrop(XTDBHandlePtr handle);
uint8_t XTDBSync(XTDBHandlePtr handle,uint8_t diskSync);

uint8_t XTDBInsert(XTDBHandlePtr handle,bson* INPUT);
XTDBCursor* XTDBFind(XTDBHandlePtr handle,bson* INPUT);
uint8_t XTDBCursorNext(XTDBCursor* cursor,
                      BinaryStr* outKey,
                      bson* OUTPUT);
void XTDBCursorFree(XTDBCursor* cursor);
uint8_t XTDBUpdate(XTDBHandlePtr handle,bson* INPUT,bson* INPUT2,uint8_t upsert);
uint8_t XTDBRemove(XTDBHandlePtr handle,bson* INPUT);
int64_t XTDBCount(XTDBHandlePtr handle, bson* INPUT);
void XTDBGetLastErrorBson(XTDBHandlePtr handle,bson* OUTPUT);
