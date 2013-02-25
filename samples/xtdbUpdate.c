#include "extenddb.h"
#define DB_NAME "testxtdb"

void
PrintDB(XTDBHandlePtr ptr,char* msg) {
   int i;
   bson query;
   BinaryStr bStr;
   bson out;
   XTDBCursor *cursor;

   printf("Printing DB contents %s <BEGIN> \n",msg);
   bson_init(&query);
   bson_finish(&query);
   cursor = XTDBFind(ptr,&query);
   if (!cursor) {
      printf("Cursor creation failed.\n");
   }
   while (XTDBCursorNext(cursor,&bStr,&out)) {
      BinaryStrFree(&bStr);
      bson_print(&out);
      bson_destroy(&out);   
   } 

   bson_destroy(&query);
   printf("Printing DB <END>\n");
   XTDBCursorFree(cursor);
}

int 
main(int argc, char** argv) {
   int n=10;
   bson obj;
   XTDBHandlePtr ptr;
   XTDBError err;
   bson query;
   int i;
   XTDBCursor * cursor ;
   bson out;
   bson newVal;
   BinaryStr bStr;
   bson error;

   if ((err = XTDBInitHandle(DB_NAME,"./",&ptr)) != XTDB_OK ) {
      printf("DB creation failed '%s'\n",XTDBGetErrString(err));
      return -1;
   }

   // Insert a document 
   // { 'a' : 100 } and {'a':200}
   
   bson_init(&obj);   
   bson_append_int(&obj,"a",100);
   bson_finish(&obj);
   
   if (!XTDBInsert(ptr,&obj)){
      printf("Error inserting document. %s\n",XTDBGetErrString(XTDBGetLastError(ptr)));
      return -1;
   }

   bson_destroy(&obj);

   bson_init(&obj);   
   bson_append_int(&obj,"a",200);
   bson_finish(&obj);
   
   if (!XTDBInsert(ptr,&obj)){
      printf("Error inserting document. %s\n",XTDBGetErrString(XTDBGetLastError(ptr)));
      return -1;
   }

   bson_destroy(&obj);

   PrintDB(ptr,"After inserting records.");
   
   printf("Updating {'a':100} to {'a':150} \n");
   bson_init(&newVal);
   bson_append_int(&newVal,"a",150);
   bson_finish(&newVal);
   
   bson_init(&query);
   bson_append_int(&query,"a",100);
   bson_finish(&query);

   if (!XTDBUpdate(ptr,&query,&newVal,False)){
      printf("Error updating document. %s\n",XTDBGetErrString(XTDBGetLastError(ptr)));
      return -1;
   }

   printf("Update response\n");
   XTDBGetLastErrorBson(ptr,&error);
   bson_print(&error);
   bson_destroy(&error);
   bson_destroy(&query);
   bson_destroy(&newVal);

   PrintDB(ptr,"After update records.");

   printf("Updating all documents less than 300. {'a':{'$lt':300}} to {'a':300} \n");

   bson_init(&query);
   bson_append_start_object(&query,"a");
   bson_append_int(&query,"$lt",300);
   bson_finish(&query);

   bson_init(&newVal);
   bson_append_int(&newVal,"a",300);
   bson_finish(&newVal);
   if (!XTDBUpdate(ptr,&query,&newVal,False)){
      printf("Error updating document. %s\n",XTDBGetErrString(XTDBGetLastError(ptr)));
      return -1;
   }

   printf("Update response\n");
   XTDBGetLastErrorBson(ptr,&error);
   bson_print(&error);
   bson_destroy(&error);
   bson_destroy(&newVal);
   bson_destroy(&query);


   PrintDB(ptr,"After updating with lt 300 query.");

   return 0;
}
