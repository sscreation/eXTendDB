#include "extenddb.h"
#define DB_NAME "testxtdb"

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
   BinaryStr bStr;

   if (argc > 2) {
      n = atoi(argv[1]);
   } 
   
   if ((err = XTDBInitHandle(DB_NAME,"./",&ptr)) != XTDB_OK ) {
      printf("DB creation failed '%s'\n",XTDBGetErrString(err));
      return -1;
   }
   for (i=0;i<n;i++) {
      bson error;
      char str[50];
      snprintf(str,50,"%d",i);
      bson_init(&obj);
      
      bson_append_int(&obj,"i",i);
      bson_append_string(&obj,"i_s",str);
      bson_finish(&obj);
      if (!XTDBInsert(ptr,&obj)){
         printf("Error happened. %s\n",XTDBGetErrString(XTDBGetLastError(ptr)));
         return -1;
      }
      printf("Storing object %d\n",i);
      //bson_print(&obj);
      XTDBGetLastErrorBson(ptr,&error);
      //printf("Response object for insert is.\n");
      bson_print(&error);
      bson_destroy(&error);
      bson_destroy(&obj);
   } 
   printf("Successfully stored %d documents.\n",n);
   XTDBFreeHandle(ptr);

   if ((err = XTDBInitHandle(DB_NAME,"./",&ptr)) != XTDB_OK ) {
      printf("DB creation failed '%s'\n",XTDBGetErrString(err));
      return -1;
   }
   printf("Verifying stored documents.\n");
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

   XTDBCursorFree(cursor);
   bson_destroy(&query);
   //  Remove the data base
   XTDBDrop(ptr);
   return 0;
}
