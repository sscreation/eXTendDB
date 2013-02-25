#include "extenddb.h"
#include "stringutil.h"
#include "query.h"
#include "dbbe.h"

#define DB_NAME "testmdbdata"

XTDBHandlePtr dbHandle;
char curDir[1024];
int count;

#define TEST_ASSERT(msg,condition) \
   if (!(condition)) {\
      printf("ASSERTION FAILURE -- %s : %s \n",msg,#condition);\
      assert(0);\
      exit(-1);\
   }

static inline double UTime(){
   struct timeval tim;
   double ret;
   gettimeofday(&tim, NULL);
   ret = tim.tv_sec+(tim.tv_usec/1000000.0);
   return ret;
}

int
TestXTDBIndex(){
   struct stat st;
   String idxFile;
   uint8_t ret;
   bson obj;
   DataBaseBE* db ;
   XTDBError err = XTDBInitHandle(DB_NAME,curDir,&dbHandle);
   if (err != XTDB_OK) {
      return -1;
   }
   StrInit(&idxFile);
       
   TEST_ASSERT("DB initialization",dbHandle);

   ret = XTDBCreateIndex(dbHandle,"idxa");
   TEST_ASSERT("Index creation",ret); 
   StrAppendFmt(&idxFile,"%s/%s",curDir,"testmdbdata.idxa.idx");
   TEST_ASSERT("Index file creation",!lstat(idxFile.ptr,&st)); 
   ret = XTDBDropIndex(dbHandle,"idxa");
   TEST_ASSERT("Index deletion",ret); 
   TEST_ASSERT("Index file removal",lstat(idxFile.ptr,&st)); 
   XTDBFreeHandle(dbHandle);

   err = XTDBInitHandle(DB_NAME,curDir,&dbHandle);
   TEST_ASSERT("DB after index removal",dbHandle); 

   bson_init(&obj);
   bson_append_int(&obj,"idxa",1);
   bson_finish(&obj);
   if (!XTDBInsert(dbHandle,&obj)) {
      TEST_ASSERT("Insert failed",0);
   }
   bson_destroy(&obj);

   bson_init(&obj);
   bson_append_string(&obj,"idxa","abcdef");
   bson_finish(&obj);
   if (!XTDBInsert(dbHandle,&obj)) {
      TEST_ASSERT("Insert failed",0);
   }
   bson_destroy(&obj);

   bson_init(&obj);
   bson_append_string(&obj,"idxb","abcdef");
   bson_finish(&obj);
   if (!XTDBInsert(dbHandle,&obj)) {
      TEST_ASSERT("Insert failed",0);
   }
   bson_destroy(&obj);
   XTDBSync(dbHandle,False);

   ret = XTDBCreateIndex(dbHandle,"idxa");
   TEST_ASSERT("Index creation after existing values",ret);
   XTDBFreeHandle(dbHandle);
   
   /* Check whether the index actually has the values.
    * Freeing the handle because DB cant be opened since
    * it is already opened.
    */
   db = DBInit(idxFile.ptr,0);
   TEST_ASSERT("Index db open.",DBOpen(db,BDBOREADER));
   TEST_ASSERT("Index db init.",db);
   printf("DB count %llu\n",DBCount(db,NULL));
   TEST_ASSERT("Checking indexed value in index db.",DBCount(db,NULL) == 2);
   DBClose(db);
   DBFree(db);

   err = XTDBInitHandle(DB_NAME,curDir,&dbHandle);
   TEST_ASSERT("DB after index removal",dbHandle); 
   ret = XTDBDropIndex(dbHandle,"idxa");
   TEST_ASSERT("Index deletion with values",ret); 
   // Add tests to verify the index db has those values.
   XTDBFreeHandle(dbHandle);
   //XTDBDrop(dbHandle);
   StrFree(&idxFile);
   return 0;
}

int 
TestXTDBInsert(){
   int i;
   // return 0;
   double t1,t2;
   char a[1024];
   bson b;

   XTDBError err = XTDBInitHandle(DB_NAME,curDir,&dbHandle);
   if (err != XTDB_OK) {
      return -1;
   }
   TEST_ASSERT("DB initialization",dbHandle);
   XTDBCreateIndex(dbHandle,"string");

   t1= UTime();
   for (i=0;i<count;i++){
      bson_init(&b);
      snprintf(a,1024,"String value %d",i);
      bson_append_string(&b,"string",a);

      bson_append_int(&b,"Int",0);
      bson_finish(&b);
      TEST_ASSERT("Inserting in loop.",XTDBInsert(dbHandle,&b));
      if(i%1000 == 0) {
         //printf("Inserted %d\n",i);
         XTDBSync(dbHandle,False);
      }
      bson_destroy(&b);
   }
   XTDBSync(dbHandle,False);
   t2= UTime();
   printf("Insertion of %d items with one index took %lf seconds.\n",count,t2-t1);
   int64_t c1 = XTDBCount(dbHandle,&b),c2;
   
   XTDBSync(dbHandle,False);
   TEST_ASSERT("Insert count",c1==count);
   printf("remove\n\n");
   TEST_ASSERT("Remove worked or not.",XTDBRemove(dbHandle,&b));
   XTDBSync(dbHandle,False);
   c2 = XTDBCount(dbHandle,&b);
   printf("XXX %d\n",c2);
   TEST_ASSERT("Remove count",(c2==0 && XTDBGetLastError(dbHandle)== XTDB_OK));
   printf("%ld %ld \n",c1,c2);
   PrintProfile(dbHandle);
   bson_destroy(&b);   
   XTDBFreeHandle(dbHandle);
   //XTDBDrop(dbHandle);
   return 0;
}

void
PrintDB(char* caption,DataBaseBE* db){ 
   void *iter=DBIter(db);
   printf("-- %s --\n",caption);
   BinaryStr key,val;
   for (;DBIterCur(db,iter,&key,&val);DBIterNext(db,iter)) {
      printf("\t'%s':'%s'\n",key.data,val.data);
   }
}

int
TestDB(){
   String dbName;
   StrInit(&dbName);
   BinaryStr key,val,oKey,oVal;
   DataBaseBE* db;
   int i;
   double tStart,tEnd;
   StrAppendFmt(&dbName,"%s/%s",curDir,"testdb.tc");
   unlink(dbName.ptr);
   db = DBInit(dbName.ptr,BDBOWRITER|BDBOCREAT);

   DBOpen(db,BDBOWRITER|BDBOCREAT);

   TEST_ASSERT("DB init",db);
   char *k1 = "A",*k2="B",*k3="C";
   char *v1 = "V1",*v2="V2",*v3="V3";
   StrToBStr(k1,&key);
   StrToBStr(v1,&val);
   TEST_ASSERT("DB Set",DBSet(db,&key,&val,False));
   TEST_ASSERT("DB Get",DBGet(db,&key,&oVal));
   TEST_ASSERT("DB Cmp",BStrEqual(&oVal,&val));
   TEST_ASSERT("DB Count",DBCount(db,NULL)==1);
   BinaryStrFree(&oVal);
   StrToBStr(v2,&val);
   TEST_ASSERT("DB Set",DBSet(db,&key,&val,False));
   TEST_ASSERT("DB Get",DBGet(db,&key,&oVal));
   TEST_ASSERT("DB Cmp",BStrEqual(&oVal,&val));
   TEST_ASSERT("DB Count",DBCount(db,NULL)==1);
   BinaryStrFree(&oVal);
   StrToBStr(v3,&val);
   TEST_ASSERT("DB Set dup",DBSet(db,&key,&val,True));
   TEST_ASSERT("DB Count dup",DBCount(db,&key)==2);
   TEST_ASSERT("DB delkey-val dup",DBDeleteKeyVal(db,&key,&val));
   TEST_ASSERT("DB Count dup",DBCount(db,&key)==1);
   StrToBStr(v3,&val);
   TEST_ASSERT("DB Set dup2",DBSet(db,&key,&val,True));
   TEST_ASSERT("DB Count dup2",DBCount(db,&key)==2);
   TEST_ASSERT("DB Set dup2",DBSet(db,&key,&val,True));
   TEST_ASSERT("DB Count dup2",DBCount(db,&key)==3);
   TEST_ASSERT("DB delkey-val dup2",DBDeleteKeyVal(db,&key,&val));
   TEST_ASSERT("DB Count dup 2",DBCount(db,&key)==1);
   TEST_ASSERT("DB delkey-val->NULL",DBDeleteKeyVal(db,&key,NULL));
   TEST_ASSERT("DB Count dup 2",DBCount(db,&key)==0);
   TEST_ASSERT("DB Insert",DBSet(db,&key,&val,True));
   TEST_ASSERT("DB Count insert",DBCount(db,&key)==1);
   TEST_ASSERT("DB Delete",DBDelete(db,&key));
   TEST_ASSERT("DB Count delete",DBCount(db,&key)==0);
   char *str = "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW";
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW";
   char *restr = "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW";
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW";
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW";
   StrToBStr(str,&val);
   bson_oid_t oid;
   bson_oid_t *oids;
   int count = 2000000;
   char buf[2048];
   oids = malloc(sizeof(oid)*count);
   double t1=UTime(),t2;
   int j;
   val.data = buf;
   val.len = 2048;
   //int null = open("/dev/null",O_RDWR);

   DBSync(db,False);
   for (i=0;i<count;i++) {
      bson_oid_gen(&oids[i]);
   }
   t2=UTime();
   printf("Time taken %lf\n",t2-t1);
   for (j=0;j<2;j++) {
      t1=UTime();
      for (i=0;i<count;i++) {
         //bson_oid_gen(&oids[i]);
         key.data =oids[i].bytes;
         key.len = sizeof(oid.bytes);
         val = key;
         DBSet(db,&key,&val,False);
         if (i%500 ==0) {
            DBSync(db,False);
         }
      }
      DBSync(db,False);
      for (i=0;i<count;i++) {
         //bson_oid_gen(&oids[i]);
         key.data =oids[i].bytes;
         key.len = sizeof(oid.bytes);

         DBDelete(db,&key);
      }
      t2=UTime();
      printf("Insert time %lf\n",t2-t1);
   }
   t2=UTime();
   printf("First insert time %lf\n",t2-t1);
   return 0;
   StrToBStr(restr,&val);
   t1=UTime();
   for (i=0;i<count;i++) {
      //bson_oid_gen(&oids[i]);
      key.data =oids[i].bytes;
      key.len = sizeof(oid.bytes);

      DBSet(db,&key,&val,False);
      if (i%500 ==0) {
         DBSync(db,False);
      }
   }
   restr =     "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW";
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW"
               "ABCDEFHGIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVW";
   StrToBStr(restr,&val);
   t2=UTime();
   printf("Rewrite time %lf\n",t2-t1);
   t1=UTime();
   for (i=0;i<count;i++) {
      //bson_oid_gen(&oids[i]);
      key.data =oids[i].bytes;
      key.len = sizeof(oid.bytes);

      DBSet(db,&key,&val,False);
      if (i%500 ==0) {
         DBSync(db,False);
      }
   }
   t2=UTime();
   printf("Rewrite time %lf\n",t2-t1);
   StrToBStr(str,&val);
   t1=UTime();
   for (i=0;i<count;i++) {
      bson_oid_gen(&oids[i]);
      key.data =oids[i].bytes;
      key.len = sizeof(oid.bytes);

      DBSet(db,&key,&val,False);
      if (i%500 ==0) {
         DBSync(db,False);
      }
   }
   t2=UTime();
   printf("Reinsert time %lf\n",t2-t1);
   printf("Count %ld \n",DBCount(db,NULL));
   DBSync(db,False);
   DBClose(db);
   DBFree(db);
   StrFree(&dbName);
   return 0;
}

typedef int (*TestHandler)();

typedef struct TestDesc {
   char* name;
   char* description;
   TestHandler handler;
}TestDesc;

int 
TestXTDBRemove(){
   int i;
   int64_t c1,c2,c3;
   XTDBError err = XTDBInitHandle(DB_NAME,curDir,&dbHandle);
   if (err != XTDB_OK) {
      return -1;
   }
   TEST_ASSERT("DB initialization",dbHandle);
   bson b;
   bson_init(&b);
   XTDBCreateIndex(dbHandle,"string");
   bson_append_string(&b,"string","String value");
   bson_finish(&b);
   c1 = XTDBCount(dbHandle,&b);

   XTDBInsert(dbHandle,&b);
   c2 = XTDBCount(dbHandle,&b);
   XTDBRemove(dbHandle,&b);
   c3 = XTDBCount(dbHandle,&b);
   printf("%ld %ld %ld \n",c1,c2,c3);
   bson_destroy(&b);
   XTDBDrop(dbHandle);
   return 0;
}

void 
PrintOid(BinaryStr* key){
   int i=0;
   for (;i<key->len;i++) {
      printf("%02x",(uint8_t)key->data[i]);
   }
}

void 
PrintXTDB(XTDBHandlePtr dbHandle) {
   bson b,valueBson;
   BinaryStr key;
   bson_init(&b);
   bson_finish(&b);
   XTDBCursor *cur = XTDBFind(dbHandle,&b);

   printf("XTDB Dump start -- \n\n");
   while (XTDBCursorNext(cur,&key,&valueBson)){
      //bson_init_finished_data(&valueBson,(char*)value.data);
      PrintOid(&key);
      printf("\n");
      bson_print(&valueBson);
      bson_destroy(&valueBson);
      BinaryStrFree(&key);
   }
   XTDBCursorFree(cur);
   bson_destroy(&b);
   printf("\nXTDB Dump finish -- \n\n");
}

int
CompareBSON(bson* b1, bson* b2){
   bson_iterator i1,i2;
   bson_iterator_init(&i1,b1);
   bson_iterator_init(&i2,b2);
   bson_type t1,t2;

   while((t1 = bson_iterator_next(&i1))) { 
      const char* keyVal = bson_iterator_key(&i1);
      t2=bson_find(&i2,b2,keyVal);
      if (t1!=t2) {
         return -1;
      }
      const char *str1=bson_iterator_string(&i1);
      const char *str2=bson_iterator_string(&i2);
      if (strcmp(str1,str2)) {
         return -1;
      } 
   }
}

int
TestXTDBUpdate(){
   XTDBError err = XTDBInitHandle(DB_NAME,curDir,&dbHandle);
   if (err != XTDB_OK) {
      return -1;
   }
   TEST_ASSERT("DB initialization",dbHandle);
   bson b,n,valBson;
   bson_iterator bsonItr; 
   int i;
   
   bson_init(&b);
   bson_append_string(&b,"idx1","Index 1 value");
   bson_append_string(&b,"idx2","Index 2 value");
   bson_finish(&b);
  
   TEST_ASSERT("Checking index creation idx1",XTDBCreateIndex(dbHandle,"idx1"));
   TEST_ASSERT("Checking index creation idx1",XTDBCreateIndex(dbHandle,"idx2"));
   TEST_ASSERT("XTDBUpsert obj.",XTDBUpdate(dbHandle,&b,&b,True));
   BinaryStr key;
   TEST_ASSERT("XTDBUpsert count.",XTDBCount(dbHandle,&b)==1);
   XTDBCursor* cur = XTDBFind(dbHandle,&b);
   
   while (XTDBCursorNext(cur,&key,&valBson)) {
      bson_print(&valBson);
   }
   TEST_ASSERT("Value is not same as it was updated.",!CompareBSON(&b,&valBson));
   bson_destroy(&valBson);
   XTDBCursorFree(cur);

   bson_init(&n);
   bson_append_string(&n,"idx1","Index 1 new value");
   bson_append_string(&n,"idx2","Index 2 new value");
   bson_finish(&n);
   TEST_ASSERT("XTDBUpdate ",XTDBUpdate(dbHandle,&b,&n,True));

   XTDBFindById(dbHandle,&key,&valBson);
   bson_print(&valBson);
   TEST_ASSERT("New value is not updated.",!CompareBSON(&n,&valBson));
   //PrintProfile(dbHandle);
   
   for (i=0;i<count;i++) {
      XTDBUpdate(dbHandle,&n,&b,True);
      //XTDBSync(dbHandle,False);
      if (i%10000 == 0) {
         printf("%d\n",i);
      }
   }
   PrintProfile(dbHandle);
   bson_destroy(&b);
   bson_destroy(&n);
   //XTDBDrop(dbHandle);
   return 0;
}

TestDesc tests[] ={
                   {"DB-Test","Lowlevel backend db tests.",TestDB},
                   {"XTDB-Index","Index creation deletion tests.",TestXTDBIndex},
                   {"XTDB-Remove","Remove entries.",TestXTDBRemove},
                   {"XTDB-Insert","BSON insertion tests.",TestXTDBInsert},
                   {"XTDB-Update","BSON document update tests.",TestXTDBUpdate}
                  };

int PrepareTest() {
   XTDBError err = XTDBInitHandle(DB_NAME,curDir,&dbHandle);
   if (err != XTDB_OK) {
      return -1;
   }
   if (!XTDBDrop(dbHandle)) {
      return -1;
   }
   return 0;
}

void
RunTests(int i){
   int len = sizeof(tests)/sizeof(tests[0]);
   XTDBError err = XTDBInitHandle(DB_NAME,curDir,&dbHandle);
   XTDBDrop(dbHandle);
   if (i<0) {
      i=0;
   } else {
      if (i>=len) {
         printf("Invalid test id %d , valid tests are ...\n\n",i);
         for (i=0;i<len;i++) {
            printf("%d %-015s -- %s\n",i,tests[i].name,tests[i].description);
         }
         return;
      }
      len = i+1;
   }

   for (;i<len;i++) {
      /*if(PrepareTest()) {
         printf("Prepare test failed.\n");
         return;
      }*/
      printf("%s\n",tests[i].name);
      //printf("--------------------------\n");
      if(!tests[i].handler()) {
         printf("\t--Passed.\n");
      } else{
         printf("\t--Failed.\n");
         
      }
   }
}

int main(int argc, char** argv){
   if (argc > 1) {
      strncpy(curDir,argv[1],sizeof(curDir));
   } else {
      getcwd(curDir,sizeof(curDir));
   }
   int i = -1;
   if (argc > 2) {
      i = atoi(argv[2]);
   }
   count = 10000;
   if (argc > 3) {
      count = atol(argv[3]);
   }
   RunTests(i);

   return 0;
}
