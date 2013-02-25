#include <stdio.h>
#include "bson.h"
#include "extenddb.h"
#include "objmod.h"

#define TEST_ASSERT(msg,condition) \
   if (!(condition)) {\
      bson_print(&error);\
      printf("ASSERTION FAILURE -- %s : %s \n",msg,#condition);\
      assert(0);\
      exit(-1);\
   } else {\
      printf("%s \n\t-- success.\n",msg);\
   }

typedef int (*TestHandler)();

typedef struct TestDesc {
   char* name;
   char* description;
   TestHandler handler;
}TestDesc;

int TestSetMod(){
   bson o;
   bson mod;
   bson out;
   bson error;

   bson_init(&o);
   bson_append_int(&o,"a",1);
   bson_finish(&o);

   bson_init(&mod);
   
   bson_append_int(&mod,"a",100);
   bson_finish(&mod);
   TEST_ASSERT("Setting existing integer",ObjModifyBson(&o,&mod,&out,&error) == XTDB_INVALID_OBJ_MOD);
   bson_print(&mod);
   bson_destroy(&mod);
   
   bson_init(&mod);
   
   bson_append_start_object(&mod,"$set");
   bson_append_int(&mod,"a",100);
   bson_append_finish_object(&mod);
   bson_finish(&mod);
   TEST_ASSERT("Setting existing integer",ObjModifyBson(&o,&mod,&out,&error) == XTDB_OK);
   bson_print(&mod);
   bson_destroy(&mod);
   bson_print(&out);
   bson_destroy(&out);
   
   bson_init(&mod);
   bson_append_start_object(&mod,"$set");
   bson_append_int(&mod,"c.b",100);
   bson_append_finish_object(&mod);
   bson_finish(&mod);
   printf("Modifier\n");
   bson_print(&mod);
   TEST_ASSERT("Setting existing integer",ObjModifyBson(&o,&mod,&out,&error) == XTDB_OK);
   bson_destroy(&mod);
   printf("OUTPUT\n");
   bson_print(&out);
   bson_destroy(&out);
   
   bson_init(&mod);
   bson_append_start_object(&mod,"$set");
   bson_append_int(&mod,"c.b",100);
   bson_append_int(&mod,"c.d",100);
   bson_append_finish_object(&mod);
   bson_finish(&mod);
   printf("Modifier\n");
   bson_print(&mod);
   TEST_ASSERT("Setting existing integer",ObjModifyBson(&o,&mod,&out,&error) == XTDB_OK);
   bson_destroy(&mod);
   printf("OUTPUT\n");
   bson_print(&out);
   bson_destroy(&out);
   return 0;
}

int TestUnsetMod(){
   bson o;
   bson mod;
   bson out;
   bson error;

   bson_init(&o);
   bson_append_int(&o,"a",1);
   bson_append_start_object(&o,"so");
   bson_append_int(&o,"b",100);
   bson_append_finish_object(&o);
   bson_finish(&o);


   bson_init(&mod);
   
   bson_append_start_object(&mod,"$unset");
   bson_append_int(&mod,"a",100);
   bson_append_finish_object(&mod);
   bson_finish(&mod);
   TEST_ASSERT("Unsetting existing integer",ObjModifyBson(&o,&mod,&out,&error) == XTDB_OK);
   bson_print(&mod);
   bson_destroy(&mod);
   bson_print(&out);
   bson_destroy(&out);

   bson_init(&mod);
   
   bson_append_start_object(&mod,"$unset");
   bson_append_int(&mod,"so.b",100);
   bson_append_finish_object(&mod);
   bson_finish(&mod);
   TEST_ASSERT("Unsetting existing integer",ObjModifyBson(&o,&mod,&out,&error) == XTDB_OK);
   bson_print(&mod);
   bson_destroy(&mod);
   bson_print(&out);
   bson_destroy(&out);
   return 0;
}

int TestIncMod(){
   bson o;
   bson mod;
   bson out;
   bson error;

   bson_init(&o);
   bson_append_int(&o,"a",1);
   bson_append_start_object(&o,"so");
   bson_append_int(&o,"b",100);
   bson_append_finish_object(&o);
   bson_finish(&o);


   bson_init(&mod);
   
   bson_append_start_object(&mod,"$inc");
   bson_append_int(&mod,"a",100);
   bson_append_finish_object(&mod);
   bson_finish(&mod);
   TEST_ASSERT("Incrementing existing integer",ObjModifyBson(&o,&mod,&out,&error) == XTDB_OK);
   bson_print(&mod);
   bson_destroy(&mod);
   bson_print(&out);
   bson_destroy(&out);

   bson_init(&mod);
   
   bson_append_start_object(&mod,"$inc");
   bson_append_int(&mod,"c",100);
   bson_append_finish_object(&mod);
   bson_finish(&mod);
   TEST_ASSERT("Incrementing existing integer",ObjModifyBson(&o,&mod,&out,&error) == XTDB_OK);
   bson_print(&mod);
   bson_destroy(&mod);
   bson_print(&out);
   bson_destroy(&out);

   bson_init(&mod);
   
   bson_append_start_object(&mod,"$inc");
   bson_append_int(&mod,"so.b",100);
   bson_append_finish_object(&mod);
   bson_finish(&mod);
   TEST_ASSERT("Incrementing existing integer",ObjModifyBson(&o,&mod,&out,&error) == XTDB_OK);
   bson_print(&mod);
   bson_destroy(&mod);
   bson_print(&out);
   bson_destroy(&out);
   return 0;
}

int TestArrSetMod(){
   bson o;
   bson mod;
   bson out;
   bson error;

   bson_init(&o);
   bson_append_int(&o,"a",1);
   bson_append_start_object(&o,"so");
   bson_append_int(&o,"b",100);
   bson_append_finish_object(&o);
   bson_append_start_array(&o,"sq");
   bson_append_finish_object(&o);
   bson_append_start_array(&o,"arr");
   bson_append_int(&o,"",1000);
   bson_append_start_object(&o,"");
   bson_append_int(&o,"i",500);
   bson_append_finish_object(&o);
   bson_append_finish_object(&o);
   bson_finish(&o);

   bson_print(&o);

   bson_init(&mod);
   bson_append_start_object(&mod,"$arrSet");
   bson_append_start_object(&mod,"arr");

   bson_append_start_object(&mod,"query");
   bson_append_int(&mod,"",1000);
   bson_append_finish_object(&mod);

   bson_append_start_object(&mod,"value");
   bson_append_start_object(&mod,"$inc");
   bson_append_int(&mod,"",1000);
   bson_append_finish_object(&mod);

   bson_append_finish_object(&mod);
   bson_append_finish_object(&mod);
   bson_finish(&mod);
   bson_print(&mod);
   TEST_ASSERT("Incrementing existing integer",ObjModifyBson(&o,&mod,&out,&error) == XTDB_OK);
   bson_print(&out);
   bson_destroy(&mod);
   bson_destroy(&out);

   bson_init(&mod);
   bson_append_start_object(&mod,"$arrSet");
   bson_append_start_object(&mod,"arr");

   bson_append_start_object(&mod,"query");
   bson_append_start_object(&mod,"");
   bson_append_int(&mod,"i",500);
   bson_append_finish_object(&mod);
   bson_append_finish_object(&mod);

   bson_append_start_object(&mod,"value");
   bson_append_start_object(&mod,"$unset");
   bson_append_int(&mod,"",1000);
   bson_append_finish_object(&mod);

   bson_append_finish_object(&mod);
   bson_append_finish_object(&mod);
   bson_finish(&mod);
   bson_print(&mod);
   TEST_ASSERT("Incrementing existing integer",ObjModifyBson(&o,&mod,&out,&error) == XTDB_OK);
   bson_print(&out);
   bson_destroy(&mod);
   bson_destroy(&out);

   bson_init(&mod);
   bson_append_start_object(&mod,"$arrSet");
   bson_append_start_object(&mod,"sq");

   bson_append_start_object(&mod,"query");
   bson_append_start_object(&mod,"");
   bson_append_int(&mod,"i",500);
   bson_append_finish_object(&mod);
   bson_append_finish_object(&mod);

   bson_append_start_object(&mod,"value");
   bson_append_start_object(&mod,"$set");
   bson_append_int(&mod,"",1000);
   bson_append_finish_object(&mod);

   bson_append_finish_object(&mod);
   bson_append_bool(&mod,"upsert",True);
   bson_append_finish_object(&mod);
   bson_finish(&mod);
   bson_print(&mod);
   TEST_ASSERT("Incrementing existing integer",ObjModifyBson(&o,&mod,&out,&error) == XTDB_OK);
   bson_print(&out);
   bson_destroy(&mod);
   bson_destroy(&out);
   return 0;
}

TestDesc tests[] ={
                   {"SetModifier","Queries related to string.",TestSetMod},
                   {"UnsetModifier","Queries related to string.",TestUnsetMod},
                   {"IncModifier","Queries related to string.",TestIncMod},
                   {"ArrSetModifer","Queries related to string.",TestArrSetMod},
};

void
RunTests(int i){
   int len = sizeof(tests)/sizeof(tests[0]);
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
      printf("%s\n",tests[i].name);
      //printf("--------------------------\n");
      if(!tests[i].handler()) {
        
         printf("\t--Passed.\n");
      } else{
         printf("\t--Failed.\n");
         
      }
   }
}

int main(int argc,char** argv) {
   int i = -1;
   if (argc > 1) {
      i = atoi(argv[1]);
   }

   RunTests(i);

}
