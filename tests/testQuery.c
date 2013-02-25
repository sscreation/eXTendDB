#include "query.h"
#include "stringutil.h"

#define TEST_ASSERT(msg,condition) \
   if (!(condition)) {\
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

int 
TestStringQuery(){
   bson o,q;
   bson_init(&o);
   bson_init(&q);
   bson_append_string(&o,"s1","abcd");
   bson_append_string(&o,"s2","efgh");
   bson_append_string(&o,"s3","abcd");
   bson_append_start_object(&o,"so");
   bson_append_string(&o,"s1","abcd");
   bson_append_string(&o,"s2","efgh");
   bson_append_string(&o,"s3","abcd");
   bson_append_finish_object(&o);
   bson_finish(&o);
   bson_print(&o);
   // object : {"s1":"abcd,"s2":"efgh","s3":"abcd",
   //           "so":{"s1":"abcd","s2":"efgh","s3":"abcd"}
   //          }
   //
   // query: {"s1":"abcd"}
   bson_append_string(&q,"s1","abcd");
   bson_finish(&q);
   TEST_ASSERT("+ve Simple one string query.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   // query: {"s1":"abcd","s2":"efgh"}
   bson_append_string(&q,"s1","abcd");
   bson_append_string(&q,"s2","efgh");
   bson_finish(&q);
   TEST_ASSERT("+ve Two string query.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   // query: {"s1":"abcd","so":{"s2":"efgh"}} -- match
   bson_append_string(&q,"s1","abcd");
//   bson_append_start_object(&q,"so.s2","efgh");
   bson_append_string(&q,"so.s2","efgh");
  // bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve Sub object element match string query.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   // query: {"s1":"abcd","so":{"s2":"efgh"}} -- match
   bson_append_string(&q,"s1","abcd");
   bson_append_start_object(&q,"so");
   bson_append_string(&q,"s2","efgh");
   bson_append_string(&q,"s1","abcd");
   bson_append_string(&q,"s3","abcd");
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve Sub object match string query.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   // query: {"s1":"abcd","s2":"efghij"} -- no match
   bson_append_string(&q,"s1","abcd");
   bson_append_string(&q,"s2","efghij");
   bson_finish(&q);
   TEST_ASSERT("-ve Partial match string query.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   // query: {"s1":"abcd","so":{"s2":"efghij"}} --no match
   bson_append_string(&q,"s1","abcd");
   bson_append_start_object(&q,"so");
   bson_append_string(&q,"s2","efghij");
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve Sub object match string query no match.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   // query: {"s1":"abcdef","so":{"s2":"efgh"}} --no match
   bson_append_string(&q,"s1","abcdef");
   bson_append_start_object(&q,"so");
   bson_append_string(&q,"s2","efgh");
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve Sub object match string query no match.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"s1");
   bson_append_string(&q,"$lt","abcd");
   bson_finish(&q);
   TEST_ASSERT("-ve Less than query border.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"s1");
   bson_append_string(&q,"$lt","aaaa");
   bson_finish(&q);
   TEST_ASSERT("-ve Less than query not less than.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"s1");
   bson_append_string(&q,"$lt","abcde");
   bson_finish(&q);
   TEST_ASSERT("+ve Less than query match.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"s1");
   bson_append_string(&q,"$lt","qabcd");
   bson_finish(&q);
   TEST_ASSERT("-ve Less than query match.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"s1");
   bson_append_string(&q,"$lte","abcd");
   bson_finish(&q);
   TEST_ASSERT("-ve lte query border.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"s1");
   bson_append_string(&q,"$lte","aaaa");
   bson_finish(&q);
   TEST_ASSERT("-ve lte query not less than.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"s1");
   bson_append_string(&q,"$lte","abcde");
   bson_finish(&q);
   TEST_ASSERT("+ve lte query match.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"s1");
   bson_append_string(&q,"$lte","qabcd");
   bson_finish(&q);
   TEST_ASSERT("-ve lte query match.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"s1");
   bson_append_string(&q,"$gte","abcd");
   bson_finish(&q);
   TEST_ASSERT("+ve gte query border.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"s1");
   bson_append_string(&q,"$gte","aaaa");
   bson_finish(&q);
   TEST_ASSERT("+ve gte greater than.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"s1");
   bson_append_string(&q,"$gte","abcde");
   bson_finish(&q);
   TEST_ASSERT("+ve gte not greater than.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"s1");
   bson_append_string(&q,"$gte","qabcd");
   bson_finish(&q);
   TEST_ASSERT("-ve gte not greater than.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"s1");
   bson_append_string(&q,"$gt","abcd");
   bson_finish(&q);
   TEST_ASSERT("+ve gt query border.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"s1");
   bson_append_string(&q,"$gt","aaaa");
   bson_finish(&q);
   TEST_ASSERT("+ve gt greater than.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"s1");
   bson_append_string(&q,"$gt","abcde");
   bson_finish(&q);
   TEST_ASSERT("+ve gt not greater than.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"s1");
   bson_append_string(&q,"$gt","qabcd");
   bson_finish(&q);
   TEST_ASSERT("-ve gt not greater than.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"s1");
   //range
   bson_append_string(&q,"$lt","qabcd");
   bson_append_string(&q,"$gt","aaaa");
   bson_finish(&q);
   TEST_ASSERT("+ve range values.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   return 0;
}

void
TestAppendElem(bson* in) {
   bson_iterator i;
   bson_iterator_init(&i,in);
   bson_type t;
   while ( t= bson_iterator_next(&i)) {
      bson t;
      bson_init(&t);
      bson_append_element(&t,bson_iterator_key(&i),&i);
      bson_finish(&t);
      bson_print(&t);
      bson_destroy(&t);
   }
}

int 
TestIntQuery(){
   bson o,q;
   bson_init(&o);
   bson_init(&q);
   int i=100;
   int64_t l=150;
   double d=200.0;
   bson_append_int(&o,"i",i);
   bson_append_long(&o,"l",l);
   bson_append_double(&o,"d",d);
   bson_append_start_object(&o,"so");
   bson_append_int(&o,"i",i);
   bson_append_long(&o,"l",l);
   bson_append_double(&o,"d",d);
   bson_append_finish_object(&o);
   bson_finish(&o);
   bson_print(&o);
   printf("Append element");
   TestAppendElem(&o);
   printf("Append element");
   // object :{"i":100,"l":150,"so":{"i":100,"l":150,"d":200.0}}
   // query: {"i":i}
   bson_append_int(&q,"i",i);
   bson_finish(&q);
   TEST_ASSERT("+ve Simple one integer.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   // query: {"i":100,"l":150}
   bson_append_int(&q,"i",i);
   bson_append_long(&q,"l",l);
   bson_finish(&q);
   TEST_ASSERT("+ve Integer and long.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   // query: {"i":100,"l":d}
   bson_append_int(&q,"i",i);
   bson_append_double(&q,"l",(double)l);
   bson_finish(&q);
   TEST_ASSERT("+ve Integer and double for long.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   // query: {"s1":"abcd","so":{"s2":"efgh"}} -- match
   bson_append_int(&q,"i",i);
   //bson_append_start_object(&q,"so");
   bson_append_int(&q,"so.l",l);
   //bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve Sub object int and long.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   // query: {"s1":"abcd","s2":"efghij"} -- no match
   bson_append_int(&q,"i",i);
   bson_append_long(&q,"l",i);
   bson_finish(&q);
   TEST_ASSERT("-ve Partial match int and long.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   // query: {"s1":"abcd","so":{"s2":"efghij"}} --no match
   bson_append_double(&q,"d",d);
   bson_append_start_object(&q,"so");
   bson_append_double(&q,"l",d);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve Sub object no match.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   // query: {"s1":"abcdef","so":{"s2":"efgh"}} --no match
   bson_append_int(&q,"i",l);
   bson_append_start_object(&q,"so");
   bson_append_long(&q,"l",l);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve Sub object no match2.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$lt",i);
   bson_finish(&q);
   TEST_ASSERT("-ve Less than query border.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$lt",i-1);
   bson_finish(&q);
   TEST_ASSERT("-ve Less than query not less than.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$lt",i+1);
   bson_finish(&q);
   TEST_ASSERT("+ve Less than query match.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_long(&q,"$lt",i+1);
   bson_finish(&q);
   TEST_ASSERT("+ve Less than query  match long.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$lte",i);
   bson_finish(&q);
   TEST_ASSERT("+ve lte query int border.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$lte",i-1);
   bson_finish(&q);
   TEST_ASSERT("-ve lte query not less than.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$lte",i+1);
   bson_finish(&q);
   TEST_ASSERT("+ve lte query match.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_double(&q,"$lte",d);
   bson_finish(&q);
   TEST_ASSERT("-ve lte query match double.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$gte",i);
   bson_finish(&q);
   TEST_ASSERT("+ve gte query border int .",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$gte",i-1);
   bson_finish(&q);
   TEST_ASSERT("+ve gte greater than int.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$gte",i+1);
   bson_finish(&q);
   TEST_ASSERT("-ve gte not greater than int.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_double(&q,"$gte",i);
   bson_finish(&q);
   TEST_ASSERT("+ve gte query border double .",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_double(&q,"$gte",i-1);
   bson_finish(&q);
   TEST_ASSERT("+ve gte greater than double.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_double(&q,"$gte",i+1);
   bson_finish(&q);
   TEST_ASSERT("-ve gte not greater than double.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_long(&q,"$gte",i);
   bson_finish(&q);
   TEST_ASSERT("+ve gte query border long .",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_long(&q,"$gte",i-1);
   bson_finish(&q);
   TEST_ASSERT("+ve gte greater than long.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_long(&q,"$gte",i+1);
   bson_finish(&q);
   TEST_ASSERT("-ve gte not greater than long.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_long(&q,"$gt",i-1);
   bson_append_long(&q,"$lt",i+1);
   bson_finish(&q);
   TEST_ASSERT("+ve range values long.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_long(&q,"$gte",i-1);
   bson_append_long(&q,"$lte",i+1);
   bson_finish(&q);
   TEST_ASSERT("+ve range values gte lte long.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$gt",i-1);
   bson_append_int(&q,"$lt",i+1);
   bson_finish(&q);
   TEST_ASSERT("+ve range values int.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$gte",i-1);
   bson_append_int(&q,"$lte",i+1);
   bson_finish(&q);
   TEST_ASSERT("+ve range values gte lte int.",QueryMatchBson(&q,&o));
   bson_destroy(&q);


   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_double(&q,"$gt",i-1);
   bson_append_double(&q,"$lt",i+1);
   bson_finish(&q);
   TEST_ASSERT("+ve range values double.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_double(&q,"$gte",i-1);
   bson_append_double(&q,"$lte",i+1);
   bson_finish(&q);
   TEST_ASSERT("+ve range values gte lte double.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   return 0;
}

void BsonUnfinish(bson *b,BinaryStr* s) {
   b->cur = b->data + s->len-1;
   b->finished = 0;
   b->stackPos = 0;
   b->err = 0;
   b->errstr = NULL;
   b->dataSize = s->len;
}

int TestArrayQuery() {
   bson o;
   bson q;
   bson_init(&o);
   bson_append_start_array(&o,"i");
   bson_append_int(&o,"",100);
   bson_append_int(&o,"",200);
   bson_append_int(&o,"",300);
   bson_append_finish_object(&o);
   bson_finish(&o);
   bson_print(&o);

   bson_init(&q);
   bson_append_start_array(&q,"i");
   bson_append_int(&q,"",100);
   bson_append_int(&q,"",200);
   bson_append_int(&q,"",300);
   bson_append_finish_object(&q);
   bson_finish(&q); 
   TEST_ASSERT("+ve Full array comparison match.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_array(&q,"i");
   bson_append_int(&q,"",100);
   bson_append_int(&q,"",200);
   bson_append_finish_object(&q);
   bson_finish(&q); 
   TEST_ASSERT("-ve Full array comparison no match.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_int(&q,"i",100);
   bson_finish(&q); 
   TEST_ASSERT("+ve First array element match.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_int(&q,"i",500);
   bson_finish(&q); 
   TEST_ASSERT("+ve First array element match.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$lt",500);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $lt for array match.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$lt",100);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $lt for array no match.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
    
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$gt",500);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $gt for array no match.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$gt",100);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $gt for array match.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$gte",500);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $gte for array no match.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$gte",100);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $gte for array match.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$gte",300);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $gte for array match border.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$lte",50);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $lte for array no match.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$lte",500);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $lte for array match.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$lte",100);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $lte for array match border.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   return 0;
}

int TestNinNeOps() {
   bson o;
   bson q;
   bson_init(&o);
   bson_append_int(&o,"i",100);
   bson_append_double(&o,"d",150);
   bson_append_string(&o,"s","abcd");
   bson_finish(&o);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_start_array(&q,"$nin");
   bson_append_int(&q,"",5);
   bson_append_int(&q,"",10);
   bson_append_int(&q,"",150);
   bson_append_finish_object(&q);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $nin int match with int values.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_start_array(&q,"$nin");
   bson_append_int(&q,"",5);
   bson_append_int(&q,"",10);
   bson_append_int(&q,"",100);
   bson_append_finish_object(&q);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $nin int no match with int values.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
      
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_start_array(&q,"$nin");
   bson_append_long(&q,"",5);
   bson_append_long(&q,"",10);
   bson_append_long(&q,"",150);
   bson_append_finish_object(&q);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $nin int match with long values.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_start_array(&q,"$nin");
   bson_append_long(&q,"",5);
   bson_append_long(&q,"",10);
   bson_append_long(&q,"",100);
   bson_append_finish_object(&q);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $nin int no match with long values.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_start_array(&q,"$nin");
   bson_append_double(&q,"",5);
   bson_append_double(&q,"",10);
   bson_append_double(&q,"",150);
   bson_append_finish_object(&q);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $nin int match with double values.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_start_array(&q,"$nin");
   bson_append_double(&q,"",5);
   bson_append_double(&q,"",10);
   bson_append_double(&q,"",100);
   bson_append_finish_object(&q);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $nin int no match with double values.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"s");
   bson_append_start_array(&q,"$nin");
   bson_append_double(&q,"",5);
   bson_append_double(&q,"",10);
   bson_append_double(&q,"",100);
   bson_append_finish_object(&q);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $nin string match with double values.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"s");
   bson_append_start_array(&q,"$nin");
   bson_append_double(&q,"",5);
   bson_append_double(&q,"",10);
   bson_append_string(&q,"","abcd");
   bson_append_finish_object(&q);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $nin string no match with double values.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"s");
   bson_append_start_array(&q,"$nin");
   bson_append_string(&q,"","bcdefgh");
   bson_append_string(&q,"","cdeef");
   bson_append_string(&q,"","abcdefh");
   bson_append_finish_object(&q);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $nin string match with string values.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"s");
   bson_append_start_array(&q,"$nin");
   bson_append_string(&q,"","bcdefgh");
   bson_append_string(&q,"","cdeef");
   bson_append_string(&q,"","abcd");
   bson_append_finish_object(&q);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $nin string match with string values.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   return 0;
}

int TestExists() {
   bson o;
   bson q;
   bson_init(&o);
   bson_append_int(&o,"i",100);
   bson_append_string(&o,"s","abcd");
   bson_append_start_object(&o,"so");
   bson_append_int(&o,"i",100);
   bson_append_finish_object(&o);
   bson_finish(&o);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_bool(&q,"$exists",1);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $exists int value.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_bool(&q,"$exists",0);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $exists int value.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"a");
   bson_append_bool(&q,"$exists",0);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $exists non-existing value.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   //bson_append_start_object(&q,"so");
   bson_append_start_object(&q,"so.i");
   bson_append_bool(&q,"$exists",1);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $exists sub object value.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"so.i");
   bson_append_bool(&q,"$exists",0);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $exists sub object value not true.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"so.a");
   bson_append_bool(&q,"$exists",0);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $exists sub object non-existing value.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   return 0;
}

int TestNeOp() {
   bson o;
   bson q;

   bson_init(&o); 
   bson_append_int(&o,"i",100);
   bson_append_string(&o,"s","ABCD");
   bson_finish(&o);
   
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$ne",500);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $ne int value not equal .",QueryMatchBson(&q,&o));


   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_int(&q,"$ne",100);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $ne int value equal .",!QueryMatchBson(&q,&o));


   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_string(&q,"$ne","ABCD");
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $ne string value equal .",!QueryMatchBson(&q,&o));

   bson_init(&q);
   bson_append_start_object(&q,"s");
   bson_append_string(&q,"$ne","ABCD");
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $ne string value not equal .",!QueryMatchBson(&q,&o));

   bson_init(&q);
   bson_append_start_object(&q,"s");
   bson_append_string(&q,"$ne","ABC");
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $ne string value not equal .",QueryMatchBson(&q,&o));
   return 0;
}

int TestInOp() {
   bson o;
   bson q;
   bson_init(&o);
   bson_append_int(&o,"i",100);
   bson_append_double(&o,"d",150);
   bson_append_string(&o,"s","abcd");
   bson_finish(&o);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_start_array(&q,"$in");
   bson_append_int(&q,"",5);
   bson_append_int(&q,"",10);
   bson_append_int(&q,"",150);
   bson_append_finish_object(&q);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $in int no match with int values.", !QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_start_array(&q,"$in");
   bson_append_int(&q,"",5);
   bson_append_int(&q,"",10);
   bson_append_int(&q,"",100);
   bson_append_finish_object(&q);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $in int match with int values.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
      
   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_start_array(&q,"$in");
   bson_append_long(&q,"",5);
   bson_append_long(&q,"",10);
   bson_append_long(&q,"",150);
   bson_append_finish_object(&q);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $in int no match with long values.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_start_array(&q,"$in");
   bson_append_long(&q,"",5);
   bson_append_long(&q,"",10);
   bson_append_long(&q,"",100);
   bson_append_finish_object(&q);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $in int match with long values.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_start_array(&q,"$in");
   bson_append_double(&q,"",5);
   bson_append_double(&q,"",10);
   bson_append_double(&q,"",150);
   bson_append_finish_object(&q);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $in int match with double values.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_start_array(&q,"$in");
   bson_append_double(&q,"",5);
   bson_append_double(&q,"",10);
   bson_append_double(&q,"",100);
   bson_append_finish_object(&q);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $in int match with double values.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"s");
   bson_append_start_array(&q,"$in");
   bson_append_double(&q,"",5);
   bson_append_double(&q,"",10);
   bson_append_double(&q,"",100);
   bson_append_finish_object(&q);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $in string no match with double values.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"s");
   bson_append_start_array(&q,"$in");
   bson_append_double(&q,"",5);
   bson_append_double(&q,"",10);
   bson_append_string(&q,"","abcd");
   bson_append_finish_object(&q);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $in string match with double values.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"s");
   bson_append_start_array(&q,"$in");
   bson_append_string(&q,"","bcdefgh");
   bson_append_string(&q,"","cdeef");
   bson_append_string(&q,"","abcdefh");
   bson_append_finish_object(&q);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $in string no match with string values.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"s");
   bson_append_start_array(&q,"$in");
   bson_append_string(&q,"","bcdefgh");
   bson_append_string(&q,"","cdeef");
   bson_append_string(&q,"","abcd");
   bson_append_finish_object(&q);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $in string match with string values.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   return 0;
}

int TestAllOp() {
   bson o;
   bson q;

   bson_init(&o);
   bson_append_start_array(&o, "i");
   bson_append_int(&o,"",100);
   bson_append_int(&o,"",200);
   bson_append_int(&o,"",300);
   bson_append_string(&o,"","abc");
   bson_append_finish_object(&o);
   bson_append_start_array(&o, "s");
   bson_append_string(&o,"","abc");
   bson_append_string(&o,"","bcd");
   bson_append_string(&o,"","cde");
   bson_append_int(&o,"",100);
   bson_append_finish_object(&o);
   bson_finish(&o);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_start_array(&q,"$all");
   bson_append_int(&q,"",100);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $all int match one element.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_start_array(&q,"$all");
   bson_append_int(&q,"",100);
   bson_append_int(&q,"",200);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $all int match 2 element.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"i");
   bson_append_start_array(&q,"$all");
   bson_append_int(&q,"",100);
   bson_append_int(&q,"",200);
   bson_append_int(&q,"",500);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $all int partial match element.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"s");
   bson_append_start_array(&q,"$all");
   bson_append_string(&q,"","abc");
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $all string match one element.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"s");
   bson_append_start_array(&q,"$all");
   bson_append_string(&q,"","abc");
   bson_append_string(&q,"","bcd");
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $all string match 2 elements.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"s");
   bson_append_start_array(&q,"$all");
   bson_append_string(&q,"","abc");
   bson_append_string(&q,"","bcd");
   bson_append_string(&q,"","bcdef");
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve $all string partial match  elements.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"s");
   bson_append_start_array(&q,"$all");
   bson_append_string(&q,"","abc");
   bson_append_string(&q,"","bcd");
   bson_append_string(&q,"","cde");
   bson_append_int(&q,"",100);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve $all int and string elements.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   return 0;
}

int TestOidQuery(){
   bson o;
   bson q;
   bson_oid_t oid1,oid2,oid3;

   bson_oid_gen(&oid1);
   bson_oid_gen(&oid2);
   bson_oid_gen(&oid3);
   bson_init(&o);
   bson_append_oid(&o,"id",&oid1);
   bson_finish(&o);
   
   bson_init(&q);
   bson_append_oid(&q,"id",&oid1);
   bson_finish(&q);
   TEST_ASSERT("+ve oid match.",QueryMatchBson(&q,&o));
   bson_destroy(&q);
   
   bson_init(&q);
   bson_append_oid(&q,"id",&oid2);
   bson_finish(&q);
   TEST_ASSERT("-ve oid no match.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   
   bson_init(&q);
   oid1.bytes[0]++;
   bson_append_start_object(&q,"id");
   bson_append_oid(&q,"$lt",&oid1);
   bson_append_finish_object(&q);
   bson_finish(&q);
   bson_print(&o);
   bson_print(&q);
   TEST_ASSERT("+ve oid lt query match.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"id");
   bson_append_oid(&q,"$lte",&oid1);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve oid lte query match.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   oid1.bytes[0]--;
   bson_append_start_object(&q,"id");
   bson_append_oid(&q,"$lte",&oid1);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve oid lte query match border.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   oid1.bytes[0]--;
   bson_append_start_object(&q,"id");
   bson_append_oid(&q,"$lte",&oid1);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve oid lte query  no match .",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"id");
   bson_append_oid(&q,"$lte",&oid1);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve oid lte query  no match .",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"id");
   bson_append_oid(&q,"$lt",&oid1);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve oid lt query  no match .",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"id");
   bson_append_oid(&q,"$gt",&oid1);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve oid gt query match.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   bson_append_start_object(&q,"id");
   bson_append_oid(&q,"$gte",&oid1);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve oid gte query match.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   oid1.bytes[0]++;
   bson_append_start_object(&q,"id");
   bson_append_oid(&q,"$gte",&oid1);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("+ve oid gte query match.",QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   oid1.bytes[0]++;
   bson_append_start_object(&q,"id");
   bson_append_oid(&q,"$gte",&oid1);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve oid gte query no match.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);

   bson_init(&q);
   oid1.bytes[0]++;
   bson_append_start_object(&q,"id");
   bson_append_oid(&q,"$gt",&oid1);
   bson_append_finish_object(&q);
   bson_finish(&q);
   TEST_ASSERT("-ve oid gt query no match.",!QueryMatchBson(&q,&o));
   bson_destroy(&q);
   return 0;
}

TestDesc tests[] ={
                   {"StringQueries","Queries related to string.",TestStringQuery},
                   {"NumberQueries","Queries related to numbers int/long/double.",TestIntQuery},
                   {"ArrayQueries","Queries related to bson array.",TestArrayQuery},
                   {"NinQueries","Queries with $nin operator.",TestNinNeOps},
                   {"NeQueries","Queries with $ne operator.",TestNeOp},
                   {"InQueries","Queries with $in operator.",TestInOp},
                   {"AllQueries","Queries with $in operator.",TestAllOp},
                   {"ExistsQuery","Queries with $exists.",TestExists},
                   {"OidQueries","Queries with oid object.",TestOidQuery},
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

int main(int argc, char** argv){
   InitQueryModule();
   int i = -1;
   if (argc > 1) {
      i = atoi(argv[1]);
   }

   RunTests(i);

   return 0;
}
