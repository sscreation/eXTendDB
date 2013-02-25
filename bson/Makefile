.PHONY: all

CC      := gcc
CFLAGS  := -fPIC -g -O2

BSONSRC := bson.c encoding.c
BSONOBJS := $(patsubst %.c,%.o,$(BSONSRC))
BSONHDRS := bson.h encoding.h

%.o: %.c Makefile $(BSONHDRS)
	$(CC) -c -o $@ $< $(CFLAGS) $(CLIBFLAGS)

all:libbson.so install

libbson.a: $(BSONOBJS)
	ar rcs libbson.a bson.o encoding.o

libbson.so: $(BSONOBJS)
	gcc -shared -o libbson.so $(BSONOBJS)

install:libbson.a libbson.so
	cp bson.h ../include
	cp libbson.so ../libs

clean:
	rm -f *.o
	rm -f libbson.a
	rm -f libbson.so
