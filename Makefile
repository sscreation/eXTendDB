LDFLAGS := -L./ -L./lib `pkg-config --libs tokyocabinet`
CC      := gcc
CFLAGS  := -fPIC -g `pkg-config --cflags tokyocabinet`

CLIBFLAGS := -fPIC

MMDBLIBSRC := query.c extenddb.c tokyobe.c dbbe.c hash.c objmod.c extenddberror.c
MMDBLIBOBJS := $(patsubst %.c,%.o,$(MMDBLIBSRC))
MMDBLIBHDRS := query.h extenddb.h stringutil.h tokyobe.h dbbe.h bson.h extenddberror.h hash.h objmod.h
XDBLIBHDRS := query.h extenddb.h stringutil.h tokyobe.h dbbe.h bson.h extenddberror.h hash.h objmod.h
DEPS      := $(HDRS) Makefile 

.PHONY: all libbson.a
TGTDIR := /usr/local

ifdef INSTALLDIR 
	TGTDIR=$(INSTALLDIR)
endif

all: libbson.a libextenddb.a libextenddb.so test libqueryobjmod.a sample

%.o: %.c Makefile
	$(CC) -c -o $@ $< $(CFLAGS) $(CLIBFLAGS)

libextenddb.a: libs $(MMDBLIBOBJS) libbson.so
	ar rcs libextenddb.a $(MMDBLIBOBJS) 
	cp libextenddb.a ./libs

libs:
	mkdir libs

libextenddb.so: $(MMDBLIBOBJS) libs
	gcc -shared -o libextenddb.so $(MMDBLIBOBJS) $(LDFLAGS)
	cp libextenddb.so ./libs

testobjmod: objmod.o hash.o libbson.a testObjMod.o query.o extenddberror.o
	gcc -o testobjmod testObjMod.o objmod.o hash.o -lbson query.o extenddberror.o

libbson.so:
	$(MAKE) -C ./bson

libqueryobjmod.a:objmod.o hash.o libbson.a query.o extenddberror.o
	ar rcs libqueryobjmod.a objmod.o hash.o query.o extenddberror.o

install:libextenddb.so libextenddb.a	
	mkdir -p $(TGTDIR)/include/extenddb/
	cp $(XDBLIBHDRS)  $(TGTDIR)/include/extenddb/ 
	mkdir -p $(TGTDIR)/libs/
	cp ./libs/* $(TGTDIR)/libs/

test:libextenddb.so libextenddb.a libqueryobjmod.a libbson.so
	$(MAKE) -C tests

sample:libextenddb.so libextenddb.a libbson.so
	$(MAKE) -C samples


clean:
	rm -f *~
	rm -f *.o
	rm -f libextenddb.so
	rm -f libextenddb.a
	rm -f *.a
	rm -f *.so
	rm -f ./libs/*
	$(MAKE) -C tests clean
	$(MAKE) -C bson clean
	$(MAKE) -C samples clean

