CC      := gcc
LDFLAGS := -L../ -L../libs
CFLAGS := -I../ -g

%.o: %.c Makefile
	$(CC) -c -o $@ $< $(CFLAGS) $(CLIBFLAGS)

all:testmdb testobjmod testquery

testmdb:testMdb.o ../libextenddb.a 
	gcc -o testmdb testMdb.o $(LDFLAGS) -lextenddb -lbson `pkg-config --libs tokyocabinet`

testobjmod:testObjMod.o ../libqueryobjmod.a
	gcc $(LDFLAGS) -o testobjmod testObjMod.o ../libqueryobjmod.a -lbson

testquery:testQuery.o ../libqueryobjmod.a
	gcc $(LDFLAGS) -o testquery testQuery.o ../libqueryobjmod.a -lbson

clean:
	rm -f *~
	rm -f *.o
	rm -f testmdb
	rm -f testobjmod
	rm -f testquery
