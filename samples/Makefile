CC      := gcc
LDFLAGS := -L../ -L../libs
CFLAGS := -I../ -g

%.o: %.c Makefile
	$(CC) -c -o $@ $< $(CFLAGS) $(CLIBFLAGS)

all:xtdbinsert xtdbupdate xtdbremove

xtdbinsert:xtdbInsert.o
	gcc -o xtdbinsert xtdbInsert.o $(LDFLAGS) -lextenddb -lbson

xtdbupdate:xtdbUpdate.o
	gcc -o xtdbupdate xtdbUpdate.o $(LDFLAGS) -lextenddb -lbson

xtdbremove:


clean:
	rm -f *~
	rm -f *.o
	rm -f xtdbinsert
	rm -f xtdbupdate
	rm -f xtdbremove
