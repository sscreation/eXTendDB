CC      := gcc
CFLAGS  := `python-config --includes` -fPIC -g -I../ -O2
CLIBFLAGS := -fPIC

EXTENDDB_SRC := extenddbint_wrap.c sfile.c 
EXTENDDB_OBJS := $(patsubst %.c,%.o,$(EXTENDDB_SRC))
LDFLAGS  := -L../libs/ -lextenddb -lbson

all: _extenddb.so

%.o: %.c Makefile
	$(CC) -c -o $@ $< $(CFLAGS)

swig:
	swig -python -lstdint.i extenddbint.i

_extenddb.so: swig $(EXTENDDB_OBJS)
	gcc -shared -o _extenddbint.so $(EXTENDDB_OBJS) $(LDFLAGS)

clean:
	rm -f *.o
	rm -f _extenddbint.so
	rm -f *~
	rm -f *.pyc
