# Make file for rgma-client-check test programs.

VPATH = ../src

CFLAGS = -Wall -fexceptions -fPIC -O3
CPPFLAGS = -I../src
LDFLAGS = -L. -lssl ../src/libglite-rgma-c.a

.PHONY: clean

clean:
	$(RM) InsertTuple QueryTuple *.o

install: InsertTuple QueryTuple
	mkdir -p ${prefix}/libexec/rgma-client-check/c 
	cp -f InsertTuple QueryTuple ${prefix}/libexec/rgma-client-check/c 
	tr < producer.py > ${prefix}/libexec/rgma-client-check/c/producer.py -d "\r"
	tr < consumer.py > ${prefix}/libexec/rgma-client-check/c/consumer.py -d "\r"
	chmod 755 ${prefix}/libexec/rgma-client-check/c/*

InsertTuple: rgma.h InsertTuple.c libglite-rgma-c.a

QueryTuple: rgma.h QueryTuple.c libglite-rgma-c.a

