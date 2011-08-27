# Make file for C++ rgma-client-check

VPATH = ../src

CXXFLAGS = -fPIC -g
CPPFLAGS = -I../src
LDFLAGS = -L. -lssl -lpthread $(SHAREDLIB) -g
SHAREDLIB = ../src/libglite-rgma-cpp.so.$(version)

HEADERS = rgma/RGMAException.h  rgma/TimeInterval.h rgma/Storage.h rgma/TimeUnit.h \
rgma/PrimaryProducer.h rgma/SupportedQueries.h

.PHONY: clean

clean:
	$(RM) InsertTuple QueryTuple *.o

install: InsertTuple QueryTuple
	mkdir -p ${prefix}/libexec/rgma-client-check/cpp 
	cp -f InsertTuple QueryTuple *.py ${prefix}/libexec/rgma-client-check/cpp 
	tr < producer.py > ${prefix}/libexec/rgma-client-check/cpp/producer.py -d "\r"
	tr < consumer.py > ${prefix}/libexec/rgma-client-check/cpp/consumer.py -d "\r"
	chmod 755 ${prefix}/libexec/rgma-client-check/cpp/*

InsertTuple.o: InsertTuple.cpp $(HEADERS)
InsertTuple: InsertTuple.o ../src/libglite-rgma-cpp.a
	$(CXX) $(LDFLAGS) InsertTuple.o -o InsertTuple

QueryTuple.o: QueryTuple.cpp $(HEADERS)
QueryTuple: QueryTuple.o ../src/libglite-rgma-cpp.a
	$(CXX)  $(LDFLAGS) QueryTuple.o -o QueryTuple
