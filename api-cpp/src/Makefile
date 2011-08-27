# Make file for api-cpp library

SHARED_LIB = libglite-rgma-cpp.so.$(version)
SHARED_LIB_SONAME = libglite-rgma-cpp.so.$(majorVersion)
STATIC_LIB = libglite-rgma-cpp.a
CXXFLAGS = -g -Wall -fPIC
CPPFLAGS = -I.
CFLAGS = -fPIC
LDFLAGS = -g
HEADERS = $(wildcard rgma/*.h)

CPPSOURCES = $(wildcard *.cpp)

include $(CPPSOURCES:.cpp=.d)

OBJECTS = $(CPPSOURCES:.cpp=.o) rgma_parsexml.o

doc: autogen/html

autogen/html: Doxyfile $(HEADERS)
	mkdir -p autogen
	doxygen Doxyfile 2>&1

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

compile: $(OBJECTS)

install: $(OBJECTS) shared_lib static_lib doc
	mkdir -p ${prefix}/lib 
	mkdir -p ${prefix}/include/rgma
	cp *.so* ${prefix}/lib
	cp *.a ${prefix}/lib 
	ln -nsf libglite-rgma-cpp.so.${version} ${prefix}/lib/libglite-rgma-cpp.so 
	ln -nsf libglite-rgma-cpp.so.${version} ${prefix}/lib/libglite-rgma-cpp.so.${majorVersion} 
	cp rgma/*.h ${prefix}/include/rgma
	mkdir -p ${prefix}/share/doc/rgma-api-cpp
	cp -r autogen/html index.html ../LICENSE ../RELEASE-NOTES ${prefix}/share/doc/rgma-api-cpp

shared_lib: $(OBJECTS)
	$(CXX) $(LDFLAGS) -O -shared $(OBJECTS) -o $(SHARED_LIB) -Wl,-soname -Wl,$(SHARED_LIB_SONAME) -lcrypto -lssl

static_lib: $(OBJECTS)
	$(AR) -sr $(STATIC_LIB) $(OBJECTS) 2>&1

.PHONY: clean

clean:
	rm -rf autogen *.d *.o *.a *.so *.d.*
