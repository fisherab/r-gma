# Make file for api-c library

SOURCES = $(wildcard *.c)

include $(SOURCES:.c=.d)

OBJECTS = $(SOURCES:.c=.o)

%.d: %.c
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$
           
STATIC_LIB = libglite-rgma-c.a
SHARED_LIB = libglite-rgma-c.so.$(version)
SHARED_LIB_SONAME = libglite-rgma-c.so.$(majorVersion)

CFLAGS = -Wall -fexceptions -fPIC -g -I. $(COMPATIBILITY) -DRGMA_WITH_SSL -DOPENSSL_NO_KRB5
#CFLAGS = -Wall -fexceptions -fPIC -O3 -I. $(COMPATIBILITY) -DRGMA_WITH_SSL -DOPENSSL_NO_KRB5

install: $(STATIC_LIB) $(SHARED_LIB) doc
	mkdir -p ${prefix}/lib 
	mkdir -p ${prefix}/include/rgma
	cp *.so* ${prefix}/lib
	cp *.a ${prefix}/lib 
	ln -nsf libglite-rgma-c.so.${version} ${prefix}/lib/libglite-rgma-c.so 
	ln -nsf libglite-rgma-c.so.${version} ${prefix}/lib/libglite-rgma-c.so.${majorVersion} 
	cp -r *.h ${prefix}/include/rgma
	mkdir -p ${prefix}/share/doc/rgma-api-c
	cp -r autogen/html index.html ../LICENSE ../RELEASE-NOTES ${prefix}/share/doc/rgma-api-c
		
doc: autogen/html 

$(STATIC_LIB): $(OBJECTS)
	$(AR) -sr $(STATIC_LIB) $(OBJECTS) 2>&1
	
autogen/html: rgma.h
	mkdir -p autogen
	doxygen Doxyfile

$(SHARED_LIB): $(OBJECTS)
	$(CC) -O -shared $(OBJECTS) -o $(SHARED_LIB) -Wl,-soname -Wl,$(SHARED_LIB_SONAME)

.PHONY: clean

clean:
	$(RM) $(OBJECTS) autogen
