export version = 1.2.3
export prefix = $(shell pwd)/build/prefix

export majorVersion := $(shell python -c "print '$(version)'.split('.')[0]")


all:

clean:
	$(MAKE) -C src clean
	$(MAKE) -C rgma-client-check clean

install:
	$(MAKE) -C src install
	$(MAKE) -C rgma-client-check install

