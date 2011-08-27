majorVersion := $(shell python -c "print '$(version)'.split('.')[0]")
export majorVersion

all:
	install

clean:
	$(MAKE) -C src clean
	$(MAKE) -C rgma-client-check clean

install:
	$(MAKE) -C src install
	$(MAKE) -C rgma-client-check install
