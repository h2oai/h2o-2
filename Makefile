#
# Nightly build directions:
#
# make PROJECT_VERSION=correct.project.version
# make build_installer PROJECT_VERSION=correct.project.version
#
# PROJECT_VERSION must be strictly numerical.  E.g. 1.3.1853
#

PROJECT_VERSION ?= 99.90

default: build

build:
	(export PROJECT_VERSION=$(PROJECT_VERSION); ./build.sh doc)
	$(MAKE) -C hadoop build PROJECT_VERSION=$(PROJECT_VERSION)
	$(MAKE) -C R build PROJECT_VERSION=$(PROJECT_VERSION)
	$(MAKE) -C launcher build PROJECT_VERSION=$(PROJECT_VERSION)
	$(MAKE) package

package:
	rm -fr target/h2o-$(PROJECT_VERSION)
	mkdir target/h2o-$(PROJECT_VERSION)
	cp -rp target/R target/h2o-$(PROJECT_VERSION)
	cp -rp target/hadoop target/h2o-$(PROJECT_VERSION)
	cp -p target/h2o.jar target/h2o-$(PROJECT_VERSION)
	cp -p target/h2o-sources.jar target/h2o-$(PROJECT_VERSION)
	(cd target; tar cf h2o-$(PROJECT_VERSION).tar h2o-$(PROJECT_VERSION))
	rm -f target/h2o-$(PROJECT_VERSION).tar.gz
	gzip target/h2o-$(PROJECT_VERSION).tar
	rm -fr target/h2o-$(PROJECT_VERSION)

# This is run directly from the nightly build.  Most people won't have
# this software installed.
build_installer:
	$(MAKE) -C installer build PROJECT_VERSION=$(PROJECT_VERSION)
	rm -f target/h2o-*-windows-installer.exe.dmg

test:
	./build.sh

#
# Set appropriately for your data size to quickly try out H2O.
# For best results, the Java heap should be at least 4x data size.
# After starting H2O, point your browser to:
#     http://localhost:54321
#
JAVA_HEAP_SIZE=-Xmx2g
run:
	java $(JAVA_HEAP_SIZE) -jar target/h2o.jar

clean:
	./build.sh clean
	$(MAKE) -C hadoop clean
	$(MAKE) -C R clean
	$(MAKE) -C launcher clean
	$(MAKE) -C installer clean
