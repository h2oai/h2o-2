#
# Nightly build directions:
#
# make PROJECT_VERSION=correct.project.version
#
# PROJECT_VERSION must be strictly numerical.  E.g. 1.3.1853
#

PROJECT_VERSION ?= 99.90

default: nightly_build_stuff

nightly_build_stuff:
	$(MAKE) build PROJECT_VERSION=$(PROJECT_VERSION)
	$(MAKE) build_installer PROJECT_VERSION=$(PROJECT_VERSION)

build:
	$(MAKE) build_h2o PROJECT_VERSION=$(PROJECT_VERSION)
	$(MAKE) -C hadoop build PROJECT_VERSION=$(PROJECT_VERSION)
	$(MAKE) -C R build PROJECT_VERSION=$(PROJECT_VERSION)
	$(MAKE) -C launcher build PROJECT_VERSION=$(PROJECT_VERSION)
	$(MAKE) package

build_h2o:
	(export PROJECT_VERSION=$(PROJECT_VERSION); ./build.sh doc)

package:
	rm -fr target/h2o-$(PROJECT_VERSION)
	mkdir target/h2o-$(PROJECT_VERSION)
	cp -rp target/R target/h2o-$(PROJECT_VERSION)
	cp -rp target/hadoop target/h2o-$(PROJECT_VERSION)
	cp -p target/h2o.jar target/h2o-$(PROJECT_VERSION)
	cp -p target/h2o-sources.jar target/h2o-$(PROJECT_VERSION)
	(cd target; zip -r h2o-$(PROJECT_VERSION).zip h2o-$(PROJECT_VERSION))
	rm -fr target/h2o-$(PROJECT_VERSION)

# Most people won't have the BitRock InstallBuilder software
# installed, which is OK.  It will harmlessly do nothing for that
# case.
build_installer:
	$(MAKE) -C installer build PROJECT_VERSION=$(PROJECT_VERSION)
	rm -fr target/h2o-$(PROJECT_VERSION)-osx-installer.app
	rm -f target/h2o-*-windows-installer.exe.dmg

test:
	(export PROJECT_VERSION=$(PROJECT_VERSION); ./build.sh)

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
