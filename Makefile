#
# Standard developer directions:
#
# $ make
#

#
# Nightly build directions:
#
# Create ci/buildnumber.properties with the following entry:
#     BUILD_NUMBER=n
# 
# $ make
#


###########################################################################
# Figure out how to create PROJECT_VERSION variable.
###########################################################################

# 'ci' directory stands for Continuous Integration, which is where the
# version number stuff for a release is stored.
#
# ci/release.properties is a file that exists in git and is updated by
# hand on a per-branch basis.  This file contains the variables
#     BUILD_MAJOR_VERSION=x
#     BUILD_MINOR_VERSION=y
#     BUILD_INCREMENTAL_VERSION=z
#
# ci/buildnumber.properties is a file that gets created by jenkins.
# This file contains the variable
#     BUILD_NUMBER=n
#
# Simply cloning a git repository won't get you a real build number.
# Only jenkins manages them.

include ci/release.properties

# Use buildnumber.properties if it exists.  Otherwise, use 99999
BUILDNUMBER_PROPERTIES_FILE=$(wildcard ci/buildnumber.properties)
ifneq ($(BUILDNUMBER_PROPERTIES_FILE),)
include ci/buildnumber.properties
else
BUILD_NUMBER=99999
endif

PROJECT_VERSION ?= $(BUILD_MAJOR_VERSION).$(BUILD_MINOR_VERSION).$(BUILD_INCREMENTAL_VERSION).$(BUILD_NUMBER)

###########################################################################

default: nightly_build_stuff

nightly_build_stuff:
	@echo PROJECT_VERSION is $(PROJECT_VERSION)
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
	cp -p README.txt target/h2o-$(PROJECT_VERSION)
	cp -p LICENSE.txt target/h2o-$(PROJECT_VERSION)
	(cd target; zip -r h2o-$(PROJECT_VERSION).zip h2o-$(PROJECT_VERSION))
	rm -fr target/h2o-$(PROJECT_VERSION)

# Most people won't have the BitRock InstallBuilder software
# installed, which is OK.  It will harmlessly do nothing for that
# case.
build_installer:
	$(MAKE) -C installer build PROJECT_VERSION=$(PROJECT_VERSION)

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
