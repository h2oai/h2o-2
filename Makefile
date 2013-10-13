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
# Directories
###########################################################################

# R/			H2O support for the R language.  Includes tests.
# bench/		Benchmarking support.
# ci/			Support files for Jenkins (continuous integration).
# docs/			Individual documents for the project.
# ec2/			EC2 scripts for use by the public.
# experiments/		Experimental code built on top of H2O.
# hadoop/		Hadoop driver and mapper for H2O.
# installer/		BitRock InstallBuilder code for windows and mac.
# launcher/		Launcher packaged with the installer.
# lib/			Libraries used for compiling and running H2O.
# packaging/		Varous files used for building the H2O packages.
# py/			Python tests.
# selenium/		Web UI tests.
# smalldata/		"Small" (in size) datasets used for testing.
# src/			H2O source code.
# target/		Build output directory (Warning: gets wiped out!).

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
	@echo
	@echo "PHASE: Cleaning..."
	@echo
	$(MAKE) clean PROJECT_VERSION=$(PROJECT_VERSION)
	$(MAKE) build PROJECT_VERSION=$(PROJECT_VERSION)
	@echo
	@echo Build completed successfully.

build:
	@echo
	@echo "PHASE: Building R packages..."
	@echo
	$(MAKE) -C R build PROJECT_VERSION=$(PROJECT_VERSION)
	@echo
	@echo "PHASE: Creating ${BUILD_VERSION_JAVA_FILE}..."
	@echo
	$(MAKE) build_version PROJECT_VERSION=$(PROJECT_VERSION)
	@echo
	@echo "PHASE: Building H2O..."
	@echo
	$(MAKE) build_h2o PROJECT_VERSION=$(PROJECT_VERSION)
	@echo
	@echo "PHASE: Building hadoop driver..."
	@echo
	$(MAKE) -C hadoop build PROJECT_VERSION=$(PROJECT_VERSION)
	@echo
	@echo "PHASE: Building launcher..."
	@echo
	$(MAKE) -C launcher build PROJECT_VERSION=$(PROJECT_VERSION)
	@echo
	@echo "PHASE: Building zip package..."
	@echo
	$(MAKE) build_package PROJECT_VERSION=$(PROJECT_VERSION)
	@echo
	@echo "PHASE: Building Mac and Windows installer packages..."
	@echo
	$(MAKE) build_installer PROJECT_VERSION=$(PROJECT_VERSION)

BUILD_BRANCH=$(shell git branch | grep '*' | sed 's/* //')
BUILD_HASH=$(shell git log -1 --format="%H")
BUILD_DESCRIBE=$(shell git describe --always --dirty)
BUILD_ON=$(shell date)
BUILD_BY=$(shell whoami | sed 's/.*\\\\//')
BUILD_VERSION_JAVA_FILE = src/main/java/water/BuildVersion.java

build_version:
	@rm -f ${BUILD_VERSION_JAVA_FILE}
	@echo "package water;"                                                        >> ${BUILD_VERSION_JAVA_FILE}.tmp
	@echo "public class BuildVersion extends AbstractBuildVersion {"              >> ${BUILD_VERSION_JAVA_FILE}.tmp
	@echo "    public String branchName()     { return \"$(BUILD_BRANCH)\"; }"    >> ${BUILD_VERSION_JAVA_FILE}.tmp
	@echo "    public String lastCommitHash() { return \"$(BUILD_HASH)\"; }"      >> ${BUILD_VERSION_JAVA_FILE}.tmp
	@echo "    public String describe()       { return \"$(BUILD_DESCRIBE)\"; }"  >> ${BUILD_VERSION_JAVA_FILE}.tmp
	@echo "    public String projectVersion() { return \"$(PROJECT_VERSION)\"; }" >> ${BUILD_VERSION_JAVA_FILE}.tmp
	@echo "    public String compiledOn()     { return \"$(BUILD_ON)\"; }"        >> ${BUILD_VERSION_JAVA_FILE}.tmp
	@echo "    public String compiledBy()     { return \"$(BUILD_BY)\"; }"        >> ${BUILD_VERSION_JAVA_FILE}.tmp
	@echo "}"                                                                     >> ${BUILD_VERSION_JAVA_FILE}.tmp
	mv ${BUILD_VERSION_JAVA_FILE}.tmp ${BUILD_VERSION_JAVA_FILE}

build_h2o:
	(export PROJECT_VERSION=$(PROJECT_VERSION); ./build.sh noclean doc)

build_package:
	echo $(PROJECT_VERSION) > target/project_version
	rm -fr target/h2o-$(PROJECT_VERSION)
	mkdir target/h2o-$(PROJECT_VERSION)
	cp -rp target/R target/h2o-$(PROJECT_VERSION)
	cp -rp target/hadoop target/h2o-$(PROJECT_VERSION)
	cp -p target/h2o.jar target/h2o-$(PROJECT_VERSION)
	cp -p target/h2o-sources.jar target/h2o-$(PROJECT_VERSION)
	cp -p packaging/README.txt target/h2o-$(PROJECT_VERSION)
	sed "s/SUBST_PROJECT_VERSION/$(PROJECT_VERSION)/g" packaging/index.html > target/index.html
	cp -p LICENSE.txt target/h2o-$(PROJECT_VERSION)
	mkdir target/h2o-$(PROJECT_VERSION)/ec2
	cp -p ec2/*.py ec2/*.sh ec2/README.txt target/h2o-$(PROJECT_VERSION)/ec2
	(cd target; zip -r h2o-$(PROJECT_VERSION).zip h2o-$(PROJECT_VERSION))
	rm -fr target/h2o-$(PROJECT_VERSION)
	rm -fr target/ci
	cp -rp ci target

# Most people won't have the BitRock InstallBuilder software
# installed, which is OK.  It will harmlessly do nothing for that
# case.
build_installer:
	$(MAKE) -C installer build PROJECT_VERSION=$(PROJECT_VERSION)

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
	rm -f ${BUILD_VERSION_JAVA_FILE}
	rm -fr target
	./build.sh clean
	$(MAKE) -C hadoop clean
	$(MAKE) -C R clean
	$(MAKE) -C launcher clean
	$(MAKE) -C installer clean
