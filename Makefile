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
# h2o-scala/		Scala DSL + REPL
# h2o-docs/             docs.0xdata.com website content.
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
	$(MAKE) clean PROJECT_VERSION=$(PROJECT_VERSION)
	$(MAKE) build PROJECT_VERSION=$(PROJECT_VERSION)
	$(MAKE) docs-website PROJECT_VERSION=$(PROJECT_VERSION)
	@echo
	@echo Build completed successfully.

MILLIS_SINCE_EPOCH = $(shell date '+%s')

build:
	@echo
	@echo "PHASE: Building R inner package..."
	@echo
ifeq ($(BUILD_NUMBER),99999)
	$(MAKE) -C R build_inner PROJECT_VERSION=$(PROJECT_VERSION).$(MILLIS_SINCE_EPOCH)
else
	$(MAKE) -C R build_inner PROJECT_VERSION=$(PROJECT_VERSION)
endif
	@echo
	@echo "PHASE: Creating ${BUILD_VERSION_JAVA_FILE}..."
	@echo
	$(MAKE) build_version PROJECT_VERSION=$(PROJECT_VERSION)
	@echo
	@echo "PHASE: Building H2O..."
	@echo
	$(MAKE) build_h2o PROJECT_VERSION=$(PROJECT_VERSION)
	@echo
	@echo "PHASE: Building Shalala..."
	@echo
	$(MAKE) -C h2o-scala PROJECT_VERSION=$(PROJECT_VERSION)
	@echo
	@echo "PHASE: Building R outer package..."
	@echo
	$(MAKE) -C R build_outer PROJECT_VERSION=$(PROJECT_VERSION)
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
GA_FILE=lib/resources/h2o/js/ga

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
	cp ${GA_FILE}.release.js ${GA_FILE}.js

build_h2o:
	(export PROJECT_VERSION=$(PROJECT_VERSION); ./build.sh noclean doc)
	git checkout -- ${GA_FILE}.js

build_package:
	echo $(PROJECT_VERSION) > target/project_version
	rm -fr target/h2o-$(PROJECT_VERSION)
	mkdir target/h2o-$(PROJECT_VERSION)
	cp -rp target/R target/h2o-$(PROJECT_VERSION)
	cp -rp target/hadoop target/h2o-$(PROJECT_VERSION)
	cp -p target/h2o.jar target/h2o-$(PROJECT_VERSION)
	cp -p target/h2o-sources.jar target/h2o-$(PROJECT_VERSION)
	cp -p target/h2o-model.jar target/h2o-$(PROJECT_VERSION)
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

# Run Cookbook tests.
# Add to make test once they are reliable.
testcb:
	$(MAKE) -C h2o-cookbook build
	$(MAKE) -C h2o-cookbook test

TOPDIR:=$(CURDIR)
BUILD_WEBSITE_DIR=$(TOPDIR)/target/docs-website
SPHINXBUILD=$(shell which sphinx-build)
ifeq ($(SPHINXBUILD),)
docs-website: dw_announce
	@echo sphinx-build not found, skipping...

docs-website-clean:
else
docs-website: dw_announce dw_1 dw_2 dw_3 dw_4

docs-website-clean:
	rm -rf h2o-docs/source/developuser/DocGen
	$(MAKE) -C h2o-docs clean
endif

dw_announce:
	@echo
	@echo "PHASE: Building docs website..."
	@echo

RANDOM_NUMBER := $(shell echo $$$$)
PORT := $(shell expr "63600" "+" "(" $(RANDOM_NUMBER) "%" "100" ")")
TMPDIR := $(shell echo /tmp/tmp.h2o.docgen.$(PORT))
dw_1:
	rm -fr $(BUILD_WEBSITE_DIR)
	rm -fr h2o-docs/source/developuser/DocGen
	mkdir -p h2o-docs/source/developuser/DocGen
	cd h2o-docs/source/developuser/DocGen && java -Xmx1g -jar $(TOPDIR)/target/h2o.jar -runClass water.api.DocGen -port $(PORT) -name $(TMPDIR) -ice_root $(TMPDIR) 1> /dev/null
	rm -rf $(TMPDIR)

# If this fails, you might need to do the following:
#     $ (possibly sudo) easy_install pip
#     $ (possibly sudo) pip install sphinxcontrib-fulltoc
#
dw_2:
	(export PROJECT_VERSION=$(PROJECT_VERSION); $(MAKE) -C h2o-docs)

dw_3:
	mkdir $(BUILD_WEBSITE_DIR)
	cp -r h2o-docs/build/html/* $(BUILD_WEBSITE_DIR)
	mkdir -p $(BUILD_WEBSITE_DIR)/bits
	cp -p docs/0xdata_H2O_Algorithms.pdf $(BUILD_WEBSITE_DIR)/bits
	cp -rp target/javadoc $(BUILD_WEBSITE_DIR)/bits
	mkdir -p $(BUILD_WEBSITE_DIR)/bits/hadoop
	cp -p hadoop/README.txt $(BUILD_WEBSITE_DIR)/bits/hadoop
	cp -p docs/H2O_on_Hadoop_0xdata.pdf $(BUILD_WEBSITE_DIR)/bits/hadoop
	mkdir -p $(BUILD_WEBSITE_DIR)/bits/ec2
	cp -p ec2/README.txt $(BUILD_WEBSITE_DIR)/bits/ec2

# Note:  to get pdfunite on a mac, try:
#     $ brew install poppler
#
PDFLATEX=$(shell which pdflatex)
PDFUNITE=$(shell which pdfunite)
dw_4:
ifeq ($(PDFLATEX),)
	@echo pdflatex not found, skipping...
else
ifeq ($(PDFUNITE),)
	@echo pdfunite not found, skipping...
else
	pdfunite R/h2o-package/h2o_package.pdf R/h2oRClient-package/h2oRClient_package.pdf $(BUILD_WEBSITE_DIR)/bits/h2oRjoin.pdf
endif
endif

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
	@echo
	@echo "PHASE: Cleaning..."
	@echo
	rm -f $(BUILD_VERSION_JAVA_FILE)
	rm -fr target
	./build.sh clean
	$(MAKE) -C h2o-scala clean
	$(MAKE) -C hadoop clean
	$(MAKE) -C R clean
	$(MAKE) -C launcher clean
	$(MAKE) -C installer clean
	$(MAKE) docs-website-clean


.phony: default build test docs-website run clean
