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
RELEASE_NAME = "master"

###########################################################################

default: nightly_build_stuff

nightly_build_stuff:
	@echo PROJECT_VERSION is $(PROJECT_VERSION)
	$(MAKE) clean PROJECT_VERSION=$(PROJECT_VERSION) 1> /dev/null
	@mkdir -p target/logs
	$(MAKE) build PROJECT_VERSION=$(PROJECT_VERSION)
	$(MAKE) docs-website PROJECT_VERSION=$(PROJECT_VERSION)
	@echo
	@echo Build completed successfully.

build_r:
	$(MAKE) build_rjar 1> target/logs/rjar_build.log
	$(MAKE) -C R PROJECT_VERSION=$(PROJECT_VERSION) BUILD_NUMBER=$(BUILD_NUMBER) 1> target/logs/r_build.log
	$(MAKE) build_rcran 1> target/logs/rcran_build.log 2> target/logs/rcran_build.err

build:
	@echo
	@echo "PHASE: Creating ${BUILD_VERSION_JAVA_FILE}..."
	@echo
	$(MAKE) build_version PROJECT_VERSION=$(PROJECT_VERSION) 1> target/logs/version_build.log

	@echo
	@echo "PHASE: Building UI..."
	@echo
	$(MAKE) -C client PROJECT_VERSION=$(PROJECT_VERSION) 1> target/logs/ui_build.log

	@echo
	@echo "PHASE: Building H2O..."
	@echo
	$(MAKE) build_h2o PROJECT_VERSION=$(PROJECT_VERSION)

	@echo
	@echo "PHASE: Building Hadoop driver..."
	@echo
	$(MAKE) -C hadoop build PROJECT_VERSION=$(PROJECT_VERSION) 1> target/logs/hadoop_build.log

	# @echo
	# @echo "PHASE: Building Shalala..."
	# @echo
	# $(MAKE) -C h2o-scala PROJECT_VERSION=$(PROJECT_VERSION)

	@echo
	@echo "PHASE: Building R package..."
	@echo
	$(MAKE) build_r

	@echo
	@echo "PHASE: Building zip package..."
	@echo
	$(MAKE) build_package PROJECT_VERSION=$(PROJECT_VERSION) 1> target/logs/package_build.log

	@echo
	@echo "PHASE: Building ZooKeeper jar..."
	@echo
	$(MAKE) -C h2o-zookeeper PROJECT_VERSION=$(PROJECT_VERSION) 1> target/logs/zookeeper_build.log 2> target/logs/zookeeper_build.err

BUILD_BRANCH=$(shell git branch | grep '*' | sed 's/* //')
BUILD_HASH=$(shell git log -1 --format="%H")
BUILD_DESCRIBE=$(shell git describe --always --dirty)
BUILD_ON=$(shell date)
BUILD_BY=$(shell whoami | sed 's/.*\\\\//')
BUILD_VERSION_JAVA_FILE = src/main/java/water/BuildVersion.java
GA_FILE=lib/resources/h2o/js/ga
ifneq ($(OS),Windows_NT)
OS := $(shell uname)
endif

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
ifneq ($(shell uname),Windows_NT)
	openssl md5 target/h2o.jar | sed 's/.*=[ ]*//' > target/h2o.jar.md5
else
	md5deep target/h2o.jar | cut -d'' -f1 > target/h2o.jar.md5
endif

# Strip out stuff from h2o.jar that isn't needed for R.
build_rjar:
	rm -fr target/Rjar
	mkdir -p target/Rjar/tmp
	cp target/h2o.jar target/Rjar/tmp/h2o_full.jar
	(cd target/Rjar/tmp && jar xf h2o_full.jar)
	(cd target/Rjar/tmp && rm -fr hadoop/0.* hadoop/1.* hadoop/cdh[35]* hadoop/cdh4_yarn hadoop/hdp*)
	(cd target/Rjar/tmp && rm -f h2o_full.jar)
	(cd target/Rjar/tmp && cp META-INF/MANIFEST.MF ..)
	(cd target/Rjar/tmp && rm -fr META-INF)
	(cd target/Rjar/tmp && jar cfm ../h2o.jar ../MANIFEST.MF *)
	rm -rf target/Rjar/tmp
	rm target/Rjar/MANIFEST.MF
ifneq ($(shell uname),Windows_NT)
	openssl md5 target/Rjar/h2o.jar | sed 's/.*=[ ]*//' > target/Rjar/h2o.jar.md5
else
	md5deep target/Rjar/h2o.jar | cut -d'' -f1 > target/Rjar/h2o.jar.md5
endif

# Build the file for submission to CRAN by stripping out h2o.jar.
H2O_R_SOURCE_FILE = h2o_$(PROJECT_VERSION).tar.gz
build_rcran:
	rm -fr target/Rcran
	mkdir target/Rcran
	cp target/R/src/contrib/$(H2O_R_SOURCE_FILE) target/Rcran/tmp.tar.gz
	cd target/Rcran && tar zxvf tmp.tar.gz
	rm -f target/Rcran/tmp.tar.gz
	rm -f target/Rcran/h2o/inst/java/h2o.jar
	cd target/Rcran/h2o && python ../../../scripts/dontrun_r_examples.py
	cd target/Rcran && tar zcvf $(H2O_R_SOURCE_FILE) h2o
	rm -fr target/Rcran/h2o

build_package:
	echo $(PROJECT_VERSION) > target/project_version
	rm -fr target/h2o-$(PROJECT_VERSION)
	mkdir target/h2o-$(PROJECT_VERSION)
	mkdir target/h2o-$(PROJECT_VERSION)/R
	cp -p target/R/src/contrib/h2o_$(PROJECT_VERSION).tar.gz target/h2o-$(PROJECT_VERSION)/R
	cp -p R/README.txt target/h2o-$(PROJECT_VERSION)/R
	cp -rp target/hadoop target/h2o-$(PROJECT_VERSION)
	cp -p target/h2o.jar target/h2o-$(PROJECT_VERSION)
	cp -p target/h2o-sources.jar target/h2o-$(PROJECT_VERSION)
	cp -p target/h2o-model.jar target/h2o-$(PROJECT_VERSION)
	cp -p packaging/README.txt target/h2o-$(PROJECT_VERSION)
	sed "s/SUBST_PROJECT_VERSION/$(PROJECT_VERSION)/g" packaging/index.html > target/index.html
	cp -p LICENSE.txt target/h2o-$(PROJECT_VERSION)
	mkdir target/h2o-$(PROJECT_VERSION)/ec2
	cp -p ec2/*.py ec2/*.sh ec2/README.txt target/h2o-$(PROJECT_VERSION)/ec2
	mkdir target/h2o-$(PROJECT_VERSION)/spark
	cp -p spark/* target/h2o-$(PROJECT_VERSION)/spark
	cp -rp tableau target/h2o-$(PROJECT_VERSION)
	cp -rp docs/TableauTutorial.docx target/h2o-$(PROJECT_VERSION)/tableau
	(cd target; zip -q -r h2o-$(PROJECT_VERSION).zip h2o-$(PROJECT_VERSION))
	rm -fr target/h2o-$(PROJECT_VERSION)
	rm -fr target/ci
	cp -rp ci target

test:
	./build.sh

# Run Cookbook tests.
# Add to make test once they are reliable.
testcb:
	$(MAKE) -C h2o-cookbook build
	$(MAKE) -C h2o-cookbook test

TOPDIR:=$(subst /cygdrive/c,c:,$(CURDIR))
BUILD_WEBSITE_DIR="$(TOPDIR)/target/docs-website"
SPHINXBUILD=$(shell which sphinx-build)
ifeq ($(SPHINXBUILD),)
docs-website: dw_announce
	@echo sphinx-build not found, skipping...

docs-website-clean:
else
docs-website: dw_announce dw_1 dw_2 dw_3

docs-website-clean:
	rm -rf h2o-docs/source/developuser/DocGen
	rm -rf h2o-docs/source/developuser/ScalaGen
	$(MAKE) -C h2o-docs clean
	$(MAKE) -C docs/uml clean
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
	cd h2o-docs/source/developuser/DocGen && java -Xmx1g -jar "$(TOPDIR)/target/h2o.jar" -runClass water.api.DocGen -port $(PORT) -name $(TMPDIR) -ice_root $(TMPDIR) 1> /dev/null
	rm -rf $(TMPDIR)

	rm -fr h2o-docs/source/developuser/ScalaGen
	mkdir -p h2o-docs/source/developuser/ScalaGen
	cp -p h2o-scala/README.rst h2o-docs/source/developuser/ScalaGen/README.rst

	$(MAKE) -C docs/uml
	rm -fr h2o-docs/source/developuser/PngGen
	mkdir -p h2o-docs/source/developuser/PngGen/pictures
	mkdir -p h2o-docs/source/developuser/PngGen/uml
	cp -p docs/pictures/*.png h2o-docs/source/developuser/PngGen/pictures
	cp -p docs/uml/*.png h2o-docs/source/developuser/PngGen/uml

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
	cp -p docs/sparkling_water_meetup.pdf $(BUILD_WEBSITE_DIR)/bits
	cp -p docs/h2o_datasheet.pdf $(BUILD_WEBSITE_DIR)/bits
	cp -p docs/H2ODeveloperCookbook.pdf $(BUILD_WEBSITE_DIR)/bits
	mkdir -p $(BUILD_WEBSITE_DIR)/bits/ec2
	cp -p ec2/README.txt $(BUILD_WEBSITE_DIR)/bits/ec2
	@if [ -f R/h2o_package.pdf ]; then \
	    echo cp -p R/h2o_package.pdf $(BUILD_WEBSITE_DIR)/bits/h2o_package.pdf; \
	    cp -p R/h2o_package.pdf $(BUILD_WEBSITE_DIR)/bits/h2o_package.pdf || exit 1; \
	fi
	sed -i -e "s/SUBST_RELEASE_NAME/$(RELEASE_NAME)/g; s/SUBST_PROJECT_VERSION/$(PROJECT_VERSION)/g; s/SUBST_BUILD_NUMBER/$(BUILD_NUMBER)/g" $(BUILD_WEBSITE_DIR)/deployment/hadoop_tutorial.html
	sed -i -e "s/SUBST_RELEASE_NAME/$(RELEASE_NAME)/g; s/SUBST_PROJECT_VERSION/$(PROJECT_VERSION)/g; s/SUBST_BUILD_NUMBER/$(BUILD_NUMBER)/g" $(BUILD_WEBSITE_DIR)/Ruser/Rinstall.html
	sed -i -e "s/SUBST_RELEASE_NAME/$(RELEASE_NAME)/g; s/SUBST_PROJECT_VERSION/$(PROJECT_VERSION)/g; s/SUBST_BUILD_NUMBER/$(BUILD_NUMBER)/g" $(BUILD_WEBSITE_DIR)/deployment/ec2_build_ami.html

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
	$(MAKE) -C client clean
	$(MAKE) -C h2o-scala clean
	$(MAKE) -C hadoop clean
	$(MAKE) -C h2o-zookeeper clean
	$(MAKE) -C R clean
	$(MAKE) -C launcher clean
	$(MAKE) -C installer clean
	$(MAKE) docs-website-clean


.PHONY: default build test docs-website run clean
