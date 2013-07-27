
UNAME = $(shell uname)

ifeq ($(UNAME),Darwin)
    INSTALLBUILDER="/Applications/BitRock InstallBuilder Enterprise 8.6.0/bin/Builder.app/Contents/MacOS/installbuilder.sh"
endif
ifeq ($(UNAME),Linux)
endif
ifndef INSTALLBUILDER
    $(error Cannot find installbuilder)
endif

INSTALLER_OUTPUT_DIR = $(CURDIR)/../target
PROJECT_VERSION ?= 99.60
BITROCK_OPTS = --setvars project.outputDirectory=$(INSTALLER_OUTPUT_DIR) project.version=$(PROJECT_VERSION)

build: build_mac build_win

build_mac:
	$(INSTALLBUILDER) build h2o.xml osx $(BITROCK_OPTS)

build_win:
	$(INSTALLBUILDER) build h2o.xml windows $(BITROCK_OPTS)

clean:

