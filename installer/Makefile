
UNAME = $(shell uname)

ifeq ($(UNAME),Darwin)
    INSTALLBUILDER ?= /Applications/BitRock\ InstallBuilder\ Enterprise\ 8.6.0/bin/Builder.app/Contents/MacOS/installbuilder.sh
endif
ifeq ($(UNAME),Linux)
    INSTALLBUILDER ?= /var/lib/jenkins/0xdata/frozen/installbuilder-8.6.0/bin/builder
endif

WC := $(wildcard $(INSTALLBUILDER))
ifeq ($(WC),)
build: helpful_message
else
build: really_build
endif

INSTALLER_OUTPUT_DIR = $(CURDIR)/../target
PROJECT_VERSION ?= 99.60
BITROCK_OPTS = --setvars project.outputDirectory=$(INSTALLER_OUTPUT_DIR) project.version=$(PROJECT_VERSION)

really_build: build_mac build_win

build_mac:
	$(INSTALLBUILDER) build h2o.xml osx $(BITROCK_OPTS)
ifeq ($(UNAME),Linux)
	(cd $(INSTALLER_OUTPUT_DIR); sudo ../installer/make_dmg.sh h2o-$(PROJECT_VERSION)-osx-installer.app h2o-$(PROJECT_VERSION)-osx-installer.dmg h2o-$(PROJECT_VERSION))
endif
	rm -fr $(INSTALLER_OUTPUT_DIR)/h2o-$(PROJECT_VERSION)-osx-installer.app

build_win:
	$(INSTALLBUILDER) build h2o.xml windows $(BITROCK_OPTS)
	rm -f $(INSTALLER_OUTPUT_DIR)/h2o-*-windows-installer.exe.dmg

clean:

helpful_message:
	@echo InstallBuilder not found, skipping creation of windows and mac installer packages.

