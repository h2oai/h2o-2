
INSTALLER_OUTPUT_DIR = $(CURDIR)/../../installer_output
BITROCK_OPTS = --setvars project.outputDirectory=$(INSTALLER_OUTPUT_DIR)

build:
	/Applications/BitRock\ InstallBuilder\ Professional\ 8.5.2/bin/Builder.app/Contents/MacOS/installbuilder.sh build h2o.xml osx $(BITROCK_OPTS)
	/Applications/BitRock\ InstallBuilder\ Professional\ 8.5.2/bin/Builder.app/Contents/MacOS/installbuilder.sh build h2o.xml windows $(BITROCK_OPTS)


