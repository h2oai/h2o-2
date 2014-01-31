
PROJECT_VERSION ?= 99.70
R_VERSION = 3.0
REPO_PATH_WIN = bin/windows/contrib/$(R_VERSION)
REPO_PATH_MAC = bin/macosx/contrib/$(R_VERSION)
TMP_BUILD_DIR = $(CURDIR)/tmp_build
OS := $(shell uname)
H2O_RCLIENT_SOURCE_FILE = h2oRClient_$(PROJECT_VERSION).tar.gz
H2O_RCLIENT_LINUX_FILE = h2oRClient_$(PROJECT_VERSION)_R_x86_64-pc-linux-gnu.tar.gz
H2O_RCLIENT_MAC_FILE = h2oRClient_$(PROJECT_VERSION).tgz
H2O_RCLIENT_WINDOWS_FILE = h2oRClient_$(PROJECT_VERSION).zip
ifeq ($(OS),Linux)
H2O_RCLIENT_PKG_FILE = $(H2O_RCLIENT_LINUX_FILE)
endif
ifeq ($(OS),Darwin)
H2O_RCLIENT_PKG_FILE = $(H2O_RCLIENT_MAC_FILE)
endif



# Do a best-effort attempt to see if the R user has pdflatex installed
# and avoid running it if they don't.
#
# But use the '-' to ignore failures and continue the build even if it does fail.
PDFLATEX=$(shell which pdflatex)

build_outer:
	sed 's/SUBST_PROJECT_VERSION/$(PROJECT_VERSION)/' h2o-DESCRIPTION.template > h2o-package/DESCRIPTION
	sed 's/SUBST_PROJECT_VERSION/$(PROJECT_VERSION)/' h2o-package.template > h2o-package/man/h2o-package.Rd
ifeq ($(PDFLATEX),)
	@echo pdflatex not found, skipping pdf generation...
else
	-R CMD Rd2pdf --force --output="h2o-package/h2o_package.pdf" --title="Package 'h2o'" --no-index --no-preview h2o-package/man 1> /dev/null
endif
	mkdir -p h2o-package/inst/java/
	cp -f ../target/h2o.jar h2o-package/inst/java/h2o.jar
	R CMD build h2o-package
	mkdir -p ../target/R
	cp -p README.txt ../target/R
	mv h2o_$(PROJECT_VERSION).tar.gz ../target/R

build_inner:
	sed 's/SUBST_PROJECT_VERSION/$(PROJECT_VERSION)/' h2oRClient-DESCRIPTION.template > h2oRClient-package/DESCRIPTION
	sed 's/SUBST_PROJECT_VERSION/$(PROJECT_VERSION)/' h2oRClient-package.template > h2oRClient-package/man/h2oRClient-package.Rd
ifeq ($(PDFLATEX),)
	@echo pdflatex not found, skipping pdf generation...
else
	-R CMD Rd2pdf --force --output="h2oRClient-package/h2oRClient_package.pdf" --title="Package 'h2oRClient'" --no-index --no-preview h2oRClient-package/man 1> /dev/null
endif
	R CMD build h2oRClient-package
	echo h2oRClient_$(PROJECT_VERSION).tar.gz > info.txt
ifeq ($(OS),Windows_NT)
	md5deep h2oRClient_$(PROJECT_VERSION).tar.gz | cut -d' ' -f1 >> info.txt
else
	openssl md5 h2oRClient_$(PROJECT_VERSION).tar.gz | sed 's/.*= //' >> info.txt
endif

	# Create file with description of all packages
	cp -p h2oRClient-package/DESCRIPTION PACKAGES
	gzip -c PACKAGES > PACKAGES.gz

	# Create temporary directory for jarring later
	mkdir -p ../r_pack_tmp/resources/R/src/contrib
	cp -p info.txt ../r_pack_tmp/resources/R/info.txt
	cp -p $(H2O_RCLIENT_SOURCE_FILE) PACKAGES PACKAGES.gz ../r_pack_tmp/resources/R/src/contrib

	# Add to resources directory
	mkdir -p ../lib/resources/R/src/contrib
	mv info.txt ../lib/resources/R/info.txt
	mv $(H2O_RCLIENT_SOURCE_FILE) ../lib/resources/R/src/contrib
	cp -p PACKAGES PACKAGES.gz ../lib/resources/R/src/contrib

	# Build binary for each OS
	rm -rf $(TMP_BUILD_DIR)
	mkdir -p $(TMP_BUILD_DIR)
	R CMD INSTALL -l $(TMP_BUILD_DIR) --build h2oRClient-package

ifneq ($(OS),Windows_NT)
	rm -fr h2oRClient
	tar zxvf $(H2O_RCLIENT_PKG_FILE)
	zip -r $(H2O_RCLIENT_WINDOWS_FILE) h2oRClient

	mkdir -p ../r_pack_tmp/resources/R/$(REPO_PATH_MAC)
	mkdir -p ../lib/resources/R/$(REPO_PATH_MAC)
	cp -p $(H2O_RCLIENT_PKG_FILE) ../r_pack_tmp/resources/R/$(REPO_PATH_MAC)/$(H2O_RCLIENT_MAC_FILE)
	cp -p PACKAGES PACKAGES.gz ../r_pack_tmp/resources/R/$(REPO_PATH_MAC)
	mv $(H2O_RCLIENT_PKG_FILE) ../lib/resources/R/$(REPO_PATH_MAC)/$(H2O_RCLIENT_MAC_FILE)
	cp -p PACKAGES PACKAGES.gz ../lib/resources/R/$(REPO_PATH_MAC)
endif

	mkdir -p ../r_pack_tmp/resources/R/$(REPO_PATH_WIN)
	mkdir -p ../lib/resources/R/$(REPO_PATH_WIN)
	cp -p $(H2O_RCLIENT_WINDOWS_FILE) PACKAGES ../r_pack_tmp/resources/R/$(REPO_PATH_WIN)
	cp -p PACKAGES.gz ../r_pack_tmp/resources/R/$(REPO_PATH_WIN)
	mv $(H2O_RCLIENT_WINDOWS_FILE) PACKAGES ../lib/resources/R/$(REPO_PATH_WIN)
	mv PACKAGES.gz ../lib/resources/R/$(REPO_PATH_WIN)

	R CMD REMOVE -l $(TMP_BUILD_DIR) h2oRClient
	rm -rf $(TMP_BUILD_DIR)

clean:
	rm -f h2oRClient-package/DESCRIPTION
	rm -f h2oRClient-package/h2oRClient_package.pdf
	rm -f h2oRClient-package/man/h2oRClient-package.Rd
	rm -f h2oRClient_*.tar.gz
	rm -f h2oRClient_*.tgz
	rm -f h2oRClient_*.zip
	rm -f h2o-package/DESCRIPTION
	rm -f h2o-package/h2o_package.pdf
	rm -f h2o-package/man/h2o-package.Rd
	rm -f h2o-package/inst/java/h2o.jar
	rm -f h2o_*.tar.gz
	rm -rf ../lib/resources/R
