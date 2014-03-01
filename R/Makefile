
PROJECT_VERSION ?= 99.70
R_VERSION = 3.0
REPO_PATH_WIN = bin/windows/contrib/$(R_VERSION)
REPO_PATH_MAC = bin/macosx/contrib/$(R_VERSION)
TMP_BUILD_DIR = tmp_build
ifneq ($(OS),Windows_NT)
OS := $(shell uname)
UNAME_P := $(shell uname -p | sed 's/unknown/x86_64/')
else
# For windows NT the shell is odd.  Cygwin 'shell uname' returns CYGWIN_NT-6.1-WOW64
# which is not really what we want.  The normal CMD shell does something else.
# So don't ask for 'uname' if we already got Windows_NT
endif

H2O_R_SOURCE_FILE = h2o_$(PROJECT_VERSION).tar.gz
H2O_R_LINUX_FILE = h2o_$(PROJECT_VERSION)_R_$(UNAME_P)-pc-linux-gnu.tar.gz
H2O_R_MAC_FILE = h2o_$(PROJECT_VERSION).tgz
H2O_R_WINDOWS_FILE = h2o_$(PROJECT_VERSION).zip

ifeq ($(OS),Linux)
H2O_R_PKG_FILE = $(H2O_R_LINUX_FILE)
endif
ifeq ($(OS),Darwin)
H2O_R_PKG_FILE = $(H2O_R_MAC_FILE)
endif
ifeq ($(OS),Windows_NT)
H2O_R_PKG_FILE = $(H2O_R_WINDOWS_FILE)
endif

# Do a best-effort attempt to see if the R user has pdflatex installed
# and avoid running it if they don't.
#
# But use the '-' to ignore failures and continue the build even if it does fail.
PDFLATEX=$(shell which pdflatex)

build_rh2o:
	sed 's/SUBST_PROJECT_VERSION/$(PROJECT_VERSION)/' h2o-DESCRIPTION.template > h2o-package/DESCRIPTION
	sed 's/SUBST_PROJECT_VERSION/$(PROJECT_VERSION)/' h2o-package.template > h2o-package/man/h2o-package.Rd
ifeq ($(PDFLATEX),)
	@echo pdflatex not found, skipping pdf generation...
else
	-R CMD Rd2pdf --force --output="h2o-package/h2o_package.pdf" --title="Package 'h2o'" --no-index --no-preview h2o-package/man 1> /dev/null
endif
	rm -rf h2o-package/inst/java
	mkdir -p h2o-package/inst/java/tmp
	cp -f ../target/h2o.jar h2o-package/inst/java/tmp/h2o_full.jar
	(cd h2o-package/inst/java/tmp && jar xf h2o_full.jar)
	(cd h2o-package/inst/java/tmp && rm -fr hadoop/0.* hadoop/1.* hadoop/cdh[35]* hadoop/cdh4_yarn)
	(cd h2o-package/inst/java/tmp && rm -f h2o_full.jar)
	(cd h2o-package/inst/java/tmp && cp META-INF/MANIFEST.MF ..)
	(cd h2o-package/inst/java/tmp && rm -fr META-INF)
	(cd h2o-package/inst/java/tmp && jar cfm ../h2o.jar ../MANIFEST.MF *)
	rm -rf h2o-package/inst/java/tmp
	R CMD build h2o-package

	# Create file with description of all packages
	cp -p h2o-package/DESCRIPTION PACKAGES
	gzip -c PACKAGES > PACKAGES.gz

	# Add to repo in target directory
	mkdir -p ../target/R/src/contrib
	cp -p README.txt ../target/R
	mv $(H2O_R_SOURCE_FILE) ../target/R/src/contrib
	cp -p PACKAGES PACKAGES.gz ../target/R/src/contrib

	# Build binary for each OS
	rm -rf $(TMP_BUILD_DIR)
	mkdir -p $(TMP_BUILD_DIR)
	[ -x "`which gnutar 2>/dev/null`" ] || echo 'Note: gnutar not found; package install in R may fail in the next step'
	R CMD INSTALL -l $(TMP_BUILD_DIR) --build h2o-package || echo 'If you got an error like "Dependency foo is not available for package h2o" you need to install the required R package by running R and executing the R command: install.packages("foo")'

ifneq ($(OS),Windows_NT)
	tar zxf $(H2O_R_PKG_FILE)
	zip -q -r $(H2O_R_WINDOWS_FILE) h2o
	rm -rf h2o

	# Create repo subdirectory for MacOSX
	mkdir -p ../target/R/$(REPO_PATH_MAC)
	mv $(H2O_R_PKG_FILE) ../target/R/$(REPO_PATH_MAC)/$(H2O_R_MAC_FILE)
	cp -p PACKAGES PACKAGES.gz ../target/R/$(REPO_PATH_MAC)

	rm -rf ../target/R/bin/macosx/contrib/2.15 ../target/R/bin/macosx/contrib/2.14 ../target/R/bin/macosx/contrib/2.13
	cp -r ../target/R/$(REPO_PATH_MAC) ../target/R/bin/macosx/contrib/2.15
	cp -r ../target/R/$(REPO_PATH_MAC) ../target/R/bin/macosx/contrib/2.14 
	cp -r ../target/R/$(REPO_PATH_MAC) ../target/R/bin/macosx/contrib/2.13
endif

	# Create repo subdirectory for Windows
	mkdir -p ../target/R/$(REPO_PATH_WIN)
	mv $(H2O_R_WINDOWS_FILE) ../target/R/$(REPO_PATH_WIN)
	mv PACKAGES PACKAGES.gz ../target/R/$(REPO_PATH_WIN)

	rm -rf ../target/R/bin/windows/contrib/2.15 ../target/R/bin/windows/contrib/2.14 ../target/R/bin/windows/contrib/2.13
	cp -r ../target/R/$(REPO_PATH_WIN) ../target/R/bin/windows/contrib/2.15
	cp -r ../target/R/$(REPO_PATH_WIN) ../target/R/bin/windows/contrib/2.14 
	cp -r ../target/R/$(REPO_PATH_WIN) ../target/R/bin/windows/contrib/2.13

	R CMD REMOVE -l $(TMP_BUILD_DIR) h2o
	rm -rf $(TMP_BUILD_DIR)

check:
	find h2o-package -name .DS_Store | xargs rm -f
	R CMD CHECK h2o-package
	curl -v http://localhost:54321/Shutdown.json

clean:
	rm -f h2o-package/DESCRIPTION
	rm -f h2o-package/h2o_package.pdf
	rm -f h2o-package/man/h2o-package.Rd
	rm -fr h2o-package/inst/java
	rm -f h2o_*.tar.gz
	rm -f h2o_*.tgz
	rm -f h2o_*.zip
	rm -rf h2o
	rm -rf h2oRClient-package
