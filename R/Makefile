
PROJECT_VERSION ?= 99.70
R_VERSION = 3.0
REPO_PATH_WIN = bin/windows/contrib/$(R_VERSION)
REPO_PATH_MAC = bin/macosx/contrib/$(R_VERSION)

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
	# gzip -c PACKAGES > PACKAGES.gz

	# Create temporary directory for jarring later
	mkdir -p ../r_pack_tmp/resources/R/src/contrib
	cp -p info.txt ../r_pack_tmp/resources/R/info.txt
	cp -p h2oRClient_$(PROJECT_VERSION).tar.gz PACKAGES ../r_pack_tmp/resources/R/src/contrib
	# cp -p PACKAGES.gz ../r_pack_tmp/resources/R/src/contrib/PACKAGES.gz

	# Add to resources directory
	mkdir -p ../lib/resources/R/src/contrib
	mv info.txt ../lib/resources/R/info.txt
	mv h2oRClient_$(PROJECT_VERSION).tar.gz ../lib/resources/R/src/contrib
	cp -p PACKAGES ../lib/resources/R/src/contrib
	# cp -p PACKAGES.gz ../lib/resources/R/src/contrib/PACKAGES.gz

	# Build binary for each OS
ifeq ($(shell uname), Linux)
	R CMD INSTALL -l $(TMP) --build h2oRClient-package
else
	R CMD INSTALL --build h2oRClient-package
endif

ifneq ($(OS),Windows_NT)
	tar zxvf h2oRClient_$(PROJECT_VERSION).tgz
	zip -r h2oRClient_$(PROJECT_VERSION).zip h2oRClient

	mkdir -p ../r_pack_tmp/resources/R/$(REPO_PATH_MAC)
	mkdir -p ../lib/resources/R/$(REPO_PATH_MAC)
	cp -p h2oRClient_$(PROJECT_VERSION).tgz PACKAGES ../r_pack_tmp/resources/R/$(REPO_PATH_MAC)
	# cp -p PACKAGES.gz ../lib/resources/R/$(REPO_PATH_MAC)
	mv h2oRClient_$(PROJECT_VERSION).tgz ../lib/resources/R/$(REPO_PATH_MAC)
	cp -p PACKAGES ../lib/resources/R/$(REPO_PATH_MAC)/PACKAGES
	# cp -p PACKAGES.gz ../lib/resources/R/$(REPO_PATH_MAC)/PACKAGES.gz
endif

	mkdir -p ../r_pack_tmp/resources/R/$(REPO_PATH_WIN)
	mkdir -p ../lib/resources/R/$(REPO_PATH_WIN)
	cp -p h2oRClient_$(PROJECT_VERSION).zip PACKAGES ../r_pack_tmp/resources/R/$(REPO_PATH_WIN)
	# cp -p PACKAGES.gz ../r_pack_tmp/resources/R/$(REPO_PATH_WIN)
	mv h2oRClient_$(PROJECT_VERSION).zip PACKAGES ../lib/resources/R/$(REPO_PATH_WIN)
	# mv PACKAGES.gz ../lib/resources/R/$(REPO_PATH_WIN)

clean:
	rm -f h2oRClient-package/DESCRIPTION
	rm -f h2oRClient-package/h2oRClient_package.pdf
	rm -f h2oRClient-package/man/h2oRClient-package.Rd
	rm -f h2oRClient_*.tar.gz
	rm -f h2o-package/DESCRIPTION
	rm -f h2o-package/h2o_package.pdf
	rm -f h2o-package/man/h2o-package.Rd
	rm -f h2o-package/inst/java/h2o.jar
	rm -f h2o_*.tar.gz
	rm -rf ../lib/resources/R
