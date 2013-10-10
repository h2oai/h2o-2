
PROJECT_VERSION ?= 99.70

PDFLATEX=$(shell which pdflatex)

build:
	sed 's/SUBST_PROJECT_VERSION/$(PROJECT_VERSION)/' DESCRIPTION.template > h2oRClient-package/DESCRIPTION
	sed 's/SUBST_PROJECT_VERSION/$(PROJECT_VERSION)/' h2oRClient-package.template > h2oRClient-package/man/h2oRClient-package.Rd
ifeq ($(PDFLATEX),)
	@echo pdflatex not found, skipping pdf generation...
else
	R CMD Rd2pdf --force --output="h2oRClient-package/h2oRClient_package.pdf" --title="Package 'h2oRClient'" --no-index --no-preview h2oRClient-package/man 1> /dev/null
	R CMD Rd2pdf --force --output="h2oWrapper-package/h2o_package.pdf" --title="Package 'h2o'" --no-index --no-preview h2o-package/man 1> /dev/null
endif
	R CMD build h2oRClient-package
	echo h2oRClient_$(PROJECT_VERSION).tar.gz > info.txt
	openssl md5 h2oRClient_$(PROJECT_VERSION).tar.gz | sed 's/.*= //' >> info.txt
	# Create temporary directory for jarring later
	mkdir -p ../r_pack_tmp/resources/R
	cp -p info.txt ../r_pack_tmp/resources/R/info.txt
	cp -p h2oRClient_$(PROJECT_VERSION).tar.gz ../r_pack_tmp/resources/R
	
	# Add to resources directory
	mkdir -p ../lib/resources/R
	mv info.txt ../lib/resources/R/info.txt
	mv h2oRClient_$(PROJECT_VERSION).tar.gz ../lib/resources/R	

	R CMD build h2o-package
	mkdir -p ../target/R
	cp -p README.txt ../target/R
	mv h2o_1.0.2.tar.gz ../target/R

clean:
	rm -f h2oRClient-package/DESCRIPTION
	rm -f h2oRClient-package/h2oRClient_package.pdf
	rm -f h2oRClient-package/man/h2oRClient-package.Rd
	rm -f h2oRClient_*.tar.gz
	
	rm -f h2o-package/h2o_package.pdf
	rm -f h2o_*.tar.gz
