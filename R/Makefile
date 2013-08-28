
PROJECT_VERSION ?= 99.70

PDFLATEX=$(shell which pdflatex)

build:
	sed 's/SUBST_PROJECT_VERSION/$(PROJECT_VERSION)/' DESCRIPTION.template > h2o-package/DESCRIPTION
	sed 's/SUBST_PROJECT_VERSION/$(PROJECT_VERSION)/' h2o-package.template > h2o-package/man/h2o-package.Rd
ifeq ($(PDFLATEX),)
	@echo pdflatex not found, skipping pdf generation...
else
	R CMD Rd2pdf --force --output="h2o-package/h2o_package.pdf" --title="Package 'h2o'" --no-index --no-preview h2o-package/man 1> /dev/null
endif
	R CMD build h2o-package
	md5 -q h2o_$(PROJECT_VERSION).tar.gz > md5.txt

	# Create temporary directory for jarring later
	mkdir -p ../r_pack_tmp/resources/R
	cp -p md5.txt > ../r_pack_tmp/resources/R/md5.txt
	cp -p h2o_$(PROJECT_VERSION).tar.gz ../r_pack_tmp/resources/R
	
	# Add to resources directory
	mkdir -p ../lib/resources/R
	md5 -q md5.txt > ../lib/resources/R/md5.txt
	cp -p h2o_$(PROJECT_VERSION).tar.gz ../lib/resources/R	

	mkdir -p ../target/R
	cp -p README.txt ../target/R
	mv h2o_$(PROJECT_VERSION).tar.gz ../target/R

clean:
	rm -f h2o-package/DESCRIPTION
	rm -f h2o-package/h2o_package.pdf
	rm -f h2o-package/man/h2o-package.Rd
	rm -f h2o_*.tar.gz
