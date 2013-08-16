
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
	mkdir -p ../target/R
	cp -p README.txt ../target/R
	mv h2o_$(PROJECT_VERSION).tar.gz ../target/R

clean:
	rm -f h2o-package/DESCRIPTION
	rm -f h2o-package/h2o_package.pdf
	rm -f h2o-package/man/h2o-package.Rd
	rm -f h2o_*.tar.gz

