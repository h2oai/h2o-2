
PROJECT_VERSION ?= 99.70

build:
	sed 's/SUBST_PROJECT_VERSION/$(PROJECT_VERSION)/' DESCRIPTION.template > h2o-package/DESCRIPTION
	R CMD build h2o-package
	mkdir -p ../target/R
	mv h2o_$(PROJECT_VERSION).tar.gz ../target/R

clean:
	rm -f h2o-package/DESCRIPTION
	rm -f h2o_*.tar.gz

