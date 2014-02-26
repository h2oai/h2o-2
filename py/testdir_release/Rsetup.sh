#!/bin/bash
which R
R --version
# don't always remove..other users may have stuff he doesn't want to re-install
if [[ $USER == "jenkins" ]]
then 
    echo "Rebuilding ~/.Renviron and ~/.Rprofile for $USER"
    # Set CRAN mirror to a default location
    rm -f ~/.Renviron
    rm -f ~/.Rprofile
    echo "options(repos = \"http://cran.stat.ucla.edu\")" > ~/.Rprofile
    echo "R_LIBS_USER=\"~/.Rlibrary\"" > ~/.Renviron
    rm -f -r ~/.Rlibrary
    mkdir -p ~/.Rlibrary
fi

# removing .Rlibrary should have removed h2oWrapper
# but maybe it was installed in another library (site library)
# make sure it's removed, so the install installs the new (latest) one
cat <<!  > /tmp/libPaths.cmd
.libPaths()
myPackages = rownames(installed.packages())
if("h2o" %in% myPackages) {
  # detach("package:h2o", unload=TRUE) 
  remove.packages("h2o")
}
if("h2oRClient" %in% myPackages) {
  # detach("package:h2oRClient", unload=TRUE) 
  remove.packages("h2oRClient")
}

# make the install conditional. Don't install if it's already there
usePackage <- function(p) {
    if (!is.element(p, installed.packages()[,1]))
        install.packages(p, dep = TRUE)
    require(p, character.only = TRUE)
}

# what packages did the h2o_master_test need?
usePackage("Rcurl")
usePackage("rjson")
usePackage("statmod")
usePackage("testthat")
usePackage("bitops")
usePackage("tools")
usePackage("LiblineaR")
usePackage("gdata")
usePackage("caTools")
usePackage("gplots")
usePackage("ROCR")
usePackage("digest")

# these came from source('../findNSourceUtils.R')
usePackage("glmnet")
usePackage("Matrix")
usePackage("survival")
usePackage("gbm")
# usePackage("splines")
usePackage("lattice")
# usePackage("parallel")
usePackage("RUnit")

# usePackage("h2o")
# usePackage("h2oRClient")
# usePackage("h2o", repos=(c("http://s3.amazonaws.com/h2o-release/h2o/master/1245/R", getOption("repos")))) 
# library(h2o)
!
# if Jenkins is running this, doing execute it..he'll execute it to logs for stdout/stderr
if [[ $USER != "jenkins" ]]
then
    R -f /tmp/libPaths.cmd
fi
