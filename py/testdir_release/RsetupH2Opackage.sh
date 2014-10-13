#!/bin/bash


echo "Remove and install local h2o package (presumed built with local make)"
#*******************************************************************************
echo "Checking that you have R, and the R version"
which R
R --version | egrep -i '(version|platform)'
echo ""

rm -f /tmp/libPaths.$USER.cmd
cat <<!  >> /tmp/libPaths.$USER.cmd
.libPaths()
myPackages = rownames(installed.packages())
if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
# this will only remove from the first library in .libPaths()
# may need permission to remove from other libraries
if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }
install.packages("h2o", repos=(c("file:~/h2o/target/R", getOption("repos"))))
!
R -f /tmp/libPaths.$USER.cmd

