# The following two commands remove any previously installed H2O packages for R.

if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }

# Next, we download, install and initialize the H2O package for R.
install.packages("h2o", repos=(c("file:///Users/bersakain/Documents/workspace/h2o/target/R", getOption("repos"))))

