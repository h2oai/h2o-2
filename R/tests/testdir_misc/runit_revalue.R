##
# Generate lots of keys then remove them
##

setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test <- function(conn) {

  hex <- as.h2o(conn, iris)

  print(hex$Species)
  print(levels(hex$Species))

  revalue(hex$Species, c(setosa = "NEW SETOSA ENUM", virginica = "NEW VIRG ENUM", versicolor = "NEW VERSI ENUM"))

  
  print(hex$Species)
  print(levels(hex$Species))

  testEnd()
}

doTest("Many Keys Test: Removing", test)

