##
# Generate lots of keys then remove them
##

source('./findNSourceUtils.R')

test <- function(conn) {
  arrests.hex = h2o.uploadFile(conn, locate("../../../smalldata/pca_test/USArrests.csv"), "arrests.hex")
  
  Log.info("Slicing column 1 of arrests 250 times")
  for(i in 1:250) {
    arrests.hex[,1]
    if( i %% 50 == 0 ) Log.info(paste("Finished ", paste(i, " slices of arrests.hex", sep = ""), sep = ""))
  }

  Log.info("Performing 1000 PCA's on the arrests data")
  for(i in 1:1000) {
    arrests.pca.h2o = h2o.prcomp(arrests.hex, standardize = FALSE)
    if( i %% 50 == 0 ) Log.info(paste("Finished ", paste(i, " PCAs of arrests.hex", sep = ""), sep = ""))
  }
  Log.info("Making a call to remove all")
  h2o.removeAll(conn)

  testEnd()
}

doTest("Many Keys Test: Removing", test)

