##
# Generate lots of keys then remove them
##

source('../Utils/h2oR.R')

Log.info("======================== Begin Test ===========================\n")

test <- function(conn) {
  arrests.hex = h2o.uploadFile(conn, "../../../smalldata/pca_test/USArrests.csv", "arrests.hex")
  
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
}

conn = new("H2OClient", ip=myIP, port=myPort)

tryCatch(test_that("many keys test", test(conn)),  error = function(e) FAIL(e))
PASS()
