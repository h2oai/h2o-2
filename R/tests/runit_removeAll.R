##
# Generate lots of keys then remove them
##

source('./Utils/h2oR.R')

logging("\n======================== Begin Test ===========================\n")
conn = new("H2OClient", ip=myIP, port=myPort)

test <- function(serverH2O) {
  
 
  arrests.hex = h2o.uploadFile(serverH2O, "../../smalldata/pca_test/USArrests.csv", "arrests.hex")

  for(i in 1:250) {
    arrests.hex[,1]
  }

  for(i in 1:1000) {
    arrests.pca.h2o = h2o.prcomp(arrests.hex, standardize = FALSE)
  }
  h2o.removeAll(serverH2O)
}

test(conn)
