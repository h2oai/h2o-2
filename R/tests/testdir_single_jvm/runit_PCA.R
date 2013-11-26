source('../Utils/h2oR.R')

Log.info("======================== Begin Test ===========================\n")

checkPCAModel <- function(myPCA.h2o, myPCA.r, toleq = 1e-5) {
  checkEqualsNumeric(myPCA.h2o@model$sdev, myPCA.r$sdev)
  
  ncomp = length(colnames(myPCA.h2o@model$rotation))
  myPCA.h2o@model$rotation = apply(myPCA.h2o@model$rotation, 2, as.numeric)
  flipped = abs(myPCA.h2o@model$rotation[1,] + myPCA.r$rotation[1,]) <= toleq
  for(i in 1:ncomp) {
    if(flipped[i])
      checkEqualsNumeric(myPCA.h2o@model$rotation[,i], -myPCA.r$rotation[,i])
    else
      checkEqualsNumeric(myPCA.h2o@model$rotation[,i], myPCA.r$rotation[,i])
  }
}

test.PCA.arrests <- function(conn) {
  Log.info("Importing USArrests.csv data...\n")
  # arrests.hex = h2o.importURL(conn, "https..//raw.github.com/0xdata/h2o/master/smalldata/pca_test/USArrests.csv")
  # arrests.hex = h2o.importFile(conn, normalizePath("../../../smalldata/pca_test/USArrests.csv"))
  arrests.hex = h2o.uploadFile(conn, "../../../smalldata/pca_test/USArrests.csv", "arrests.hex")
  arrests.sum = summary(arrests.hex)
  print(arrests.sum)
  
  Log.info("H2O PCA on non-standardized USArrests:\n")
  arrests.pca.h2o = h2o.prcomp(arrests.hex, standardize = FALSE)
  print(arrests.pca.h2o)
  arrests.pca = prcomp(USArrests, center = FALSE, scale. = FALSE, retx = TRUE)
  checkPCAModel(arrests.pca.h2o, arrests.pca)
  
  Log.info("H2O PCA on standardized USArrests:\n")
  arrests.pca.h2o.std = h2o.prcomp(arrests.hex, standardize = TRUE)
  print(arrests.pca.h2o.std)
  arrests.pca.std = prcomp(USArrests, center = TRUE, scale. = TRUE, retx = TRUE)
  checkPCAModel(arrests.pca.h2o.std, arrests.pca.std)
}

test.PCA.australia <- function(conn) {
  Log.info("Importing AustraliaCoast.csv data...\n")
  australia.data = read.csv("../../../smalldata/pca_test/AustraliaCoast.csv", header = TRUE)
  # australia.hex = h2o.importFile(conn, normalizePath("../../../smalldata/pca_test/AustraliaCoast.csv"))
  australia.hex = h2o.uploadFile(conn, "../../../smalldata/pca_test/AustraliaCoast.csv")
  australia.sum = summary(australia.hex)
  print(australia.sum)
  
  Log.info("H2O PCA on non-standardized Australia coastline data:\n")
  australia.pca.h2o = h2o.prcomp(australia.hex, standardize = FALSE)
  print(australia.pca.h2o)
  australia.pca = prcomp(australia.data, center = FALSE, scale. = FALSE, retx = TRUE)
  checkPCAModel(australia.pca.h2o, australia.pca)
  
  Log.info("H2O PCA on standardized Australia coastline data:\n")
  australia.pca.h2o.std = h2o.prcomp(australia.hex, standardize = TRUE)
  print(australia.pca.h2o.std)
  australia.pca.std = prcomp(australia.data, center = TRUE, scale. = TRUE, retx = TRUE)
  checkPCAModel(australia.pca.h2o.std, australia.pca.std)
}

conn = new("H2OClient", ip=myIP, port=myPort)

tryCatch(test_that("PCA Test: USArrests", test.PCA.arrests(conn)), error = function(e) FAIL(e))
PASS()
tryCatch(test_that("PCA Test: Australia", test.PCA.australia(conn)), error = function(e) FAIL(e))
PASS()
