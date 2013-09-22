# Test principal components analysis in H2O
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

test.PCA.arrests <- function() {
  cat("\nImporting USArrests.csv data...\n")
  serverH2O = new("H2OClient", ip=myIP, port=myPort)
  arrests.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/pca_test/USArrests.csv")
  arrests.sum = summary(arrests.hex)
  print(arrests.sum)
  
  cat("\nH2O PCA on non-standardized USArrests:\n")
  arrests.h2o = h2o.prcomp(arrests.hex, standardize = FALSE)
  print(arrests.h2o)
  arrests.pca = prcomp(USArrests, center = FALSE, scale. = FALSE, retx = TRUE)
  checkPCAModel(arrests.h2o, arrests.pca)
  
  cat("\nH2O PCA on standardized USArrests:\n")
  arrests.h2o.std = h2o.prcomp(arrests.hex, standardize = TRUE)
  print(arrests.h2o.std)
  arrests.pca.std = prcomp(USArrests, center = TRUE, scale. = TRUE, retx = TRUE)
  checkPCAModel(arrests.h2o.std, arrests.pca.std)
}