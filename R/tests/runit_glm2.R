source('./Utils/h2oR.R')


#------------------------------ Begin Tests ------------------------------#
conn <- new("H2OClient", ip=myIP, port=myPort)


glm2Benign <- function(conn) { 
  bhexFV <- h2o.importFile.FV(conn, "./smalldata/logreg/benign.csv", key="benignFV.hex")
  maxX <- 11
  Y <- 4
  X   <- 3:maxX
  X   <- X[ X != Y ] 
  
  
  logging("\nBuild the model\n")
  mFV <- h2o.glm.FV(y = Y, x = colnames(bhexFV)[X], data = bhexFV, family = "binomial", nfolds = 5, alpha = 0, lambda = 1e-5)
  
  
  logging("\nCheck that the columns used in the model are the ones we passed in.\n")
  
  cat("\n\n===================Columns passed in: ================\n")
  cat( colnames(bhexFV)[X], "\n")
  cat("======================================================\n")
  cat("\n\n===================Columns Used in Model: =========================\n")
  cat(mFV@model$x, "\n")
  cat("=======================================================================\n")
  
  expect_that(mFV@model$x, equals(colnames(bhexFV)[X]))
}

test_that("glm2 benign", glm2Benign(conn))
