source('./Utils/h2oR.R', echo=FALSE, verbose=FALSE, print.eval=FALSE)

Log.info("------------------------------ Begin Tests ------------------------------")

glm2Benign <- function(conn) { 
  bhexFV <- h2o.importFile.FV(conn, "./smalldata/logreg/benign.csv", key="benignFV.hex")
  maxX <- 11
  Y <- 4
  X   <- 3:maxX
  X   <- X[ X != Y ] 
  
  Log.info("Build the model")
  mFV <- h2o.glm.FV(y = Y, x = colnames(bhexFV)[X], data = bhexFV, family = "binomial", nfolds = 5, alpha = 0, lambda = 1e-5)
  
  Log.info("Check that the columns used in the model are the ones we passed in.")
  
  Log.info("===================Columns passed in: ================")
  Log.info(paste(colnames(bhexFV)[X], "\n", sep=""))
  Log.info("======================================================")
  Log.info("===================Columns Used in Model: =========================")
  Log.info(paste(mFV@model$x, "\n", sep=""))
  Log.info("================================================================")
  
  #Check coeffs here
  expect_that(mFV@model$x, equals(colnames(bhexFV)[X]))

}

conn <- new("H2OClient", ip=myIP, port=myPort)
tryCatch(testResult <- test_that("glm2 benign", glm2Benign(conn)), 
            error = function(e) { FAIL(e)})

PASS()
