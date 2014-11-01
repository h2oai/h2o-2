#### Testing h2o.predict on PCA models 
#######################################################################

setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

test.PCA.prostate.predict <- function(conn) {
  Log.info("Importing AustraliaCoast.csv data...\n")
  prostate.hex = h2o.uploadFile(conn, locate( "smalldata/logreg/prostate.csv",))
  prostate.sum = summary(prostate.hex)
  print(prostate.sum)
  
  Log.info('Changing GLEASON column to factor column:')
  prostate.hex$GLEASON =as.factor(prostate.hex$GLEASON)  
  
  Log.info("H2O PCA on standardized prostate data without scoring:\n")
  # Choose columns for running PCA
  cols = 3:9
  prostate.pca1 = h2o.prcomp(prostate.hex[,cols], standardize = TRUE)
  print(prostate.pca1)
  
  Log.info("Score on PCA model with prostate data:\n")
  prostate.pred1 = h2o.predict(object = prostate.pca1, newdata = prostate.hex, num_pc = 3)
  print(prostate.pred1)
  
  Log.info("H2O PCA on standardized prostate data with retx = T:\n")
  prostate.pca2 = h2o.prcomp(prostate.hex[,cols], standardize = TRUE, retx = TRUE, max_pc = 3)
  prostate.pred2 = prostate.pca2@model$x
  print(prostate.pca2)
  Log.info("Scores in the model object:\n")
  print(prostate.pred2)

  checkEquals(prostate.pca1@model$num_pc, prostate.pca2@model$num_pc)
  checkEquals(nrow(prostate.pred1), nrow(prostate.pred2))
  checkEquals(ncol(prostate.pred1), ncol(prostate.pred2))
  
  testEnd()
}

doTest("PCA: Australia Data", test.PCA.prostate.predict)

