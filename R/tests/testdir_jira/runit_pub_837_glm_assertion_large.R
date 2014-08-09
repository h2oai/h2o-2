##
# NOPASS TEST: The following bug is associated with JIRA PUB-837 
# 'GLM with Cross Validation: ArrayIndexOutOfBoundsException: 89'
# Testing glm cross validation performance with adult dataset 
##

setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')


test <- function(conn) {
  print("Reading in original adult data.")
  adult.train <-  h2o.uploadFile(conn, locate("smalldata/adult.gz"), key="adult.train")
  
  print("Make labels 1/0 for binomial glm")
  adult.train$label <- ifelse(adult.train$"C15"==">50K",1,0)
  
  print("Head of adult data: ")
  head(adult.train)
  print("Dimensions of adult data: ")
  dim(adult.train)
  
  print("Set variables to build models")
  myX = c(1:14)
  myY = "label"
  
  print("Creating model without CV")
  system.time(h2o.glm <- h2o.glm(x=myX, y=myY, data=adult.train, key="h2o.glm.adult", family="binomial", alpha=1, higher_accuracy=T, lambda_search=T, nfolds=0, variable_importances=1, use_all_factor_levels=1))
  h2o.glm
  
  print("Creating model with CV")
  system.time(h2o.glm.CV <- h2o.glm(x=myX, y=myY, data=adult.train, key="h2o.glm.CV.adult", family="binomial", alpha=1, higher_accuracy=T, lambda_search=T, nfolds=5, variable_importances=1, use_all_factor_levels=1))    # This line is failing
  h2o.glm.CV
  
  testEnd()
}

doTest("Testing glm cross validation performance with adult dataset", test)
