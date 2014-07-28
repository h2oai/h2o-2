##
# NOPASS TEST: The following bug is associated with JIRA PUB-874
# 'Discrepancy Reporting GLM Cross Validation Models in R'
# Testing R's glm model with cross validation on for Binomial and Gaussian distribution
##


setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')


test <- function(conn) {
  
	print("Reading in Mushroom data for binomial glm.")
	mushroom.train <-  h2o.uploadFile(conn, locate("smalldata/Mushroom.gz"), key="mushroom.train")
	mushroom.train$label <- ifelse(mushroom.train$"C1"=="e",1,0)
	head(mushroom.train, 15)
	dim(mushroom.train)
	myX = c(2:23)
	myY = "label"
	print("Creating model without CV")
	system.time(h2o.glm <- h2o.glm(x=myX, y=myY, data=mushroom.train, key="h2o.glm.mushroom", family="binomial", alpha=1, higher_accuracy=T, lambda_search=T, nfolds=0, variable_importances=1, use_all_factor_levels=1))
	h2o.glm
	print("Creating model with CV")
	system.time(h2o.glm.CV <- h2o.glm(x=myX, y=myY, data=mushroom.train, key="h2o.glm.CV.mushroom", family="binomial", alpha=1, higher_accuracy=T, lambda_search=T, nfolds=5, variable_importances=1, use_all_factor_levels=1))
	print(h2o.glm.CV) ## Reported non zeros and deviance explained are 0, auc is 0.5



	print("Reading in Abalone data for gaussian glm.")
	abalone.train <-  h2o.uploadFile(conn, locate("smalldata/Abalone.gz"), key="abalone.train")
	head(abalone.train, 15)
	dim(abalone.train)
	myX = c(1:8)
	myY = "C9"
	print("Creating model without CV")
	system.time(h2o.glm <- h2o.glm(x=myX, y=myY, data=abalone.train, key="h2o.glm.abalone", family="gaussian", alpha=1, higher_accuracy=T, lambda_search=T, nfolds=0, variable_importances=1, use_all_factor_levels=1))
	h2o.glm
	print("Creating model with CV")
	system.time(h2o.glm.CV <- h2o.glm(x=myX, y=myY, data=abalone.train, key="h2o.glm.CV.abalone", family="gaussian", alpha=1, higher_accuracy=T, lambda_search=T, nfolds=5, variable_importances=1, use_all_factor_levels=1))
	print(h2o.glm.CV) ## Reported non zeros and deviance explained are 0
  
  testEnd()
}

doTest("Testing R's glm model with cross validation on for Binomial and Gaussian distribution", test)
