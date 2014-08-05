##
# Testing glm performance (reasonable coefficients) on balanced synthetic dataset with perfect separation.
# Separation recognized by R glm with following warning: 
#       1: glm.fit: algorithm did not converge 
#       2: glm.fit: fitted probabilities numerically 0 or 1 occurred 
##


setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')


test <- function(conn) {

    print("Generate balanced dataset by column in R")
        y.a = sample(0:0, 200, replace=T)
        y.b = sample(1:1, 200, replace=T)
        x1.a = sample(-1203:-1, 200, replace=T)
        x1.b = sample(1:1, 200, replace=T)
        x2.a = sample(0:0, 200, replace=T)
        x2.b = sample(1:1203, 200, replace=T)
        data.a = cbind(y.a, x1.a, x2.a)
        data.b = cbind(y.b, x1.b, x2.b)
        data.balanced = rbind(data.a, data.b)
        colnames(data.balanced) <- c("y", "x1", "x2")

    print("Read data into H20.")
        data.b.hex <- as.h2o(conn, as.data.frame(data.balanced), "data.b.hex")
    print("Fit model on dataset.")
        model.balanced <- h2o.glm(x=c("x1", "x2"), y="y", data.b.hex, family="binomial", lambda_search=TRUE, use_all_factor_levels=1, alpha=0.5, nfolds=0, higher_accuracy=TRUE, lambda=0)
    print("Check line search invoked even with higher_accuracy off")
        model.balanced.ls <- h2o.glm(x=c("x1", "x2"), y="y", data.b.hex, family="binomial", lambda_search=TRUE, use_all_factor_levels=1, alpha=0.5, nfolds=0, higher_accuracy=FALSE, lambda=0)

    print("Extract models' coefficients and assert reasonable values (ie. no greater than 50)")
    print("Balanced dataset; higher_accuracy TRUE")
        coef <- model.balanced@model$coefficients
        suppressWarnings((coef$"Intercept"<-NULL))
        stopifnot(coef < 50)
    print("Balanced dataset; higher_accuracy FALSE")
        coef.ls <- model.balanced.ls@model$coefficients
        suppressWarnings((coef.ls$"Intercept"<-NULL))
        stopifnot(coef.ls < 50)

    testEnd()
}

doTest("Testing glm performance on balanced synthetic dataset with perfect separation.", test)