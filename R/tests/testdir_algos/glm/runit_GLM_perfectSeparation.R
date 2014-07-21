##
# Testing glm performance (reasonable coefficients) on balanced and unbalanced synthetic datasets with perfect separation.
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
    print("Dimensions of dataset: ")
        print(dim(data.b.hex))
    print("Fit model on dataset.")
        model.balanced <- h2o.glm(x=c("x1", "x2"), y="y", data.b.hex, family="binomial", lambda_search=TRUE, use_all_factor_levels=1, alpha=0.5, nfolds=0, higher_accuracy=TRUE, lambda=0)
    print("Check line search invoked even with higher_accuracy off")
        model.balanced.ls <- h2o.glm(x=c("x1", "x2"), y="y", data.b.hex, family="binomial", lambda_search=TRUE, use_all_factor_levels=1, alpha=0.5, nfolds=0, higher_accuracy=FALSE, lambda=0)

    print("Extract models' coefficients")
    print("Balanced dataset; higher_accuracy TRUE")
        coef <- model.balanced@model$coefficients
        suppressWarnings((coef$"Intercept"<-NULL))
        stopifnot(coef < 50)
    print("Balanced dataset; higher_accuracy FALSE")
        coef.ls <- model.balanced.ls@model$coefficients
        suppressWarnings((coef.ls$"Intercept"<-NULL))
        stopifnot(coef.ls < 50)


    print("Generate unbalanced dataset by column in R")
        y = sample(0:0, 10000, replace=T)
        x1 = sample(-1:-10, 10000, replace=T)
        x2 = sample(6:10, 10000, replace=T)
        data = cbind(y, x1, x2)
        data.unbalanced = rbind(data, c(1, 30, 7))

    print("Read data into H20.")
        data.u.hex <- as.h2o(conn, as.data.frame(data.unbalanced), "data.u.hex")
    print("Dimensions of dataset: ")
        print(dim(data.u.hex))
    print("Fit model on dataset.")
        model.unbalanced <- h2o.glm(x=c("x1", "x2"), y="y", data.u.hex, family="binomial", lambda_search=TRUE, use_all_factor_levels=1, alpha=0.5, nfolds=0, higher_accuracy=TRUE, lambda=0)
    print("Check line search invoked even with higher_accuracy off")
        model.unbalanced.ls <- h2o.glm(x=c("x1", "x2"), y="y", data.u.hex, family="binomial", lambda_search=TRUE, use_all_factor_levels=1, alpha=0.5, nfolds=0, higher_accuracy=FALSE, lambda=0)

    print("Extract models' coefficients")
    print("Unbalanced dataset; higher_accuracy TRUE")
            coef <- model.unbalanced@model$coefficients
        suppressWarnings((coef$"Intercept"<-NULL))
        stopifnot(coef < 50)
    print("Unbalanced dataset; higher_accuracy FALSE")
            coef.ls <- model.unbalanced.ls@model$coefficients
        suppressWarnings((coef.ls$"Intercept"<-NULL))
        stopifnot(coef.ls < 50)

    testEnd()
}

doTest("Testing glm performance on balanced and unbalanced synthetic datasets with perfect separation.", test)