source('./Utils/h2oR.R')

logging("\nLoading LiblineaR and ROCR packages\n")

if(!"LiblineaR" %in% rownames(installed.packages())) install.packages("LiblineaR")
if(!"ROCR" %in% rownames(installed.packages())) install.packages("ROCR")
require(LiblineaR)
require(ROCR)

logging("\n======================== Begin Test ===========================\n")
H2Ocon <- new("H2OClient", ip=myIP, port=myPort)

test.LiblineaR <- function(con) {
  L1logistic <- function(train,trainLabels,test,testLabels,trainhex,testhex) {
    logging("\nUsing default parameters for LiblineaR: \n")
    logging("   type =    6: Logistic Regression L1-Regularized\n")
    logging("   cost =    1: Cost of constraints parameter\n")
    logging("epsilon = 0.01: Tolerance of termination criterion\n")
    logging("  cross =    0: No k-fold cross-validation\n")
    LibR.m      <- LiblineaR(train, trainLabels,type=6)
    LibRpreds  <- predict(LibR.m, test, proba=1, decisionValues=TRUE)
    LibRCM <- table(testLabels, LibRpreds$predictions)
    
    LibRPrecision <- LibRCM[1] / (LibRCM[1] + LibRCM[3])
    LibRRecall    <- LibRCM[1] / (LibRCM[1] + LibRCM[2])
    LibRF1        <- 2 * (LibRPrecision * LibRRecall) / (LibRPrecision + LibRRecall)
    LibRAUC       <- performance(prediction(as.numeric(LibRpreds$predictions), testLabels), measure = "auc")@y.values
    
    logging("\nUsing default parameters for H2O: \n")
    logging(" family =  'binomial': Logistic Regression\n")
    logging(" lambda =       1E-05: Shrinkage Parameter\n")
    logging("epsilon =       1E-04: Tolerance of termination criterion\n")
    logging(" nfolds =           0: No k-fold cross-validation\n")
    h2o.m <- h2o.glm(x      = c("AGE", "RACE", "PSA", "DCAPS"), 
                     y      = "CAPSULE", 
                     data   = trainhex, 
                     family = "binomial",
                     nfolds = 1, 
                     lambda = 1.0E-5)
    
    h2op     <- h2o.predict(h2o.m, testhex)
    h2opreds <- head(h2op, nrow(h2op))
    h2oCM    <- table(testLabels, h2opreds$CAPSULE)
    
    h2oPrecision <- h2oCM[1] / (h2oCM[1] + h2oCM[3])
    h2oRecall    <- h2oCM[1] / (h2oCM[1] + h2oCM[2])
    h2oF1        <- 2 * (h2oPrecision * h2oRecall) / (h2oPrecision + h2oRecall)
    h2oAUC       <- performance(prediction(h2opreds$CAPSULE, testLabels), measure = "auc")@y.values
    
    cat("\n                ============= H2O Performance =============\n")
    cat("H2O AUC (performance(prediction(predictions,actual))): ", h2oAUC[[1]], "\n")
    cat("                        H2O Precision (tp / (tp + fp): ", h2oPrecision, "\n")
    cat("                           H2O Recall (tp / (tp + fn): ", h2oRecall, "\n")
    cat("                                         H2O F1 Score: ", h2oF1, "\n")
    cat("\n                ========= LiblineaR Performance ===========\n")
    cat("LiblineaR AUC (performance(prediction(predictions,actual))): ", LibRAUC[[1]], "\n")
    cat("                        LiblineaR Precision (tp / (tp + fp): ", LibRPrecision, "\n")
    cat("                           LiblineaR Recall (tp / (tp + fn): ", LibRRecall, "\n")
    cat("                                         LiblineaR F1 Score: ", LibRF1, "\n")  
  }
  
  L2logistic <- function(train,trainLabels,test,testLabels,trainhex,testhex) {
    logging("\nUsing default parameters for LiblineaR: \n")
    logging("   type =    0: Logistic Regression L2-Regularized\n")
    logging("   cost =    1: Cost of constraints parameter\n")
    logging("epsilon = 0.01: Tolerance of termination criterion\n")
    logging("  cross =    0: No k-fold cross-validation\n")
    LibR.m      <- LiblineaR(train, trainLabels)
    LibRpreds  <- predict(LibR.m, test, proba=1, decisionValues=TRUE)
    LibRCM <- table(testLabels, LibRpreds$predictions)
    
    LibRPrecision <- LibRCM[1] / (LibRCM[1] + LibRCM[3])
    LibRRecall    <- LibRCM[1] / (LibRCM[1] + LibRCM[2])
    LibRF1        <- 2 * (LibRPrecision * LibRRecall) / (LibRPrecision + LibRRecall)
    LibRAUC       <- performance(prediction(as.numeric(LibRpreds$predictions), testLabels), measure = "auc")@y.values
    
    logging("\nUsing default parameters for H2O: \n")
    logging(" family =  'binomial': Logistic Regression\n")
    logging(" lambda =       1E-05: Shrinkage Parameter\n")
    logging("epsilon =       1E-04: Tolerance of termination criterion\n")
    logging(" nfolds =           0: No k-fold cross-validation")
    h2o.m <- h2o.glm(x      = c("AGE", "RACE", "PSA", "DCAPS"), 
                     y      = "CAPSULE", 
                     data   = trainhex, 
                     family = "binomial",
                     nfolds = 1, 
                     lambda = 1.0E-5)
    
    h2op     <- h2o.predict(h2o.m, testhex)
    h2opreds <- head(h2op, nrow(h2op))
    h2oCM    <- table(testLabels, h2opreds$CAPSULE)
    
    h2oPrecision <- h2oCM[1] / (h2oCM[1] + h2oCM[3])
    h2oRecall    <- h2oCM[1] / (h2oCM[1] + h2oCM[2])
    h2oF1        <- 2 * (h2oPrecision * h2oRecall) / (h2oPrecision + h2oRecall)
    h2oAUC       <- performance(prediction(h2opreds$CAPSULE, testLabels), measure = "auc")@y.values
    
    cat("\n============= H2O Performance =============\n")
    cat("H2O AUC (performance(prediction(predictions,actual))): ", h2oAUC[[1]], "\n")
    cat("                        H2O Precision (tp / (tp + fp): ", h2oPrecision, "\n")
    cat("                           H2O Recall (tp / (tp + fn): ", h2oRecall, "\n")
    cat("                                         H2O F1 Score: ", h2oF1, "\n")
    cat("\n========= LiblineaR Performance ===========\n")
    cat("LiblineaR AUC (performance(prediction(predictions,actual))): ", LibRAUC[[1]], "\n")
    cat("                        LiblineaR Precision (tp / (tp + fp): ", LibRPrecision, "\n")
    cat("                           LiblineaR Recall (tp / (tp + fn): ", LibRRecall, "\n")
    cat("                                         LiblineaR F1 Score: ", LibRF1, "\n")
  }
  
  logging("\nImporting prostate test/train data...\n")
  prostate.train.hex <- h2o.uploadFile(H2Ocon, "../../smalldata/logreg/prostate_train.csv", "pTrain.hex")
  prostate.test.hex  <- h2o.uploadFile(H2Ocon, "../../smalldata/logreg/prostate_test.csv", "pTest.hex")
  
  prostate.train.dat <- head(prostate.train.hex,nrow(prostate.train.hex))
  prostate.test.dat <- head(prostate.test.hex,nrow(prostate.test.hex))
  xTrain <- prostate.train.dat[,-1]
  yTrain <- factor(prostate.train.dat[,1])
  xTest <- prostate.test.dat[,-1]
  yTest <- factor(prostate.test.dat[,1])
  
  L1logistic(xTrain,yTrain,xTest,yTest,prostate.train.hex,prostate.test.hex)
  L2logistic(xTrain,yTrain,xTest,yTest,prostate.train.hex,prostate.test.hex)
}

test_that("LiblineaR Test", test.LiblineaR(H2Ocon))
