source('./Utils/h2oR.R')
#source('../../R/h2o-package/R/Internal.R')
#source('../../R/h2o-package/R/Algorithms.R')
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
    logging("   cost =  100: Cost of constraints parameter\n")
    logging("epsilon = 1E-2: Tolerance of termination criterion\n")
    logging("  cross =    0: No k-fold cross-validation\n")
    LibR.m      <- LiblineaR(train, trainLabels,type=6, epsilon=1E-2, cost=100)
    LibRpreds  <- predict(LibR.m, test, proba=1, decisionValues=TRUE)
    LibRCM <- table(testLabels, LibRpreds$predictions)
    
    LibRPrecision <- LibRCM[1] / (LibRCM[1] + LibRCM[3])
    LibRRecall    <- LibRCM[1] / (LibRCM[1] + LibRCM[2])
    LibRF1        <- 2 * (LibRPrecision * LibRRecall) / (LibRPrecision + LibRRecall)
    LibRAUC       <- performance(prediction(as.numeric(LibRpreds$predictions), testLabels), measure = "auc")@y.values
    
    logging("\nUsing these parameters for H2O: \n")
    logging(" family = 'binomial': Logistic Regression\n")
    logging(" lambda =         34: Shrinkage Parameter\n")
    logging("  alpha =        0.0: Elastic Net Parameter\n")
    logging("epsilon =      1E-02: Tolerance of termination criterion\n")
    logging(" nfolds =          1: No k-fold cross-validation\n")
    h2o.m <- h2o.glm(x            = c("GLEASON","DPROS","PSA","DCAPS","AGE","RACE","VOL"), 
                     y            = "CAPSULE", 
                     data         = trainhex, 
                     family       = "binomial",
                     nfolds       = 1, 
                     lambda       = 34,
                     alpha        = 0.0,
                     beta_epsilon = 1E-2)
    
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
    cat("\n                ========= H2O & LibR coeff. comparison ====\n")
    cat("              ", format(names(h2o.m@model$coefficients),width=10,justify='right'))
    cat("\n H2O coefficients: ", h2o.m@model$coefficients)
    cat("\nLibR coefficients: ", LibR.m$W, "\n")
    return(list(h2o.m,LibR.m))
  }
  
  L2logistic <- function(train,trainLabels,test,testLabels,trainhex,testhex) {
    logging("\nUsing these parameters for LiblineaR: \n")
    logging("   type =    0: Logistic Regression L2-Regularized\n")
    logging("   cost =  100: Cost of constraints parameter\n")
    logging("epsilon = 1E-2: Tolerance of termination criterion\n")
    logging("  cross =    0: No k-fold cross-validation\n")
    LibR.m      <- LiblineaR(train, trainLabels, type=0, epsilon=1E-2,cost=0.0097)
    LibRpreds  <- predict(LibR.m, test, proba=1, decisionValues=TRUE)
    LibRCM <- table(testLabels, LibRpreds$predictions)
    
    LibRPrecision <- LibRCM[1] / (LibRCM[1] + LibRCM[3])
    LibRRecall    <- LibRCM[1] / (LibRCM[1] + LibRCM[2])
    LibRF1        <- 2 * (LibRPrecision * LibRRecall) / (LibRPrecision + LibRRecall)
    LibRAUC       <- performance(prediction(as.numeric(LibRpreds$predictions), testLabels), measure = "auc")@y.values
    
    logging("\nUsing these parameters for H2O: \n")
    logging(" family = 'binomial': Logistic Regression\n")
    logging(" lambda =      1E-03: Shrinkage Parameter\n")
    logging("  alpha =        0.0: Elastic Net Parameter\n")
    logging("epsilon =      1E-02: Tolerance of termination criterion\n")
    logging(" nfolds =          1: No k-fold cross-validation")
    h2o.m <- h2o.glm(x            = c("GLEASON","DPROS","PSA","DCAPS","AGE","RACE","VOL"), 
                     y            = "CAPSULE", 
                     data         = trainhex, 
                     family       = "binomial",
                     nfolds       = 1, 
                     lambda       = 1E-3,
                     alpha        = 0.00,
                     beta_epsilon = 1E-2)
    
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
    cat("\n                ========= H2O & LibR coeff. comparison ===\n")
    cat("              ", format(names(h2o.m@model$coefficients),width=10,justify='right'), "\n")
    cat(" H2O coefficients: ", h2o.m@model$coefficients, "\n")
    cat("LibR coefficients: ", LibR.m$W, "\n")
    return(list(h2o.m,LibR.m))
  }
  
  compareCoefs <- function(h2o, libR) {
    logging("\n
            Comparing the L1-regularized LR coefficients (should be close in magnitude)
            Expect a sign flip because modeling against log(p/(1-p)) vs log((1-p)/p).
            Note that this is not the issue of consistency of signs between odds ratios
            and coefficients.\n")

    cat("\n                ========= H2O & LibR coeff. comparison ===\n")
    cat("              ", format(names(h2o@model$coefficients),width=10,justify='right'), "\n")
    cat(" H2O coefficients: ", h2o@model$coefficients, "\n")
    cat("LibR coefficients: ", libR$W, "\n")
    rms_diff <- sqrt(sum(abs(h2o@model$coefficients) - abs(libR$W))**2)
    cat("RMS of the absolute difference in the sets of coefficients is: ", rms_diff, "\n")
    cat(all.equal(abs(as.vector(h2o@model$coefficients)), abs(as.vector(libR$W))), "\n")
  }

  logging("\nImporting prostate test/train data...\n")
  prostate.train.hex <- h2o.uploadFile(H2Ocon, "../../smalldata/logreg/prostate_train.csv", "pTrain.hex")
  prostate.test.hex  <- h2o.uploadFile(H2Ocon, "../../smalldata/logreg/prostate_test.csv", "pTest.hex")
  prostate.train.dat <- head(prostate.train.hex,nrow(prostate.train.hex))
  prostate.test.dat  <- head(prostate.test.hex,nrow(prostate.test.hex))
  xTrain             <- prostate.train.dat[,-1]
  yTrain             <- factor(prostate.train.dat[,1])
  xTest              <- prostate.test.dat[,-1]
  yTest              <- factor(prostate.test.dat[,1])
  models             <- L1logistic(xTrain,yTrain,xTest,yTest,prostate.train.hex,prostate.test.hex)
  models2            <- L2logistic(xTrain,yTrain,xTest,yTest,prostate.train.hex,prostate.test.hex)
  compareCoefs(models2[[1]], models[[2]])
}

test_that("LiblineaR Test", test.LiblineaR(H2Ocon))
