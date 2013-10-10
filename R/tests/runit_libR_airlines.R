source('./Utils/h2oR.R')
logging("\nLoading LiblineaR and ROCR packages\n")
if(!"LiblineaR" %in% rownames(installed.packages())) install.packages("LiblineaR")
if(!"ROCR" %in% rownames(installed.packages())) install.packages("ROCR")
require(LiblineaR)
require(ROCR)

logging("\n======================== Begin Test ===========================\n")
H2Ocon <- new("H2OClient", ip=myIP, port=myPort)

test.LiblineaR.airlines <- function(con) {
  L1logistic <- function(train,trainLabels,test,testLabels,trainhex,testhex) {
    logging("\nUsing these parameters for LiblineaR: \n")
    logging("   type =    0: Logistic Regression L1-Regularized\n")
    logging("   cost = 100: Cost of constraints parameter\n")
    logging("epsilon = 1E-4: Tolerance of termination criterion\n")
    logging("  cross =    0: No k-fold cross-validation\n")
    
    LibR.m        <- LiblineaR(train, trainLabels,type=0, epsilon=1E-4, cost=100)
    LibRpreds     <- predict(LibR.m, test, proba=1, decisionValues=TRUE)
    LibRCM        <- table(testLabels, LibRpreds$predictions)
    LibRPrecision <- LibRCM[1] / (LibRCM[1] + LibRCM[3])
    LibRRecall    <- LibRCM[1] / (LibRCM[1] + LibRCM[2])
    LibRF1        <- 2 * (LibRPrecision * LibRRecall) / (LibRPrecision + LibRRecall)
    LibRAUC       <- performance(prediction(as.numeric(LibRpreds$predictions), testLabels), measure = "auc")@y.values
    
    logging("\nUsing these parameters for H2O: \n")
    logging(" family =          'binomial': Logistic Regression\n")
    logging(" lambda = 1 / (cost * params) [3.8e-05]: Shrinkage Parameter\n")
    logging("  alpha =                           0.0: Elastic Net Parameter\n")
    logging("epsilon =                         1E-04: Tolerance of termination criterion\n")
    logging(" nfolds =                             1: No k-fold cross-validation\n")
    h2o.m <- h2o.glm(x            = c("DepTime", "ArrTime", "Distance"),
                                    #c("fYear","fMonth","fDayofMonth","fDayOfWeek","DepTime","ArrTime","UniqueCarrier","Origin","Dest","Distance"), 
                     y            = "IsDepDelayed_REC", 
                     data         = trainhex, 
                     family       = "binomial",
                     nfolds       = 1, 
                     lambda       = 1 / (3*100),
                     alpha        = 0.0,
                     standardize  = 1,
                     beta_epsilon = 1E-4)
    
    h2op         <- h2o.predict(h2o.m, testhex)
    h2opreds     <- head(h2op, nrow(h2op))
    h2oCM        <- table(testLabels, h2opreds$IsDepDelayed_REC)
    h2oPrecision <- h2oCM[1] / (h2oCM[1] + h2oCM[3])
    h2oRecall    <- h2oCM[1] / (h2oCM[1] + h2oCM[2])
    h2oF1        <- 2 * (h2oPrecision * h2oRecall) / (h2oPrecision + h2oRecall)
    h2oAUC       <- performance(prediction(h2opreds$IsDepDelayed_REC, testLabels), measure = "auc")@y.values
    
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
    return(list(h2o.m,LibR.m));
  }

  compareCoefs <- function(h2o, libR) {
    logging("\n
            Comparing the L1-regularized LR coefficients (should be close in magnitude)
            Expect a sign flip because modeling against log(p/(1-p)) vs log((1-p)/p).
            Note that this is not the issue of consistency of signs between odds ratios
            and coefficients.\n")
    

    cat("\n H2O betas: ", h2o@model$coefficients, "\n")
    cat("\n ============================================== \n")
    cat("\n LiblineaR betas: ", libR$W, "\n")
    cat("\n ============================================== \n")
    rms_diff <- sqrt(sum(abs(h2o@model$coefficients) - abs(libR$W))**2)
    cat("RMS of the absolute difference in the sets of coefficients is: ", rms_diff, "\n")
    cat(all.equal(abs(as.vector(h2o@model$coefficients)), abs(as.vector(libR$W))), "\n")
  }
  
  logging("\nImporting Airlines test/train data...\n")
  exdir         <- "../../smalldata/airlines/unzipped"
  airlinesTrain <- "../../smalldata/airlines/AirlinesTrain.csv.zip"
  airlinesTest  <- "../../smalldata/airlines/AirlinesTest.csv.zip"
  aTrain        <- na.omit(read.zip(zipfile = airlinesTrain, exdir = exdir))
  aTest         <- na.omit(read.zip(zipfile = airlinesTest,  exdir = exdir))
  trainhex      <- h2o.uploadFile(H2Ocon, paste(exdir, "/AirlinesTrain.csv", sep=""), "aTrain.hex")
  testhex       <- h2o.uploadFile(H2Ocon, paste(exdir, "/AirlinesTest.csv",  sep=""), "aTest.hex")
  remove_exdir(exdir)
  
  #xTrain  <- scale(model.matrix(IsDepDelayed_REC ~., aTrain[,-11])[,-1])
  xTrain  <- scale(data.frame(aTrain$DepTime, aTrain$ArrTime, aTrain$Distance))
  yTrain  <- aTrain[,12]
  #xTest   <- model.matrix(IsDepDelayed_REC ~., aTest[-11])[,-1]
  xTest   <- scale(data.frame(aTest$DepTime, aTest$ArrTime, aTest$Distance))
  yTest   <- aTest[,12]
  models  <- L1logistic(xTrain,yTrain,xTest,yTest,trainhex,testhex)
  compareCoefs(models[[1]], models[[2]])
}
options(digits=8)
test_that("LiblineaR Test Airlines", test.LiblineaR.airlines(H2Ocon))
