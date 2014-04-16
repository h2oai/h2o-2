##                                                      ##
#   This prologue file contains "public" and "private"   #
#   R methods. The public methods are responsible for    #
#   handling the setup of h2o and any GLOBALS needed     #
#   by the epilogue file.                                #
#                                                        #
#   This file also contains a number of helper functions #
#   such as hdfs.VA. These are simple wrappers of the    #
#   h2o-R function names. Using them is MANDATORY.       #
#                                                        # 
#   "Private" function declarations begin with a '.'     #
##                                                      ##



#TODO list:
  #Track upload, import, and import HDFS
  #Track VA v FV


.libPaths(c("~/.libR/", .libPaths()))
options(echo=F)
#source("../../../R/h2o-package/R/Internal.R")
#source("../../../R/h2o-package/R/Algorithms.R")
#source("../../../R/h2o-package/R/Classes.R")
#source("../../../R/h2o-package/R/ParseImport.R")

source("../../../../../R/h2o-package/R/Internal.R")
source("../../../../../R/h2o-package/R/Algorithms.R")
source("../../../../../R/h2o-package/R/Classes.R")
source("../../../../../R/h2o-package/R/ParseImport.R")


#GLOBALS
aic               <<- "None"
auc               <<- "None"
.binomial         <<- "None"
cm.json           <<- "None"
confusion_matrix  <<- "None"
correct_pass      <<- "None"
data_center       <<- "None"
data_name         <<- "None"    
data_source       <<- "None" 
end_time          <<- "None"    
error_rate        <<- "None"  
h                 <<- "None"    
IP                <<- "None"    
.isTest           <<- FALSE
kmeans_k          <<- "None"
kmeans_withinss   <<- "None"
levels.json       <<- "None"
minority_error    <<- "None"
model             <<- "None"
model.json        <<- "None"
null_dev          <<- "None"
num_explan_cols   <<- -1
num_train_rows    <<- -1   
path              <<- "None"
phase             <<- "None"
PORT              <<- "None"
precision         <<- "None"
predict_type      <<- "None"
recall            <<- "None"
.representation   <<- -1
res_dev           <<- "None"
response          <<- "None"
start_time        <<- "None"
testData          <<- "None"
test_data_name    <<- "None"
test_data_url     <<- "None"
time_pass         <<- "None"
trainData         <<- "None"
train_data_name   <<- "None"
train_data_url    <<- "None"

#Internal.R Extensions:
.h2o.__PAGE_CM <- "2/ConfusionMatrix.json" #actual/vactual, predict/vpredict
.h2o.__GLM_SCORE <- "GLMScore.json" #model_key, key, thresholds
.h2o.__PAGE_AUC <- "2/AUC.json"

#"Private" Methods
.setup<-
function() {
  options(echo=F)
  options(digits=16)
  local({r <- getOption("repos"); r["CRAN"] <- "http://cran.us.r-project.org"; options(repos = r)})
  if (!"R.utils" %in% rownames(installed.packages())) install.packages("R.utils")
  library(R.utils)
  .getArgs(commandArgs(trailingOnly = TRUE))
  .h2oSetup()
  .calcPhase()
#  .locateInternal()
  start_time <<- round(System$currentTimeMillis())[[1]]
}

.locateInternal<-
function() {
#  for (i in 1:10) {
#    bn <- basename(path)
#    if (bn == "py") {
#      source("../R/h2oPerf/Internal.R")
#      print("SUCCESSFULLY SOURCED INTERNAL R CODE")
      return(0)
#    }
#    if (bn == "h2o") {
#      stop("Couldn't find Internal.R")
#    }
#  }
}

h2o.removeAll <-
function(object) {
  .h2o.__remoteSend(object, .h2o.__PAGE_REMOVEALL)
}

.calcPhase<-
function() {
  path <<- normalizePath(basename(R.utils::commandArgs(asValues=TRUE)$"f"))
  f <- function(it) {
    return(grepl(it, path))
  }
  pa <- sapply(c("parse", "model", "predict"), f)
  phase <<- tolower(names(pa[pa == TRUE]))
  if(phase == "parse") {
    if (file.exists(".RData")) file.remove(".RData")
  }
}

.getArgs<-
function(args) {
  fileName <- commandArgs()[grepl('*\\.R',unlist(commandArgs()))]

  if (length(args) == 0) {
    IP   <<- "127.0.0.1"
    PORT <<- 54321
  } else {
    argsplit <- strsplit(args[1], ":")[[1]]
    IP       <<- argsplit[1]
    PORT     <<- as.numeric(argsplit[2])
  }
}

.installDepPkgs<- 
function(optional = FALSE) {
  myPackages <- rownames(installed.packages())
  myReqPkgs  <- c("RCurl", "rjson", "tools", "statmod")

  # For plotting clusters in h2o.kmeans demo
  if(optional)
    myReqPkgs <- c(myReqPkgs, "fpc", "cluster")

  # For communicating with H2O via REST API
  temp <- lapply(myReqPkgs, function(x) { if(!x %in% myPackages) install.packages(x) })
  temp <- lapply(myReqPkgs, require, character.only = TRUE)
}

.h2oSetup<- 
function() {
  if (!"h2o" %in% rownames(installed.packages())) {
      envPath  <- Sys.getenv("H2OWrapperDir")
      wrapDir  <- ifelse(envPath == "", defaultPath, envPath)
      wrapName <- list.files(wrapDir, pattern  = "h2o")[1]
      wrapPath <- paste(wrapDir, wrapName, sep = "/")

      if (!file.exists(wrapPath))
        stop(paste("h2o package does not exist at", wrapPath));
      print(paste("Installing h2o package from", wrapPath))
      .installDepPkgs()
      install.packages(wrapPath, repos = NULL, type = "source")
    }
 
  .installDepPkgs()
  library(h2o)
  h <<- h2o.init(ip            = IP,
                port           = PORT,
                startH2O       = FALSE)
}

#"Public" Methods

#Import/Parsing
upload.VA<-
function(pkey, dataPath) {
  h2o.uploadFile.VA(h, path = dataPath, key = pkey)
}

upload.FV<-
function(pkey, dataPath) {
  h2o.uploadFile.FV(h, dataPath, key = pkey)
}

import.VA<-
function(pkey, dataPath) {
  h2o.importFile.VA(h, dataPath, key = pkey)
}

import.FV<-
function(pkey, dataPath) {
  h2o.importFile.FV(h, dataPath, key = pkey)
}

hdfs.VA<-
function(pkey, dataPath) {
  h2o.importHDFS.VA(h, dataPath, key = pkey)
source("../../R/h2oPerf/prologue.R")

data_source <<- "s3://h2o-bench/AirlinesClean2"
trainData   <<- "s3n://h2o-bench/AirlinesClean2"

hex <- h2o.importFile.FV(h, "s3n://h2o-bench/AirlinesClean2")

num_train_rows <<- 1021368222
num_explan_cols <<- 12
upload.VA("parsed.hex", trainData)
source("../../R/h2oPerf/epilogue.R")
}

hdfs.FV<-
function(pkey, dataPath) {
  h2o.importHDFS.FV(h, dataPath, key = pkey)
}

#Modeling
runSummary.VA<-
function() {
  data <- new("H2OParsedDataVA", h2o = h, key = "parsed.hex", logic = FALSE)
  summary(data)
}

runSummary.FV<-
function() {
  data <- new("H2OParsedData", h2o = h, key = "parsed.hex", logic = TRUE)
  summary(data)
}

runH2o.ddply<-
function(.variables, .fun = NULL, ..., .progress = 'none') {
  data <- new("H2OParsedDataVA", h2o = h, key = "parsed.hex", logic = FALSE)
  h2o.ddply(data, .variables, .fun, ..., .progress)
}

runGBM<-
function(x, y, distribution='multinomial', 
         n.trees=10, interaction.depth=5, 
         n.minobsinnode=10, shrinkage=0.02, 
         n.bins=100) {
  data <- new("H2OParsedData", h2o = h, key = "parsed.hex", logic = TRUE)
  model <<- h2o.gbm(x = x, y = y, distribution = distribution, data = data, n.trees = n.trees,
          interaction.depth = interaction.depth, n.minobsinnode = n.minobsinnode,
          shrinkage = shrinkage, n.bins = n.bins)
  model.json <<- .h2o.__remoteSend(h, .h2o.__PAGE_GBMModelView, '_modelKey' = model@key)
}

runGLM.VA<-
function(x, y, family, nfolds=10, alpha=0.5, lambda=1.0e-5, epsilon=1.0e-5, 
         standardize=TRUE, tweedie.p=ifelse(family=='tweedie', 1.5, as.numeric(NA)), 
         thresholds=ifelse(family=='binomial', seq(0, 1, 0.01), as.numeric(NA))) {
  data <- new("H2OParsedDataVA", h2o = h, key = "parsed.hex", logic = FALSE)
  model <<- h2o.glm.VA(x = x, y = y, data = data, family = family, nfolds = nfolds,
                       alpha = alpha, lambda = lambda, epsilon = epsilon, standardize = standardize,
                       tweedie.p = tweedie.p, thresholds = thresholds)
  model.json <<- .h2o.__remoteSend(h, .h2o.__PAGE_INSPECT, key = model@key)
}

runGLM.FV<-
function(x, y, family, nfolds = 10, alpha = 0.5, lambda = 1.0e-5, epsilon = 1.0e-5, 
         standardize = TRUE, tweedie.p = ifelse(family == "tweedie", 0, as.numeric(NA))) {
  data <- new("H2OParsedData", h2o = h, key = "parsed.hex", logic = TRUE)
  model <<- h2o.glm.FV(x = x, y = y, data = data, family = family, nfolds = nfolds, alpha = alpha,
                       lambda = lambda, epsilon = epsilon, standardize = standardize, tweedie.p = tweedie.p)
  model.json <<- .h2o.__remoteSend(h, .h2o.__PAGE_GLMModelView, '_modelKey'=model@key)
}

runKMeans.FV<-
function(centers, cols = '', iter.max = 10, normalize = FALSE) {
  data <- new("H2OParsedData", h2o = h, key = "parsed.hex", logic = TRUE)
  model <<- h2o.kmeans.FV(data = data, centers = centers, cols = cols, iter.max = iter.max, normalize = normalize)
  model.json <<- .h2o.__remoteSend(h, .h2o.__PAGE_KM2ModelView, model = model@key)
  kmeans_k <<- dim(model@model$centers)[1]
  kmeans_withinss <<- model@model$tot.withinss
}

runKMeans.VA<-
function(centers, cols = '', iter.max = 10, normalize = FALSE) {
  data <- new("H2OParsedDataVA", h2o = h, key = "parsed.hex", logic = TRUE)
  model <<- h2o.kmeans.VA(data = data, centers = centers, cols = cols, iter.max = iter.max, normalize = normalize)
  model.json <<- .h2o.__remoteSend(h, .h2o.__PAGE_INSPECT, key = model@key)
  kmeans_k <<- dim(model@model$centers)[1]
  kmeans_withinss <<- model@model$tot.withinss
}

runNN<-
function(x, y, classification=T, activation='Tanh', layers=500, 
         rate=0.01, l1_reg=1e-4, l2_reg=0.0010, epoch=100) {
  data <- new("H2OParsedData", h2o = h, key = "parsed.hex", logic = TRUE)
  model <<- h2o.nn(x = x, y = y, data = data, classification = classification,
                   activation = activation, layers = layers, rate = rate, 
                   l1_reg = l1_reg, l2_reg = l2_reg, epoch = epoch)
  model.json <<- .h2o.__remoteSend(h, .h2o.__PAGE_NNModelView, '_modelKey'=model@key)
}

runPCA<-
function(tol = 0, standardize = TRUE, retx = FALSE) {
  data <- new("H2OParsedData", h2o = h, key = "parsed.hex", logic = TRUE)
  model <<- h2o.prcomp(data = data, tol = tol, standardize = standardize, retx = retx)
  model.json <<- .h2o.__remoteSend(h, .h2o.__PAGE_PCAModelView, '_modelKey'=model@key)
}

runRF.VA<-
function(x, y, ntree=50, depth=50, sample.rate=2/3, 
         classwt=NULL, nbins=100, seed=-1, use_non_local=TRUE) {
  data <- new("H2OParsedDataVA", h2o = h, key = "parsed.hex", logic = FALSE)
  model <<- h2o.randomForest.VA(x = x, y = y, data = data, depth = depth,
                                sample.rate = sample.rate, classwt = classwt,
                                nbins = nbins, seed = seed, use_non_local = use_non_local)
  model.json <<- .h2o.__remoteSend(h, .h2o.__PAGE_RFVIEW, model_key=model@key, data_key=data@key)
}

runRF.FV<-
function(x, y, ntree=50, depth=50, nodesize=1, 
         sample.rate=2/3, nbins=100, seed=-1) {
  data <- new("H2OParsedData", h2o = h, key = "parsed.hex", logic = TRUE)
  model <<- h2o.randomForest.FV(x = x, y = y, data = data, ntree = ntree,
                                depth = depth, nodesize = nodesize,
                                sample.rate = sample.rate, nbins = nbins, seed = seed)
  model.json <<- .h2o.__remoteSend(h, .h2o.__PAGE_DRFModelView, '_modelKey'= model@key)
}

#Scoring/Predicting
runGBMScore<-
function() {
  testData <<- new("H2OParsedData", h2o = h, key = "test.hex", logic = TRUE)
  .predict(model)
}

runGLMScore.VA<-
function() {
  testData <<- new("H2OParsedData", h2o = h, key = "test.hex", logic = TRUE)
  .predict(model)
}

runGLMScore.FV<-
function() {
  testData <<- new("H2OParsedData", h2o = h, key = "test.hex", logic = TRUE)
  .predict(model)
}

runRFScore.VA<-
function() {
  testData <<- new("H2OParsedData", h2o = h, key = "test.hex", logic = TRUE)
  .predict(model)
}

runRFScore.FV<-
function() {
  testData <<- new("H2OParsedData", h2o = h, key = "test.hex", logic = TRUE)
  .predict(model)
}

runNNScore<-
function() {
  testData <<- new("H2OParsedData", h2o = h, key = "test.hex", logic = TRUE)
  .predict(model)
}

.retrieveModel<-
function(modelType, datatype = "VA") {
  model_key <- h2o.ls(h)[1,1]
  return(.newModel(modelType, model_key, datatype))
}

.predict<-
function(model) {
  print("SPENCER!!!!")
  print(model)
  if( class(model)[1] == "H2OGLMModelVA") {
    res <- .h2o.__remoteSend(h, .h2o.__PAGE_PREDICT, model_key = model@key, data_key=testData@key, destination_key = "h2opreds.hex")
  } else {
    res <- .h2o.__remoteSend(h, .h2o.__PAGE_PREDICT2, model = model@key, data="test.hex", prediction = "h2opreds.hex")
  }
  h2opred <- new("H2OParsedData", h2o = h, key = "h2opreds.hex")
  if (predict_type == "binomial") 
    .calcBinomResults(h2opred)
  if (predict_type == "multinomial")
    .calcMultinomResults(h2opred)
  if (predict_type == "regression")
    .calcRegressionResults(h2opred)
}

.calcBinomResults<-
function(h2opred) {
  res <- .h2o.__remoteSend(h, .h2o.__PAGE_AUC, actual = testData@key,
                          vactual = response, predict = h2opred@key,
                          vpredict = response)

  print("RESULT!!")
  print(res)
  cm <- res$confusion_matrix_for_criteria[[2]]
  confusion_matrix    <<- t(matrix(unlist(cm), 2 ,2))
  precision           <<- confusion_matrix[1,1] / (confusion_matrix[1,1] + confusion_matrix[1,2])
  recall              <<- confusion_matrix[1,1] / (confusion_matrix[1,1] + confusion_matrix[2,1])
  error_rate          <<- -1 #confusion_matrix[3,3]
  minority_error_rate <<- -1 #confusion_matrix[2,3]
  auc                 <<- res$AUC
  cm.json             <<- cm
  levels.json         <<- res$actual_domain
}

.calcMultinomResults<-
function(h2opred) {
  res <- .h2o.__remoteSend(h, .h2o.__PAGE_CM, actual = "test.hex", 
                          vactual = response, predict = "h2opreds.hex",
                          vpredict = "predict")
  .buildcm2(res)
}

.calcRegressionResults<-
function(h2opred) {
  #TODO: Write out logic for calculating regression results....

  aic      <<- -1
  null_dev <<- -1
  res_dev  <<- -1
}

.calcPredictType<-
function() {
  if (.isNone(model)) {
    return(0)
  }
  print("PREDICT TYPE:")
  if (any(grepl("GLM", h2o.ls(h)))) {
    if(model@model$params$family[[1]] != "binomial") {
      cat("regression\n")
      predict_type <<- "regression"
      return(0)
    } else {
      cat("binomial\n")
      predict_type <<- "binomial"
      return(0)
    }   
  }
  if (any(grepl("GBM", h2o.ls(h)))) {
    if(model@model$params$distribution == "gaussian") {
      cat("regression")
      predict_type <<- "regression"
      return(0)
    } else {
      cat("multinomial\n")
      predict_type <<- "multinomial"
      return(0)
    }   
  }
  if (any(grepl("NeuralNet", h2o.ls(h)))) {
    cat("multinomial")
    predict_type <<- "multinomial"
    return(0)
  }
  if (any(grepl("RF", h2o.ls(h)))) {
    cat("multinomial\n")
    predict_type <<- "multinomial"
    return(0)
  }
  if (any(grepl("PCA", h2o.ls(h)) || grepl("KMeans", h2o.ls(h)))) {
    cat("no predict\n")
    predict_type <<- "no predict"
    return(0)
  }
}

.build_cm <- function(cm, actual_names = NULL, predict_names = actual_names, transpose = TRUE) {
  #browser()
  categories = length(cm)
  cf_matrix = matrix(unlist(cm), nrow=categories)
  if(transpose) cf_matrix = t(cf_matrix)

  cf_total = apply(cf_matrix, 2, sum)
  # cf_error = c(apply(cf_matrix, 1, sum)/diag(cf_matrix)-1, 1-sum(diag(cf_matrix))/sum(cf_matrix))
  cf_error = c(1-diag(cf_matrix)/apply(cf_matrix,1,sum), 1-sum(diag(cf_matrix))/sum(cf_matrix))
  cf_matrix = rbind(cf_matrix, cf_total)
  cf_matrix = cbind(cf_matrix, round(cf_error, 3)) 

  if(!is.null(actual_names))
    dimnames(cf_matrix) = list(Actual = c(actual_names, "Totals"), Predicted = c(predict_names, "Error"))
  return(cf_matrix)
}

.buildcm2<-
function(res) {
  cm <- res$cm
  #remove_nth <- length(res$response_domain) + 1
  cm <- .build_cm(cm) #[-remove_nth, -remove_nth]
  confusion_matrix <<- cm
  cm.json <<- res$cm
  levels.json <<- res$response_domain
}


.setup()
if(file.exists('.RData')) {
   load(".RData")
}
