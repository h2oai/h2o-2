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
source("../../../../../R/h2o-package/R/models.R")



#GLOBALS
aic               <<- "None"
auc               <<- "None"
.binomial         <<- "None"
cm.json           <<- "None"
confusion_matrix  <<- "None"
correct_pass      <<- 1 #1 unless otherwise over written by Predict.R
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
multinom_errors   <<- "None"
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
#  if (!"h2o" %in% rownames(installed.packages())) {
#      envPath  <- Sys.getenv("H2OWrapperDir")
#      wrapDir  <- ifelse(envPath == "", defaultPath, envPath)
#      wrapName <- list.files(wrapDir, pattern  = "h2o")[1]
#      wrapPath <- paste(wrapDir, wrapName, sep = "/")
#
#      if (!file.exists(wrapPath))
#        stop(paste("h2o package does not exist at", wrapPath));
#      print(paste("Installing h2o package from", wrapPath))
#      .installDepPkgs()
#      install.packages(wrapPath, repos = NULL, type = "source")
#    }
 
  .installDepPkgs()
  library(h2o)
  h <<- h2o.init(ip            = IP,
                port           = PORT,
                startH2O       = FALSE)
}

#"Public" Methods

#check that two vectors are approximately equal
checkEquals<-
function(v1, v2) {
    print("CHECKING RESULTS")
    print("v1: ")
    print(v1)
    print("v2: ")
    print(v2)

    #if v1 < v2 then it's OK
    if (all(v1 < v2)) {
      correct_pass <<- 1
      return(0)
    }
    new_v1 <- v1[v1>v2]
    new_v2 <- v2[v1>v2]
    DIFFERENCE <- 0.1
    v <- abs(v1-v2)
    if(sum(v > DIFFERENCE) > 0) {
      correct_pass <<- 0
    }
}

#Import/Parsing
upload<-
function(pkey, dataPath) {
  h2o.uploadFile(h, dataPath, key = pkey)
}

import<-
function(pkey, dataPath) {
  h2o.importFile(h, dataPath, key = pkey)
}

hdfs<-
function(pkey, dataPath) {
  h2o.importHDFS(h, dataPath, key = pkey)
}

#Modeling
runSummary<-
function() {
  data <- h2o.getFrame(h2o = h, key = "parsed.hex") 
  summary(data)
}

runH2o.ddply<-
function(.variables, .fun = NULL, ..., .progress = 'none') {
  data <- h2o.getFrame(h2o = h, key = "parsed.hex")
  h2o.ddply(data, .variables, .fun, ..., .progress)
}

runGBM<-
function(x, y, distribution='multinomial', nfolds = 0,
         n.trees=10, interaction.depth=5, 
         n.minobsinnode=10, shrinkage=0.02, 
         n.bins=100) {
  data <- h2o.getFrame(h2o = h, key = "parsed.hex")
  model <<- h2o.gbm(x = x, y = y, distribution = distribution, data = data, n.trees = n.trees,
          interaction.depth = interaction.depth, n.minobsinnode = n.minobsinnode,
          shrinkage = shrinkage, n.bins = n.bins, nfolds = nfolds)
  model.json <<- .h2o.__remoteSend(h, .h2o.__PAGE_GBMModelView, '_modelKey' = model@key)
}

runGLM<-
function(x, y, family, nfolds = 10, alpha = 0.5, lambda = 1.0e-5, epsilon = 1.0e-5, 
         standardize = TRUE, tweedie.p = ifelse(family == "tweedie", 0, as.numeric(NA))) {
  data <- h2o.getFrame(h2o = h, key = "parsed.hex")
  model <<- h2o.glm(x = x, y = y, data = data, family = family, nfolds = nfolds, alpha = alpha,
                       lambda = lambda, epsilon = epsilon, standardize = standardize, tweedie.p = tweedie.p)
  model.json <<- .h2o.__remoteSend(h, .h2o.__PAGE_GLMModelView, '_modelKey'=model@key)
}

runKMeans<-
function(centers, cols = '', iter.max = 10, normalize = FALSE) {
  data <- h2o.getFrame(h2o = h, key = "parsed.hex")
  model <<- h2o.kmeans(data = data, centers = centers, cols = cols, iter.max = iter.max, normalize = normalize)
  model.json <<- .h2o.__remoteSend(h, .h2o.__PAGE_KM2ModelView, model = model@key)
  kmeans_k <<- dim(model@model$centers)[1]
  kmeans_withinss <<- model@model$tot.withinss
}

runPCA<-
function(tol = 0, standardize = TRUE, retx = FALSE) {
  data <- h2o.getFrame(h2o = h, key = "parsed.hex")
  model <<- h2o.prcomp(data = data, tol = tol, standardize = standardize, retx = retx)
  model.json <<- .h2o.__remoteSend(h, .h2o.__PAGE_PCAModelView, '_modelKey'=model@key)
}

runRF<-
function(x, y, ntree=50, depth=50, nodesize=1, nfolds = 0,
         sample.rate=2/3, nbins=100, seed=-1, mtries = -1, type="fast",...) {
  data <- h2o.getFrame(h2o = h, key = "parsed.hex")
  model <<- h2o.randomForest(x = x, y = y, data = data, ntree = ntree, nfolds = nfolds, mtries = mtries,
                                depth = depth, nodesize = nodesize,
                                sample.rate = sample.rate, nbins = nbins, seed = seed, type = type, ...)
  page <- .h2o.__PAGE_SpeeDRFModelView
  if (type != "fast") page <- .h2o.__PAGE_DRFModelView
  model.json <<- .h2o.__remoteSend(h, page, '_modelKey'= model@key)
}

runSRF<-
function(x, y, classification=TRUE, nfolds=0,
          mtry=-1, ntree=50, depth=50, sample.rate=2/3,
          oobee = TRUE,importance = FALSE,nbins=1024, seed=-1,
          stat.type="ENTROPY",balance.classes=FALSE) {

  data <- h2o.getFrame(h2o = h, key = "parsed.hex")
  model <<- h2o.SpeeDRF(x = x, y = y, data = data, ntree = ntree, depth = depth, nbins = nbins, sample.rate = sample.rate, nfolds = nfolds, mtry=mtry,
                        oobee = oobee, importance = importance, seed = seed, stat.type = stat.type, balance.classes = balance.classes)

}

runDL<-
function(x, y, activation="RectifierWithDropout", 
hidden=c(1024,1024,2048), 
nfolds = 0,
epochs=32, 
train_samples_per_iteration=-1, 
adaptive_rate=TRUE, 
rho=0.99, 
epsilon=1E-6, 
rate = 0.01, 
rate_annealing=1E-6,
rate_decay=1.0, 
momentum_start=0.0, 
momentum_ramp=1000000, 
momentum_stable=0.0, 
nesterov_accelerated_gradient=TRUE, 
input_dropout_ratio=0.0, 
l1 = 1E-5, 
l2 = 0.0, 
initial_weight_distribution="UniformAdaptive", 
initial_weight_scale=1.0, 
loss="CrossEntropy", 
score_interval=5.0, 
score_training_samples=10000, 
score_duty_cycle=0.1, 
quiet_mode=FALSE, 
max_confusion_matrix_size=20, 
max_hit_ratio_k=10, 
balance_classes=FALSE, 
max_after_balance_size=5.0, 
score_validation_sampling="Uniform", 
diagnostics=TRUE, 
variable_importances=FALSE, 
fast_mode=TRUE, 
ignore_const_cols=TRUE, 
force_load_balance=TRUE, 
replicate_training_data=TRUE, 
single_node_mode=FALSE,
shuffle_training_data=FALSE, ...) {
  data <- h2o.getFrame(h2o = h, key = "parsed.hex") 
  model <<- h2o.deeplearning(x = x, y = y, data = data, nfolds = nfolds,
      activation=activation,
      hidden=hidden,
      epochs=epochs,
      train_samples_per_iteration=train_samples_per_iteration,
      adaptive_rate=adaptive_rate,
      rho=rho,
      epsilon=epsilon,
      rate=rate,
      rate_annealing=rate_annealing,
      rate_decay=rate_decay,
      momentum_start=momentum_start,
      momentum_ramp=momentum_ramp,
      momentum_stable=momentum_stable,
      nesterov_accelerated_gradient=nesterov_accelerated_gradient,
      input_dropout_ratio=input_dropout_ratio,
      l1=l1,
      l2=l2,
      initial_weight_distribution=initial_weight_distribution,
      initial_weight_scale=initial_weight_scale,
      loss=loss,
      score_interval=score_interval,
      score_training_samples=score_training_samples,
      score_duty_cycle=score_duty_cycle,
      quiet_mode=quiet_mode,
      max_confusion_matrix_size=max_confusion_matrix_size,
      max_hit_ratio_k=max_hit_ratio_k,
      balance_classes=balance_classes,
      max_after_balance_size=max_after_balance_size,
      score_validation_sampling=score_validation_sampling,
      diagnostics=diagnostics,
      variable_importances=variable_importances,
      fast_mode=fast_mode,
      ignore_const_cols=ignore_const_cols,
      force_load_balance=force_load_balance,
      replicate_training_data=replicate_training_data,
      single_node_mode=single_node_mode,
      shuffle_training_data=shuffle_training_data, ...)
  
  model.json <<- .h2o.__remoteSend(h, .h2o.__PAGE_DeepLearningModelView, '_modelKey'= model@key)
}

#Scoring/Predicting
runGBMScore<-
function(expected_results=NULL, type=NULL) {
  testData <<- h2o.getFrame(h2o = h, key = "test.hex") 
  .predict(model)
  if (!is.null(expected_results)) {
    if (type == "cm") {
      rr <- confusion_matrix[,dim(confusion_matrix)[2]]
      rr <- data.frame(rr)[,1]
      rr <- rr[1:(length(rr) - 2)] # -2 becuase the last row is totals, and the penultimate row is bogus fill by R
      checkEquals(rr, expected_results)
    }   
  }
}

runGLMScore<-
function() {
  testData <<- h2o.getFrame(h2o = h, key = "test.hex") 
  .predict(model)
}

runRFScore<-
function(expected_results=NULL, type=NULL) {
  testData <<- h2o.getFrame(h2o = h, key = "test.hex") 
  .predict(model)
  if (!is.null(expected_results)) {
    if (type == "cm") {
      rr <- confusion_matrix[,dim(confusion_matrix)[2]]
      rr <- data.frame(rr)[,1]
      rr <- rr[1:(length(rr) - 2)] # -2 becuase the last row is totals, and the penultimate row is bogus fill by R
      checkEquals(rr, expected_results)
    }
  }
}

runDLScore<-
function(expected_results=NULL, type=NULL) {
  testData <<- h2o.getFrame(h2o = h, key = "test.hex") 
  .predict(model)
  if (!is.null(expected_results)) {
    if (type == "cm") {
      print("CONFUSION MATRIX DATA")
      print(confusion_matrix)
      print(dim(confusion_matrix))
      rr <- confusion_matrix[,dim(confusion_matrix)[2]]
      rr <- data.frame(rr)[,1]
      rr <- rr[1:(length(rr) - 2)] # -2 becuase the last row is totals, and the penultimate row is bogus fill by R
      checkEquals(rr, expected_results)
    }   
  }
}

.retrieveModel<-
function(modelType) {
  model_key <- h2o.ls(h)[1,1]
  return(.newModel(modelType, model_key, datatype))
}

.predict<-
function(model) {
  print(model)
  res <- .h2o.__remoteSend(h, .h2o.__PAGE_PREDICT2, model = model@key, data="test.hex", prediction = "h2opreds.hex")
  h2opred <- h2o.getFrame(h2o = h, key = "h2opreds.hex")  
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
  multinom_errors <<- diag(as.matrix(confusion_matrix)) / rowSums(confusion_matrix)
}


.setup()
if(file.exists('.RData')) {
   load(".RData")
}
