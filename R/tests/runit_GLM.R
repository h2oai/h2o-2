# Test generalized linear models in H2O
# R -f runit_GLM.R --args H2OServer:Port
# By default, H2OServer = 127.0.0.1 and Port = 54321
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 1)
  stop("Usage: R -f runit_GLM.R --args H2OServer:Port")
if(length(args) == 0) {
  myIP = "127.0.0.1"
  myPort = 54321
} else {
  argsplit = strsplit(args[1], ":")[[1]]
  myIP = argsplit[1]
  myPort = as.numeric(argsplit[2])
}
defaultPath = "../../target/R"

# Check if H2O R wrapper package is installed
if(!"h2oWrapper" %in% rownames(installed.packages())) {
  envPath = Sys.getenv("H2OWrapperDir")
  wrapDir = ifelse(envPath == "", defaultPath, envPath)
  wrapName = list.files(wrapDir, pattern="h2oWrapper")[1]
  wrapPath = paste(wrapDir, wrapName, sep="/")
  
  if(!file.exists(wrapPath))
    stop(paste("h2oWrapper package does not exist at", wrapPath))
  print(paste("Installing h2oWrapper package from", wrapPath))
  install.packages(wrapPath, repos = NULL, type = "source")
}

# Check that H2O R package matches version on server
library(h2oWrapper)
h2oWrapper.installDepPkgs()      # Install R package dependencies
h2oWrapper.init(ip=myIP, port=myPort, startH2O=FALSE, silentUpgrade = TRUE)

# Load H2O R package and run test
if(!"RUnit" %in% rownames(installed.packages())) install.packages("RUnit")
if(!"glmnet" %in% rownames(installed.packages())) install.packages("glmnet")
if(!"gbm" %in% rownames(installed.packages())) install.packages("gbm")
library(RUnit)
library(glmnet)
library(gbm)
library(h2o)

if(Sys.info()['sysname'] == "Windows")
  options(RCurlOptions = list(cainfo = system.file("CurlSSL", "cacert.pem", package = "RCurl")))

#------------------------------ Begin Tests ------------------------------#
serverH2O = new("H2OClient", ip=myIP, port=myPort)
checkGLMModel <- function(myGLM.h2o, myGLM.r) {
  coeff.mat = as.matrix(myGLM.r$beta)
  numcol = ncol(coeff.mat)
  coeff.R = c(coeff.mat[,numcol], Intercept = as.numeric(myGLM.r$a0[numcol]))
  print(myGLM.h2o@model$coefficients)
  print(coeff.R)
  checkEqualsNumeric(myGLM.h2o@model$coefficients, coeff.R, tolerance = 0.5)
  
  checkEqualsNumeric(myGLM.h2o@model$null.deviance, myGLM.r$nulldev, tolerance = 0.5)
}

# Test GLM on benign.csv dataset
test.GLM.benign <- function(serverH2O) {
  cat("\nImporting benign.csv data...\n")
  # benign.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/benign.csv")
  # benign.hex = h2o.importFile(serverH2O, normalizePath("../../smalldata/logreg/benign.csv"))
  benign.hex = h2o.uploadFile(serverH2O, "../../smalldata/logreg/benign.csv")
  benign.sum = summary(benign.hex)
  print(benign.sum)
  
  # benign.data = read.csv(text = getURL("https://raw.github.com/0xdata/h2o/master/smalldata/logreg/benign.csv"), header = TRUE)
  benign.data = read.csv("../../smalldata/logreg/benign.csv", header = TRUE)
  benign.data = na.omit(benign.data)
  
  myY = "3"; myY.r = as.numeric(myY) + 1
  for(maxx in 10:13) {
    myX = 0:maxx
    myX = myX[which(myX != myY)]; myX.r = myX + 1
    myX = paste(myX, collapse=",")
    
    cat("\nH2O GLM (binomial) with parameters:\nX:", myX, "\nY:", myY, "\n")
    benign.glm.h2o = h2o.glm(y = myY, x = myX, data = benign.hex, family = "binomial", nfolds = 5, alpha = 0.5)
    print(benign.glm.h2o)
    
    # benign.glm = glm.fit(y = benign.data[,myY.r], x = benign.data[,myX.r], family = binomial)
    benign.glm = glmnet(y = benign.data[,myY.r], x = data.matrix(benign.data[,myX.r]), family = "binomial", alpha = 0.5)
    checkGLMModel(benign.glm.h2o, benign.glm)
  }
}

# Test GLM on prostate dataset
test.GLM.prostate <- function(serverH2O) {
  cat("\nImporting prostate.csv data...\n")
  # prostate.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", "prostate.hex")
  # prostate.hex = h2o.importFile(serverH2O, normalizePath("../../smalldata/logreg/prostate.csv"), "prostate.hex")
  prostate.hex = h2o.uploadFile(serverH2O, "../../smalldata/logreg/prostate.csv", "prostate.hex")
  prostate.sum = summary(prostate.hex)
  print(prostate.sum)
  
  # prostate.data = read.csv(text = getURL("https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv"), header = TRUE)
  prostate.data = read.csv("../../smalldata/logreg/prostate.csv", header = TRUE)
  prostate.data = na.omit(prostate.data)
  
  myY = "1"; myY.r = as.numeric(myY) + 1
  for(maxx in 3:8) {
    myX = 1:maxx
    myX = myX[which(myX != myY)]; myX.r = myX + 1
    myX = paste(myX, collapse=",")
    
    cat("\nH2O GLM (binomial) with parameters:\nX:", myX, "\nY:", myY, "\n")
    prostate.glm.h2o = h2o.glm(y = myY, x = myX, data = prostate.hex, family = "binomial", nfolds = 10, alpha = 0.5)
    print(prostate.glm.h2o)
    
    # prostate.glm = glm.fit(y = prostate.data[,myY.r], x = prostate.data[,myX.r], family = binomial)
    prostate.glm = glmnet(y = prostate.data[,myY.r], x = data.matrix(prostate.data[,myX.r]), family = "binomial", alpha = 0.5)
    checkGLMModel(prostate.glm.h2o, prostate.glm)
  }
}

# Test GLM on covtype (20k) dataset
test.GLM.covtype <- function(serverH2O) {
  cat("\nImporting covtype.20k.data...\n")
  # covtype.hex = h2o.importFile(serverH2O, "../../UCI/UCI-large/covtype/covtype.data")
  # covtype.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/covtype/covtype.20k.data")
  # covtype.hex = h2o.importFile(serverH2O, normalizePath("../../smalldata/covtype/covtype.20k.data"))
  covtype.hex = h2o.uploadFile(serverH2O, "../../smalldata/covtype/covtype.20k.data")
  covtype.sum = summary(covtype.hex)
  print(covtype.sum)
  
  myY = "54"
  myX = ""
  # max_iter = 8
  
  # L2: alpha = 0, lambda = 0
  start = Sys.time()
  covtype.h2o1 = h2o.glm(y = myY, x = myX, data = covtype.hex, family = "binomial", nfolds = 2, alpha = 0, lambda = 0)
  end = Sys.time()
  cat("\nGLM (L2) on", covtype.hex@key, "took", as.numeric(end-start), "seconds\n")
  print(covtype.h2o1)
  
  # Elastic: alpha = 0.5, lambda = 1e-4
  start = Sys.time()
  covtype.h2o2 = h2o.glm(y = myY, x = myX, data = covtype.hex, family = "binomial", nfolds = 2, alpha = 0.5, lambda = 1e-4)
  end = Sys.time()
  cat("\nGLM (Elastic) on", covtype.hex@key, "took", as.numeric(end-start), "seconds\n")
  print(covtype.h2o2)
  
  # L1: alpha = 1, lambda = 1e-4
  start = Sys.time()
  covtype.h2o3 = h2o.glm(y = myY, x = myX, data = covtype.hex, family = "binomial", nfolds = 2, alpha = 1, lambda = 1e-4)
  end = Sys.time()
  cat("\nGLM (L1) on", covtype.hex@key, "took", as.numeric(end-start), "seconds\n")
  print(covtype.h2o3)
}

test.GLM.benign(serverH2O)
test.GLM.prostate(serverH2O)
test.GLM.covtype(serverH2O)