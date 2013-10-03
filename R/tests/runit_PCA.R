# Test principal components analysis in H2O
# R -f runit_PCA.R --args H2OServer:Port
# By default, H2OServer = 127.0.0.1 and Port = 54321
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 1)
  stop("Usage: R -f runit_PCA.R --args H2OServer:Port")
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
checkPCAModel <- function(myPCA.h2o, myPCA.r, toleq = 1e-5) {
  checkEqualsNumeric(myPCA.h2o@model$sdev, myPCA.r$sdev)
  
  ncomp = length(colnames(myPCA.h2o@model$rotation))
  myPCA.h2o@model$rotation = apply(myPCA.h2o@model$rotation, 2, as.numeric)
  flipped = abs(myPCA.h2o@model$rotation[1,] + myPCA.r$rotation[1,]) <= toleq
  for(i in 1:ncomp) {
    if(flipped[i])
      checkEqualsNumeric(myPCA.h2o@model$rotation[,i], -myPCA.r$rotation[,i])
    else
      checkEqualsNumeric(myPCA.h2o@model$rotation[,i], myPCA.r$rotation[,i])
  }
}

test.PCA.arrests <- function(serverH2O) {
  cat("\nImporting USArrests.csv data...\n")
  # arrests.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/pca_test/USArrests.csv")
  # arrests.hex = h2o.importFile(serverH2O, normalizePath("../../smalldata/pca_test/USArrests.csv"))
  arrests.hex = h2o.uploadFile(serverH2O, "../../smalldata/pca_test/USArrests.csv", "arrests.hex")
  arrests.sum = summary(arrests.hex)
  print(arrests.sum)
  
  cat("\nH2O PCA on non-standardized USArrests:\n")
  arrests.pca.h2o = h2o.prcomp(arrests.hex, standardize = FALSE)
  print(arrests.pca.h2o)
  arrests.pca = prcomp(USArrests, center = FALSE, scale. = FALSE, retx = TRUE)
  checkPCAModel(arrests.pca.h2o, arrests.pca)
  
  cat("\nH2O PCA on standardized USArrests:\n")
  arrests.pca.h2o.std = h2o.prcomp(arrests.hex, standardize = TRUE)
  print(arrests.pca.h2o.std)
  arrests.pca.std = prcomp(USArrests, center = TRUE, scale. = TRUE, retx = TRUE)
  checkPCAModel(arrests.pca.h2o.std, arrests.pca.std)
}

test.PCA.australia <- function(serverH2O) {
  cat("\nImporting AustraliaCoast.csv data...\n")
  australia.data = read.csv("../../smalldata/pca_test/AustraliaCoast.csv", header = TRUE)
  # australia.hex = h2o.importFile(serverH2O, normalizePath("../../smalldata/pca_test/AustraliaCoast.csv"))
  australia.hex = h2o.uploadFile(serverH2O, "../../smalldata/pca_test/AustraliaCoast.csv")
  australia.sum = summary(australia.hex)
  print(australia.sum)
  
  cat("\nH2O PCA on non-standardized Australia coastline data:\n")
  australia.pca.h2o = h2o.prcomp(australia.hex, standardize = FALSE)
  print(australia.pca.h2o)
  australia.pca = prcomp(australia.data, center = FALSE, scale. = FALSE, retx = TRUE)
  checkPCAModel(australia.pca.h2o, australia.pca)
  
  cat("\nH2O PCA on standardized Australia coastline data:\n")
  australia.pca.h2o.std = h2o.prcomp(australia.hex, standardize = TRUE)
  print(australia.pca.h2o.std)
  australia.pca.std = prcomp(australia.data, center = TRUE, scale. = TRUE, retx = TRUE)
  checkPCAModel(australia.pca.h2o.std, australia.pca.std)
}

test.PCA.arrests(serverH2O)
test.PCA.australia(serverH2O)