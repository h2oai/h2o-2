# Test k-means clustering in H2O
# R -f runit_kmeans.R --args H2OServer:Port
# By default, H2OServer = 127.0.0.1 and Port = 54321
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 1)
  stop("Usage: R -f runit_kmeans.R --args H2OServer:Port")
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
checkKMModel <- function(myKM.h2o, myKM.r) {
  # checkEqualsNumeric(myKM.h2o@model$size, myKM.r$size, tolerance = 1.0)
  # checkEqualsNumeric(myKM.h2o@model$withinss, myKM.r$withinss, tolerance = 1.5)
  # checkEqualsNumeric(myKM.h2o@model$tot.withinss, myKM.r$tot.withinss, tolerance = 1.5)
  # checkEqualsNumeric(myKM.h2o@model$centers, myKM.r$centers, tolerance = 0.5)
}

# Test k-means clustering on benign.csv
test.km.benign <- function(serverH2O) {
  cat("\nImporting benign.csv data...\n")
  benign.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/benign.csv")
  benign.sum = summary(benign.hex)
  print(benign.sum)
  
  benign.data = read.csv(text = getURL("https://raw.github.com/0xdata/h2o/master/smalldata/logreg/benign.csv"), header = TRUE)
  benign.data = na.omit(benign.data)
  
  for(i in 1:5) {
    cat("\nH2O K-Means with", i, "clusters:\n")
    benign.km.h2o = h2o.kmeans(data = benign.hex, centers = i)
    print(benign.km.h2o)
    benign.km = kmeans(benign.data, centers = i)
    checkKMModel(benign.km.h2o, benign.km)
  }
}

# Test k-means clustering on prostate.csv
test.km.prostate <- function(serverH2O) {
  cat("\nImporting prostate.csv data...\n")
  prostate.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", "prostate.hex")
  prostate.sum = summary(prostate.hex)
  print(prostate.sum)
  
  prostate.data = read.csv(text = getURL("https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv"), header = TRUE)
  prostate.data = na.omit(prostate.data)
  
  for(i in 5:8) {
    cat("\nH2O K-Means with", i, "clusters:\n")
    prostate.km.h2o = h2o.kmeans(data = prostate.hex, centers = i, cols = "2")
    print(prostate.km.h2o)
    prostate.km = kmeans(prostate.data[,3], centers = i)
    checkKMModel(prostate.km.h2o, prostate.km)
  }
}

test.km.benign(serverH2O)
test.km.prostate(serverH2O)