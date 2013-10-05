if(!"testthat" %in% rownames(installed.packages())) install.packages("testthat")
library(testthat)
context("GBM Test") #set context for test. Here we are checking GBM
# Test gradient boosting machines in H2O
# R -f runit_GBM_args.R --args H2OServer:Port
# By default, H2OServer = 127.0.0.1 and Port = 54321
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 1)
  stop("Usage: R -f runit_GBM_args.R --args H2OServer:Port")
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
grabRemote <- function(myURL, myFile) {
  temp <- tempfile()
  download.file(myURL, temp, method = "curl")
  aap.file <- read.csv(file = unz(description = temp, filename = myFile), as.is = TRUE)
  unlink(temp)
  return(aap.file)
}


check.params <- function() {
  #current allowed params as of 10/04/2013 5:13PM PST: 
  cat("\nParameters of H2O GBM as of 10/04/2013\n")
  x <- c("x", "y", "data", "n.trees", "interaction.depth", "n.minobsinnode", "shrinkage")
  print(x)
  #grab params from newest build:
  cat("\nParameters of current build: \n")
  y <- h2o.gbm@signature
  print(y)
  expect_that(length(x), equals(length(y)))
  cat("\nExpect that all of the parameters in the old set are in the new set and vice-versa: ")
  print(x)
  print(y)
  print(x %in% y)
  print(y %in% x)
  expect_that(length(x), equals(sum(x %in% y)))
  expect_that(length(y), equals(sum(y %in% x)))
  cat("End of test.")
}

test_that("gbm params test", check.params)

