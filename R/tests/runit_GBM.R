# Test gradient boosting machines in H2O
# R -f runit_GBM.R --args H2OServer:Port
# By default, H2OServer = 127.0.0.1 and Port = 54321
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 1)
  stop("Usage: R -f runit_GBM.R --args H2OServer:Port")
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
grabRemote <- function(myURL, myFile) {
  temp <- tempfile()
  download.file(myURL, temp, method = "curl")
  aap.file <- read.csv(file = unz(description = temp, filename = myFile), as.is = TRUE)
  unlink(temp)
  return(aap.file)
}

checkGBMModel <- function(myGBM.h2o, myGBM.r) {
  # Check GBM model against R
}

test.GBM.ecology <- function(serverH2O) {
  cat("\nImporting ecology_model.csv data...\n")
  # ecology.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/gbm_test/ecology_model.csv")
  ecology.hex = h2o.importFile(serverH2O, normalizePath("../../smalldata/gbm_test/ecology_model.csv"))
  ecology.sum = summary(ecology.hex)
  print(ecology.sum)
  
  # ecology.data = read.csv(text = getURL("https://raw.github.com/0xdata/h2o/master/smalldata/gbm_test/ecology_model.csv"), header = TRUE)
  ecology.data = read.csv("../../smalldata/gbm_test/ecology_model.csv", header = TRUE)
  ecology.data = na.omit(ecology.data)
  
  cat("\nH2O GBM with parameters:\nntrees = 100, max_depth = 5, min_rows = 10, learn_rate = 0.1\n")
  ecology.h2o = h2o.gbm(ecology.hex, destination = "ecology.gbm", y = "Site", ntrees = 100, max_depth = 5, min_rows = 10, learn_rate = 0.1)
  print(ecology.h2o)
  
  allyears.gbm = gbm(Site ~ ., data = ecology.data, distribution = "gaussian", n.trees = 100, interaction.depth = 5, n.minobsinnode = 10, shrinkage = 0.1)
  checkGBMModel(allyears.h2o, allyears.gbm)
}

test.GBM.airlines <- function() {
  # allyears.data = grabRemote("https://raw.github.com/0xdata/h2o/master/smalldata/airlines/allyears2k.zip", "ecology.csv")
  # allyears.data = na.omit(allyears.data)
  # allyears.data = data.frame(rapply(allyears.data, as.factor, classes = "character", how = "replace"))
  
  # allCol = colnames(allyears.data)
  # allXCol = allCol[-which(allCol == "IsArrDelayed")]
  # ignoreFeat = c("CRSDepTime", "CRSArrTime", "ActualElapsedTime", "CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "TaxiIn", "TaxiOut", "Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")
  # ignoreNum = sapply(ignoreFeat, function(x) { which(allXCol == x) })
  
  # allyears.hex = h2o.importFile(serverH2O, "../../smalldata/airlines/allyears2k.zip")
  # allyears.h2o = h2o.gbm(allyears.hex, destination = "allyears.gbm", y = "IsArrDelayed", x_ignore = ignoreNum, ntrees = 100, max_depth = 5, min_rows = 10, learn_rate = 0.1)
  
  # allyears.x = allyears.data[,-which(allCol == "IsArrDelayed")]
  # allyears.x = subset(allyears.x, select = -ignoreNum)
  # allyears.gbm = gbm.fit(y = allyears.data$IsArrDelayed, x = allyears.x, distribution = "bernoulli", n.trees = 100, interaction.depth = 5, n.minobsinnode = 10, shrinkage = 0.1)
}

test.GBM.ecology(serverH2O)