# Test random forest (classification) in H2O
# R -f runit_RF.R --args H2OServer:Port
# By default, H2OServer = 127.0.0.1 and Port = 54321
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 1)
  stop("Usage: R -f runit_RF.R --args H2OServer:Port")
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

test.RF.iris_class <- function(serverH2O) {
  serverH2O = new("H2OClient", ip=myIP, port=myPort)
  # iris.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/iris/iris22.csv", "iris.hex")
  # iris.hex = h2o.importFile(serverH2O, normalizePath("../../smalldata/iris/iris22.csv"), "iris.hex")
  iris.hex = h2o.uploadFile(serverH2O, "../../smalldata/iris/iris22.csv", "iris.hex")
  iris.rf = h2o.randomForest(y = 5, x = seq(1,4), data = iris.hex, ntree = 50, depth = 100, classwt = c("Iris-versicolor"=20.0, "Iris-virginica"=30.0))
  print(iris.rf)
  iris.rf = h2o.randomForest(y = 6, x = seq(1,4), data = iris.hex, ntree = 50, depth = 100 )
  print(iris.rf)
}

test.RF.iris_ignore <- function(serverH2O) {
  serverH2O = new("H2OClient", ip=myIP, port=myPort)
  # iris.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/iris/iris22.csv", "iris.hex")
  # iris.hex = h2o.importFile(serverH2O, normalizePath("../../smalldata/iris/iris22.csv"), "iris.hex")
  iris.hex = h2o.uploadFile(serverH2O, "../../smalldata/iris/iris22.csv", "iris.hex")
  h2o.randomForest(y = 5, x = seq(1,4), data = iris.hex, ntree = 50, depth = 100)
  for(maxx in 1:4) {
    # myIgnore = as.character(seq(0, maxx))
    myX = seq(1, maxx)
    iris.rf = h2o.randomForest(y = 5, x = myX, data = iris.hex, ntree = 50, depth = 100)
    print(iris.rf)
  }
}

test.RF.iris_class(serverH2O)
test.RF.iris_ignore(serverH2O)