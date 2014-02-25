options(echo=FALSE)

read.zip<- 
function(zipfile, exdir,header=T) {
  zipdir <- exdir
  unzip(zipfile, exdir=zipdir)
  files <- list.files(zipdir)
  file <- paste(zipdir, files[1], sep="/")
  read.csv(file,header=header)
}

remove_exdir<- 
function(exdir) {
  exec <- paste("rm -r ", exdir, sep="")
  system(exec)
}

sandbox<-
function() {
  unlink("./Rsandbox", TRUE)
  dir.create("./Rsandbox")
  h2o.__LOG_COMMAND <- "./Rsandbox/"
  h2o.__LOG_ERROR   <- "./Rsandbox/"
  h2o.__changeCommandLog(normalizePath(h2o.__LOG_COMMAND))
  h2o.__changeErrorLog(normalizePath(h2o.__LOG_ERROR))
  h2o.__startLogging()
}

Log.info<-
function(m) {
  message <- paste("[INFO]: ",m, sep="")
  logging(message)
}

Log.warn<-
function(m) {
  logging(paste("[WARN] : ",m,sep=""))
  #temp <- strsplit(as.character(Sys.time()), " ")[[1]]
  #m <- paste('[',temp[1], ' ',temp[2],']', '\t', m)
  h2o.__logIt("[WARN] :", m, "Error")
  traceback()
}

Log.err<-
function(m) {
  logging(paste("[ERROR] : ",m,sep=""))
  logging("[ERROR] : TEST FAILED")
  #temp <- strsplit(as.character(Sys.time()), " ")[[1]]
  #m <- paste('[',temp[1], ' ',temp[2],']', '\t', m)
  h2o.__logIt("[ERROR] :", m, "Error")
  traceback()
  q("no",1,FALSE) #exit with nonzero exit code
}

logging<- 
function(m) {
  cat(sprintf("[%s] %s\n", Sys.time(),m))
}

PASS<- 
function() {
  cat("######     #     #####   #####  \n")
  cat("#     #   # #   #     # #     # \n")
  cat("#     #  #   #  #       #       \n")
  cat("######  #     #  #####   #####  \n")
  cat("#       #######       #       # \n")
  cat("#       #     # #     # #     # \n")
  cat("#       #     #  #####   #####  \n")

  Log.info("TEST PASSED")
  q("no",0,FALSE)
}

FAIL<-
function(e) {
  cat("")
  cat("########    ###    #### ##       \n")
  cat("##         ## ##    ##  ##       \n")
  cat("##        ##   ##   ##  ##       \n")
  cat("######   ##     ##  ##  ##       \n")
  cat("##       #########  ##  ##       \n")
  cat("##       ##     ##  ##  ##       \n")
  cat("##       ##     ## #### ######## \n")
  
  Log.err(e)
}

WARN<-
function(w) {
  Log.warn(w)
}

get_args<-
function(args) {
  fileName <- commandArgs()[grep('*\\.R',unlist(commandArgs()))]
  if (length(args) > 1) {
    m <- paste("Usage: R -f ", paste(fileName, " --args H2OServer:Port",sep=""),sep="")
    stop(m);
  }

  if (length(args) == 0) {
    myIP   = "127.0.0.1"
    myPort = 54321
  } else {
    argsplit = strsplit(args[1], ":")[[1]]
    myIP     = argsplit[1]
    myPort   = as.numeric(argsplit[2])
  }
  return(list(myIP,myPort));
}

checkNLoadWrapper<-
function(ipPort) {
  Log.info("Check if H2O R wrapper package is installed\n")
  if (!"h2o" %in% rownames(installed.packages())) {
    envPath  = Sys.getenv("H2OWrapperDir")
    wrapDir  = ifelse(envPath == "", defaultPath, envPath)
    wrapName = list.files(wrapDir, pattern  = "h2o")[1]
    wrapPath = paste(wrapDir, wrapName, sep = "/")
    
    if (!file.exists(wrapPath))
      stop(paste("h2o package does not exist at", wrapPath));
    print(paste("Installing h2o package from", wrapPath))
    # install.packages(wrapPath, repos = NULL, type = "source")
	install.packages("h2o", repos = c(H2O = paste("file://", wrapDir, sep=""), getOption("repos")))
  }

  Log.info("Check that H2O R package matches version on server\n")
  library(h2o)
  h2o.installDepPkgs()      # Install R package dependencies
  h2o.init(ip            = ipPort[[1]], 
           port          = ipPort[[2]], 
           startH2O      = FALSE)
  #source("../../h2o-package/R/Algorithms.R")
  #source("../../h2o-package/R/Classes.R")
  #source("../../h2o-package/R/ParseImport.R")
  #source("../../h2o-package/R/Internal.R")
  #sandbox()
}

checkNLoadPackages<-
function() {
  Log.info("Checking Package dependencies for this test.\n")
  if (!"RUnit"    %in% rownames(installed.packages())) install.packages("RUnit")
  if (!"testthat" %in% rownames(installed.packages())) install.packages("testthat")
  
  if (Sys.info()['sysname'] == "Windows")
    options(RCurlOptions = list(cainfo = system.file("CurlSSL", "cacert.pem", package = "RCurl")))

  Log.info("Loading RUnit and testthat\n")
  require(RUnit)
  require(testthat)
}

Log.info("============== Setting up R-Unit environment... ================")
defaultPath <- normalizePath("../../target/R")
ipPort <- get_args(commandArgs(trailingOnly = TRUE))
checkNLoadWrapper(ipPort)
checkNLoadPackages()

h2o.removeAll <-
function(object) {
  Log.info("Throwing away any keys on the H2O cluster")
  h2o.__remoteSend(object, h2o.__PAGE_REMOVEALL)
}

Log.info("Loading other required test packages")
if(!"glmnet" %in% rownames(installed.packages())) install.packages("glmnet")
if(!"gbm"    %in% rownames(installed.packages())) install.packages("gbm")
require(glmnet)
require(gbm)

myIP   <- ipPort[[1]]
myPort <- ipPort[[2]]
h2o.removeAll(new("H2OClient", ip=myIP, port=myPort))
