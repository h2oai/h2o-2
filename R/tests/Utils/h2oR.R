options(echo=FALSE)
local({r <- getOption("repos"); r["CRAN"] <- "http://cran.us.r-project.org"; options(repos = r)})

grabRemote <- function(myURL, myFile) {
  temp <- tempfile()
  download.file(myURL, temp, method = "curl")
  aap.file <- read.csv(file = unz(description = temp, filename = myFile), as.is = TRUE)
  unlink(temp)
  return(aap.file)
}

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
  seedm <- paste("SEED used: ", SEED, sep = "")
  m <- paste(m, "\n", seedm, "\n", sep = "")
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

PASS_BANNER<-
function() {
  cat("")
  cat("######     #     #####   #####  \n")
  cat("#     #   # #   #     # #     # \n")
  cat("#     #  #   #  #       #       \n")
  cat("######  #     #  #####   #####  \n")
  cat("#       #######       #       # \n")
  cat("#       #     # #     # #     # \n")
  cat("#       #     #  #####   #####  \n")  
}

PASS<- 
function() {
  PASS_BANNER()
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

testEnd<-
function() {
    Log.info("End of test.")
    PASSS <<- TRUE
}


withWarnings <- function(expr) {
    myWarnings <- NULL
    wHandler <- function(w) {
        myWarnings <<- c(myWarnings, list(w))
        invokeRestart("muffleWarning")
    }
    val <- withCallingHandlers(expr, warning = wHandler)
    list(value = val, warnings = myWarnings)
    for(w in myWarnings) WARN(w)
}

doTest<-
function(testDesc, test) {
    Log.info("======================== Begin Test ===========================\n")
    conn <- new("H2OClient", ip=myIP, port=myPort)
    tryCatch(test_that(testDesc, withWarnings(test(conn))), warning = function(w) WARN(w), error =function(e) FAIL(e))
    if (!PASSS) FAIL("Did not reach the end of test. Check Rsandbox/errors.log for warnings and errors.")
    PASS()
}

installDepPkgs <- function(optional = FALSE) {
  myPackages = rownames(installed.packages())
  myReqPkgs = c("RCurl", "rjson", "tools", "statmod")
  
  # For plotting clusters in h2o.kmeans demo
  if(optional)
    myReqPkgs = c(myReqPkgs, "fpc", "cluster")
  
  # For communicating with H2O via REST API
  temp = lapply(myReqPkgs, function(x) { if(!x %in% myPackages) install.packages(x) })
  temp = lapply(myReqPkgs, require, character.only = TRUE)
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
    installDepPkgs()
    install.packages(wrapPath, repos = NULL, type = "source")
  }

  Log.info("Check that H2O R package matches version on server\n")
  installDepPkgs()
  library(h2o)
  h2o.init(ip            = ipPort[[1]], 
           port          = ipPort[[2]], 
           startH2O      = FALSE, 
           silentUpgrade = TRUE)
  #source("../../h2oRClient-package/R/Algorithms.R")
  #source("../../h2oRClient-package/R/Classes.R")
  #source("../../h2oRClient-package/R/ParseImport.R")
  #source("../../h2oRClient-package/R/Internal.R")
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
Log.info("Branch: ")
system('git branch')
Log.info("Hash: ")
system('git rev-parse HEAD')

defaultPath <- locate("../../target/R")
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

#Global Variables
myIP   <- ipPort[[1]]
myPort <- ipPort[[2]]
PASSS <- FALSE
view_max <- 10000 #maximum returned by Inspect.java
SEED <- NULL

