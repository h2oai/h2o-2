read.zip <- function(zipfile, exdir,header=T) {
    zipdir <- exdir
    unzip(zipfile, exdir=zipdir)
    files <- list.files(zipdir)
    file <- paste(zipdir, files[1], sep="/")
    read.csv(file,header=header)
}

remove_exdir <- function(exdir) {
    exec <- paste("rm -r ", exdir, sep="")
    system(exec)
}

logging<- 
function(m) {
  cat(sprintf("[%s] %s\n", Sys.time(),m))
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
  logging("\nCheck if H2O R wrapper package is installed\n")
  if (!"h2oWrapper" %in% rownames(installed.packages())) {
    envPath  = Sys.getenv("H2OWrapperDir")
    wrapDir  = ifelse(envPath == "", defaultPath, envPath)
    wrapName = list.files(wrapDir, pattern  = "h2oWrapper")[1]
    wrapPath = paste(wrapDir, wrapName, sep = "/")
    
    if (!file.exists(wrapPath))
      stop(paste("h2oWrapper package does not exist at", wrapPath));
    print(paste("Installing h2oWrapper package from", wrapPath))
    install.packages(wrapPath, repos = NULL, type = "source")
  }

  logging("\nCheck that H2O R package matches version on server\n")
  library(h2oWrapper)
  h2oWrapper.installDepPkgs()      # Install R package dependencies
  h2oWrapper.init(ip            = ipPort[[1]], 
                  port          = ipPort[[2]], 
                  startH2O      = FALSE, 
                  silentUpgrade = TRUE)
}

checkNLoadPackages<-
function() {
  logging("\nChecking Package dependencies for this test.\n")
  if (!"RUnit"    %in% rownames(installed.packages())) install.packages("RUnit")
  if (!"testthat" %in% rownames(installed.packages())) install.packages("testthat")
  
  if (Sys.info()['sysname'] == "Windows")
    options(RCurlOptions = list(cainfo = system.file("CurlSSL", "cacert.pem", package = "RCurl")))

  logging("\nLoading RUnit and testthat\n")
  require(RUnit)
  require(testthat)
  require(h2o)
}

logging("\n============== Setting up R-Unit environment... ================\n")
defaultPath <- "../../target/R"
ipPort <- get_args(commandArgs(trailingOnly = TRUE))
checkNLoadWrapper(ipPort)
checkNLoadPackages()

logging("\nLoading other required test packages")
if(!"glmnet" %in% rownames(installed.packages())) install.packages("glmnet")
if(!"gbm"    %in% rownames(installed.packages())) install.packages("gbm")
require(glmnet)
require(gbm)

myIP   <- ipPort[[1]]
myPort <- ipPort[[2]]




