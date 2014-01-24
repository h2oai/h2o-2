options(echo=F)
options(digits=16)
local({r <- getOption("repos"); r["CRAN"] <- "http://cran.us.r-project.org"; options(repos = r)})
if (!"R.utils" %in% rownames(installed.packages())) install.packages("R.utils")
library(R.utils)

IP <- ""
PORT <- ""
start_time <- ""

get_args<-
function(args) {
  fileName <- commandArgs()[grep('*\\.R',unlist(commandArgs()))]

  if (length(args) == 0) {
    IP   <<- "127.0.0.1"
    PORT <<- 54321
  } else {
    argsplit <- strsplit(args[1], ":")[[1]]
    IP       <<- argsplit[1]
    PORT     <<- as.numeric(argsplit[2])
  }
  return(list(myIP,myPort));
}

installDepPkgs<- 
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

h2o_setup<- 
function() {
  if (!"h2o" %in% rownames(installed.packages())) {
      envPath  <- Sys.getenv("H2OWrapperDir")
      wrapDir  <- ifelse(envPath == "", defaultPath, envPath)
      wrapName <- list.files(wrapDir, pattern  = "h2o")[1]
      wrapPath <- paste(wrapDir, wrapName, sep = "/")
      
      if (!file.exists(wrapPath))
        stop(paste("h2o package does not exist at", wrapPath));
      print(paste("Installing h2o package from", wrapPath))
      installDepPkgs()
      install.packages(wrapPath, repos = NULL, type = "source")
    }
 
  installDepPkgs()
  library(h2o)
  h <- h2o.init(ip            = IP,
                port          = PORT,
                startH2O      = FALSE, 
                silentUpgrade = TRUE)
}

upload.VA<-
function(file) {
  h2o.uploadFile.VA(h, file)
}

upload.FV<-
function(file) {
  h2o.uploadFile.FV(h, file)
}

hdfs.VA<-
function(file) {
  h2o.importHDFS.VA(h, file)
}

hdfs.FV<-
function(file) {
  h2o.importHDFS.FV(h, file)
}
get_args()
h2o_setup()
start_time <<- round(System$currentTimeMillis())

