# R -f test_R_boilerplate.R --args H2OServer:Port
args <- commandArgs(trailingOnly = TRUE)
if(length(args) != 1)
  stop("Usage: R -f test_R_boilerplate.R --args H2OServer:Port")
argsplit = strsplit(args[1], ":")[[1]]
myIP = argsplit[1]
myPort = as.numeric(argsplit[2])
defaultPath = "../../target/R"

# Install prerequisite R packages for running H2O
if(!"bitops" %in% rownames(installed.packages())) install.packages("bitops")
if(!"RCurl" %in% rownames(installed.packages())) install.packages("RCurl")
if(!"rjson" %in% rownames(installed.packages())) install.packages("rjson")

library(RCurl)
library(rjson)
library(tools)

# Check if H2O R wrapper package is installed
if(!"h2oWrapper" %in% rownames(installed.packages())) {
  wrapEnv = Sys.getenv("H2OWrapperPath")
  wrapName = list.files(defaultPath, pattern="h2oWrapper")[1]
  wrapPath = ifelse(wrapEnv == "", paste(defaultPath, wrapName, sep="/"), wrapEnv)
  
  if(!file.exists(wrapPath))
    stop(paste("h2oWrapper package does not exist at", wrapPath))
  install.packages(wrapPath, repos = NULL, type = "source")
}

# Check that H2O R package matches version on cloud
library(h2oWrapper)
h2o.initCloud(ip=myIP, port=myPort)

# Load H2O R package and run test
library(h2o)
localH2O = new("H2OClient", ip=myIP, port=myPort)
# Body of test goes here