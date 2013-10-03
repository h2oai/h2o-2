context("Tail Tests Numeric") #set context for test. Here we are checking the tail() functionality
# R -f runit_tail.R --args H2OServer:Port
# By default, H2OServer = 127.0.0.1 and Port = 54321
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 1)
  stop("Usage: R -f runit_tail_numeric.R --args H2OServer:Port")
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
if(!"testthat" %in% rownames(installed.packages())) install.packages("testthat")

library(RUnit)
library(testthat)
library(h2o)

if(Sys.info()['sysname'] == "Windows")
  options(RCurlOptions = list(cainfo = system.file("CurlSSL", "cacert.pem", package = "RCurl")))

#------------------------------ Begin Tests ------------------------------#
view_max <- 10000 #maximum returned by Inspect.java

H2Ocon <- new("H2OClient", ip=myIP, port=myPort)

test.tail.numeric <- function(con) {
  cat("\nImporting USArrests.csv data...\n")
  arrests.hex = h2o.uploadFile(H2Ocon, "../../smalldata/pca_test/USArrests.csv", "arrests.hex")
  cat("\nCheck that tail works...")
  tail(arrests.hex)
  tail_ <- tail(arrests.hex)
  cat("\nCheck that we get a data frame back from the tail(hex)")
  expect_that(tail_, is_a("data.frame"))
  tail_2 <- tail(USArrests)
  rownames(tail_2) <- 1:nrow(tail_2) #remove state names from USArrests
  cat("\nCheck that the tail of the dataset is the same as what R produces: ")
  cat("\ntail(USArrests)\n")
  print(tail_2)
  cat("\ntail(arrests.hex)\n")
  print(tail_)
  expect_that(tail_, equals(tail_2))
  if( nrow(arrests.hex) <= view_max) {
    cat("\nTry doing tail w/ n > nrows(data). Should do same thing as R (returns all rows)")
    cat(paste("\n Data has ", paste(nrow(arrests.hex), " rows",sep=""),sep=""))
    cat("\n")
    tail_max <- tail(arrests.hex,nrow(arrests.hex) + 1)
  }
  print("End of test.")
}

test_that("tailTests",test.tail.numeric(H2Ocon))

