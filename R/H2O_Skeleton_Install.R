myPackages = rownames(installed.packages())
if(!"bitops" %in% myPackages) install.packages("bitops")
if(!"RCurl" %in% myPackages) install.packages("RCurl")
if(!"rjson" %in% myPackages) install.packages("rjson")

library(RCurl)
library(rjson)

ip = readline("Enter IP address: ")
port = readline("Enter port number: ")

myURL = paste("http://", ip, ":", port, sep="")
if(!url.exists(myURL)) stop("Cannot connect to H2O server")
temp = postForm(paste(myURL, "Cloud.json", sep="/"), style = "POST")
res = fromJSON(temp)
H2OVersion = res$version
if(!("h2o" %in% myPackages && packageVersion("h2o") == H2OVersion)) {
  cat("Downloading and installing H2O R package version", H2OVersion, "\n")
  myFile = paste("h2o_", H2OVersion, ".tar.gz", sep="")
  # download.file(paste(myURL, "R", myFile, sep="/"), destfile = paste(getwd(), myFile, sep="/"), mode = "wb")
  temp = getBinaryURL(paste(myURL, "R", myFile, sep="/"))
  writeBin(temp, paste(getwd(), myFile, sep="/"))
  install.packages(paste(getwd(), myFile, sep="/"), repos = NULL, type = "source")
} else
  cat("H2O R package and server version", H2OVersion, "match\n")