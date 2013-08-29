myPackages = rownames(installed.packages())
if(!"bitops" %in% myPackages) install.packages("bitops")
if(!"RCurl" %in% myPackages) install.packages("RCurl")
if(!"rjson" %in% myPackages) install.packages("rjson")

library(RCurl)
library(rjson)
library(tools)

ip = readline("Enter IP address: ")
port = readline("Enter port number: ")

myURL = paste("http://", ip, ":", port, sep="")
if(!url.exists(myURL)) stop("Cannot connect to H2O server")
temp = postForm(paste(myURL, "RPackage.json", sep="/"), style = "POST")
res = fromJSON(temp)
H2OVersion = res$version
myFile = res$filename
serverMD5 = res$md5_hash

if(!("h2o" %in% myPackages && packageVersion("h2o") == H2OVersion)) {
  if("h2o" %in% myPackages) {
    cat("Removing old H2O R package version", H2OVersion, "\n")
    remove.packages("h2o")
  }
  cat("Downloading and installing H2O R package version", H2OVersion, "\n")
  # download.file(paste(myURL, "R", myFile, sep="/"), destfile = paste(getwd(), myFile, sep="/"), mode = "wb")
  temp = getBinaryURL(paste(myURL, "R", myFile, sep="/"))
  writeBin(temp, paste(getwd(), myFile, sep="/"))
  
  if(as.character(serverMD5) != as.character(md5sum(paste(getwd(), myFile, sep="/"))))
    warning("Mismatched MD5 hash! Check you have downloaded complete R package.")
  install.packages(paste(getwd(), myFile, sep="/"), repos = NULL, type = "source")
} else
  cat("H2O R package and server version", H2OVersion, "match\n")