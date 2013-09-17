if(!"bitops" %in% rownames(installed.packages())) install.packages("bitops")
if(!"RCurl" %in% rownames(installed.packages())) install.packages("RCurl")
if(!"rjson" %in% rownames(installed.packages())) install.packages("rjson")

library(RCurl)
library(rjson)
library(tools)

setGeneric("h2o.initCloud", function(ip, port) { standardGeneric("h2o.initCloud") })
setGeneric("h2o.initLocal", function(ip = "localhost", port = 54321) { standardGeneric("h2o.initLocal") })

setMethod("h2o.initCloud", signature(ip="character", port="numeric"), function(ip, port) {
  myURL = paste("http://", ip, ":", port, sep="")
  if(!url.exists(myURL))
    stop(paste("Cannot connect to H2O server. Please check that H2O is running at", myURL))
  h2o.checkPackage(myURL)
})

setMethod("h2o.initLocal", signature(ip="character", port="numeric"), function(ip, port) {
  myURL = paste("http://", ip, ":", port, sep="")
  if(!url.exists(myURL)) {
    print("H2O is not running yet, launching it now.")
    h2o.startLauncher()
    invisible(readline("Start H2O, then hit <Return> to continue: "))
    if(!url.exists(myURL)) stop("H2O failed to start, stopping execution.")
  }
  cat("Successfully connected to", myURL, "\n")
  h2o.checkPackage(myURL)
})

setMethod("h2o.initLocal", signature(ip="ANY", port="ANY"), function(ip, port) {
  if(!(missing(ip) || class(ip) == "character"))
    stop(paste("ip cannot be of class", class(ip)))
  if(!(missing(port) || class(port) == "numeric"))
    stop(paste("port cannot be of class", class(port)))
  h2o.initLocal(ip, port)
})

h2o.startLauncher <- function() {
  myOS = Sys.info()["sysname"]
  
  if(myOS == "Windows") verPath = paste(Sys.getenv("APPDATA"), "h2o", sep="/")
  else verPath = paste(Sys.getenv("HOME"), "Library/Application Support/h2o", sep="/")
  myFiles = list.files(verPath)
  if(length(myFiles) == 0) stop("Cannot find location of H2O launcher. Please check that your H2O installation is complete.")
  # Must trim myFiles so all have format 1.2.3.45678.txt (use regexpr)!
  
  # Get H2O with latest version number
  # If latest isn't working, maybe go down list to earliest until one executes?
  fileName = paste(verPath, tail(myFiles, n=1), sep="/")
  myVersion = strsplit(tail(myFiles, n=1), ".txt")[[1]]
  launchPath = readChar(fileName, file.info(fileName)$size)
  if(is.null(launchPath) || launchPath == "")
    stop(paste("No H2O launcher matching H2O version", myVersion, "found"))
  
  cat("Launching H2O version", myVersion)
  if(myOS == "Windows") {
    tempPath = paste(launchPath, "windows/h2o.bat", sep="/")
    if(!file.exists(tempPath)) stop(paste("Cannot open H2OLauncher.jar! Please check if it exists at", tempPath))
    shell.exec(tempPath)
  }
  else {
    tempPath = paste(launchPath, "Contents/MacOS/h2o", sep="/")
    if(!file.exists(tempPath)) stop(paste("Cannot open H2OLauncher.jar! Please check if it exists at", tempPath))
    system(paste("bash ", tempPath))
  }
}

h2o.checkPackage <- function(myURL) {
  temp = postForm(paste(myURL, "RPackage.json", sep="/"), style = "POST")
  res = fromJSON(temp)
  if (!is.null(res$error))
    stop(paste(myURL," returned the following error:\n", h2o.__formatError(res$error)))
  
  H2OVersion = res$version
  myFile = res$filename
  serverMD5 = res$md5_hash
  
  myPackages = rownames(installed.packages())
  if("h2o" %in% myPackages && packageVersion("h2o") == H2OVersion)
    cat("H2O R package and server version", H2OVersion, "match\n")
  else {
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
  }
}

h2o.__formatError <- function(error, prefix="  ") {
  result = ""
  items = strsplit(error,"\n")[[1]];
  for (i in 1:length(items))
    result = paste(result, prefix, items[i], "\n", sep="")
  result
}