setGeneric("h2oWrapper.init", function(ip = "127.0.0.1", port = 54321, startH2O = TRUE, silentUpgrade = FALSE, promptUpgrade = TRUE) { standardGeneric("h2oWrapper.init") })

# Install H2O R package dependencies
# MUST RUN THIS ON FIRST INSTALLATION!!
h2oWrapper.installDepPkgs <- function(optional = FALSE) {
  myPackages = rownames(installed.packages())
  
  # For communicating with H2O via REST API
  if(!"bitops" %in% myPackages) install.packages("bitops")
  if(!"RCurl" %in% myPackages) install.packages("RCurl")
  if(!"rjson" %in% myPackages) install.packages("rjson")
  if(!"statmod" %in% myPackages) install.packages("statmod")
  if(!"uuid" %in% myPackages) install.packages("uuid")
  myReqPkgs = c("RCurl", "rjson", "tools", "statmod", "uuid")
  
  # For plotting clusters in h2o.kmeans demo
  if(optional) {
    if(!"fpc" %in% myPackages) install.packages("fpc")
    if(!"cluster" %in% myPackages) install.packages("cluster")
    myReqPkgs = c(myReqPkgs, "fpc", "cluster")
  }
  
  # myReqPkgs = c("RCurl", "rjson", "tools", "statmod", "uuid", "fpc", "cluster")
  temp = lapply(myReqPkgs, require, character.only = TRUE)
}

# Checks H2O connection and installs H2O R package matching version on server if indicated by user
# 1) If can't connect and user doesn't want to start H2O, stop immediately
# 2) If user does want to start H2O and running locally, attempt to bring up H2O launcher
# 3) If user does want to start H2O, but running non-locally, print an error
setMethod("h2oWrapper.init", signature(ip="character", port="numeric", startH2O="logical", silentUpgrade="logical", promptUpgrade="logical"), 
          function(ip, port, startH2O, silentUpgrade, promptUpgrade) {
  myReqPkgs = c("RCurl", "rjson", "tools", "statmod", "uuid")
  temp = lapply(myReqPkgs, require, character.only = TRUE)
            
  myURL = paste("http://", ip, ":", port, sep="")
  if(!url.exists(myURL)) {
    if(!startH2O)
      stop(paste("Cannot connect to H2O server. Please check that H2O is running at", myURL))
    else if(ip=="localhost" || ip=="127.0.0.1") {
      print("H2O is not running yet, launching it now.")
      h2oWrapper.startLauncher()
      invisible(readline("Start H2O, then hit <Return> to continue: "))
      if(!url.exists(myURL)) stop("H2O failed to start, stopping execution.")
    } else stop("Can only start H2O launcher if IP address is localhost")
  }
  cat("Successfully connected to", myURL, "\n")
  h2oWrapper.checkPackage(myURL, silentUpgrade, promptUpgrade)
  
  library(h2o)
  return(new("H2OClient", ip = ip, port = port))
})

setMethod("h2oWrapper.init", signature(ip="ANY", port="ANY", startH2O="ANY", silentUpgrade="ANY", promptUpgrade="ANY"), 
          function(ip, port, startH2O, silentUpgrade, promptUpgrade) {
  if(!(missing(ip) || class(ip) == "character"))
    stop(paste("ip cannot be of class", class(ip)))
  if(!(missing(port) || class(port) == "numeric"))
    stop(paste("port cannot be of class", class(port)))
  if(!(missing(startH2O) || class(startH2O) == "logical"))
    stop(paste("startH2O cannot be of class", class(startH2O)))
  if(!(missing(silentUpgrade) || class(silentUpgrade) == "logical"))
    stop(paste("silentUpgrade cannot be of class", class(silentUpgrade)))
  if(!(missing(promptUpgrade) || class(promptUpgrade) == "logical"))
    stop(paste("promptUpgrade cannot be of class", class(promptUpgrade)))
  h2oWrapper.init(ip, port, startH2O, silentUpgrade, promptUpgrade)
})

# Start H2O launcher GUI if installed locally from InstallBuilder executable
h2oWrapper.startLauncher <- function() {
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

h2oWrapper.checkPackage <- function(myURL, silentUpgrade, promptUpgrade) {
  library(RCurl)
  library(rjson)
  library(tools)
  
  temp = postForm(paste(myURL, h2o.__PAGE_RPACKAGE, sep="/"), style = "POST")
  res = fromJSON(temp)
  if (!is.null(res$error))
    stop(paste(myURL," returned the following error:\n", h2oWrapper.__formatError(res$error)))
  
  H2OVersion = res$version
  myFile = res$filename
  serverMD5 = res$md5_hash
  
  myPackages = rownames(installed.packages())
  if("h2o" %in% myPackages && packageVersion("h2o") == H2OVersion)
    cat("H2O R package and server version", H2OVersion, "match\n")
  else if(h2oWrapper.shouldUpgrade(silentUpgrade, promptUpgrade, H2OVersion)) {    
    if("h2o" %in% myPackages) {
      cat("Removing old H2O R package version", toString(packageVersion("h2o")), "\n")
      remove.packages("h2o")
    }
    cat("Downloading and installing H2O R package version", H2OVersion, "\n")
    # download.file(paste(myURL, "R", myFile, sep="/"), destfile = paste(getwd(), myFile, sep="/"), mode = "wb")
    temp = getBinaryURL(paste(myURL, "R", myFile, sep="/"))
    writeBin(temp, paste(getwd(), myFile, sep="/"))
    
    if(as.character(serverMD5) != as.character(md5sum(paste(getwd(), myFile, sep="/"))))
      warning("Mismatched MD5 hash! Check you have downloaded complete R package.")
    install.packages(paste(getwd(), myFile, sep="/"), repos = NULL, type = "source")
    file.remove(paste(getwd(), myFile, sep="/"))
    #cat("\nSuccess\nYou may now type 'library(h2o)' to load the R package\n\n")
    library(h2o)
  }
}

# Check if user wants to install H2O R package matching version on server
# Note: silentUpgrade supercedes promptUpgrade
h2oWrapper.shouldUpgrade <- function(silentUpgrade, promptUpgrade, H2OVersion) {
  if(silentUpgrade) return(TRUE)
  if(promptUpgrade) {
    ans = readline(paste("Do you want to install H2O R package", H2OVersion, "from the server (Y/N)? "))
    temp = substr(ans, 1, 1)
    if(temp == "Y" || temp == "y") return(TRUE)
    else if(temp == "N" || temp == "n") return(FALSE)
    else stop("Invalid answer! Please enter Y for yes or N for no")
  } else return(FALSE)
}

h2o.__PAGE_RPACKAGE = "RPackage.json"

h2oWrapper.__formatError <- function(error, prefix="  ") {
  result = ""
  items = strsplit(error,"\n")[[1]];
  for (i in 1:length(items))
    result = paste(result, prefix, items[i], "\n", sep="")
  result
}
