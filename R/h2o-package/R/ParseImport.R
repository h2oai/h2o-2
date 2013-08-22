setGeneric("h2o.checkClient", function(object) { standardGeneric("h2o.checkClient") })
setGeneric("h2o.ls", function(object) { standardGeneric("h2o.ls") })
setGeneric("h2o.rm", function(object, keys) { standardGeneric("h2o.rm") })
# setGeneric("h2o.importFile", function(object, path, key = "", header, parse = TRUE) { standardGeneric("h2o.importFile") })
setGeneric("h2o.importFile", function(object, path, key = "", parse = TRUE, sep = "") { standardGeneric("h2o.importFile") })
setGeneric("h2o.importFolder", function(object, path, parse = TRUE, sep = "") { standardGeneric("h2o.importFolder") })
# setGeneric("h2o.importURL", function(object, path, key = "", parse = TRUE, header, sep = "", col.names) { standardGeneric("h2o.importURL") })
setGeneric("h2o.importURL", function(object, path, key = "", parse = TRUE, sep = "") { standardGeneric("h2o.importURL") })
setGeneric("h2o.importHDFS", function(object, path, parse = TRUE, sep = "") { standardGeneric("h2o.importHDFS") })
setGeneric("h2o.parseRaw", function(data, key = "", header, sep = "", col.names) { standardGeneric("h2o.parseRaw") })
setGeneric("h2o.setColNames", function(data, col.names) { standardGeneric("h2o.setColNames") })

# Unique methods to H2O
# H2O client management operations
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

setMethod("h2o.checkClient", signature(object="H2OClient"), function(object) { 
  myURL = paste("http://", object@ip, ":", object@port, sep="")
  if(!url.exists(myURL)) {
    print("H2O is not running yet, launching it now.")
    h2o.startLauncher()
    invisible(readline("Start H2O, then hit <Return> to continue: "))
    if(!url.exists(myURL)) stop("H2O failed to start, stopping execution.")
  } else { 
    cat("Successfully connected to", myURL, "\n")
    if("h2o" %in% rownames(installed.packages()) && (pv=packageVersion("h2o")) != (sv=h2o.__version(object)))
      warning(paste("Version mismatch! Server running H2O version", sv, "but R package is version", pv))
  }
})

setMethod("h2o.ls", signature(object="H2OClient"), function(object) {
  res = h2o.__remoteSend(object, h2o.__PAGE_VIEWALL)
  if(length(res$keys) == 0) return(list())
  myList = lapply(res$keys, function(y) c(y$key, y$value_size_bytes))
  temp = data.frame(matrix(unlist(myList), nrow = res$num_keys, byrow = TRUE))
  colnames(temp) = c("Key", "Bytesize")
  temp$Key = as.character(temp$Key)
  temp$Bytesize = as.numeric(as.character(temp$Bytesize))
  temp
})

setMethod("h2o.rm", signature(object="H2OClient", keys="character"), function(object, keys) {
  for(i in 1:length(keys))
    h2o.__remoteSend(object, h2o.__PAGE_REMOVE, key=keys[i])
  # myKeys = h2o.ls(object)$Key
  # if(length(grep("Result_[[:digit:]]+.hex", myKeys)) == 0)
  #  RESULT_COUNT = 0
})

# Data import operations
setMethod("h2o.importURL", signature(object="H2OClient", path="character", key="character", parse="logical", sep="character"),
          function(object, path, key, parse, sep) {
            destKey = ifelse(parse, "", key)
            res = h2o.__remoteSend(object, h2o.__PAGE_IMPORTURL, url=path, key=destKey)
            rawData = new("H2ORawData", h2o=object, key=res$key)
            if(parse) parsedData = h2o.parseRaw(data=rawData, key=key, sep=sep) else rawData
          })

setMethod("h2o.importURL", signature(object="H2OClient", path="character", key="ANY", parse="ANY", sep="ANY"),
          function(object, path, key, parse, sep) { 
            if(!(missing(key) || class(key) == "character"))
              stop(paste("key cannot be of class", class(key)))
            if(!(missing(parse) || class(parse) == "logical"))
              stop(paste("parse cannot be of class", class(parse)))
            if(!(missing(sep) || class(sep) == "character"))
              stop(paste("sep cannot be of class", class(sep)))
            h2o.importURL(object, path, key, parse, sep)
          })

setMethod("h2o.importFolder", signature(object="H2OClient", path="character", parse="logical", sep="character"),
          function(object, path, parse, sep) {
            # if(!file.exists(path)) stop("Directory does not exist!")
            # res = h2o.__remoteSend(object, h2o.__PAGE_IMPORTFILES, path=normalizePath(path))
            res = h2o.__remoteSend(object, h2o.__PAGE_IMPORTFILES, path=path)
            myKeys = res$keys
            myData = vector("list", length(myKeys))
            for(i in 1:length(myKeys)) {
              rawData = new("H2ORawData", h2o=object, key=myKeys[i])
              if(parse) {
                cat("Parsing key", myKeys[i], "\n")
                myData[[i]] = h2o.parseRaw(data=rawData, key="", sep=sep)
              }
              else myData[[i]] = rawData
            }
            myData
          })

setMethod("h2o.importFolder", signature(object="H2OClient", path="character", parse="ANY", sep="ANY"),
          function(object, path, parse, sep) {
            if(!(missing(parse) || class(parse) == "logical"))
              stop(paste("parse cannot be of class", class(parse)))
            if(!(missing(sep) || class(sep) == "character"))
              stop(paste("sep cannot be of class", class(sep)))
            h2o.importFolder(object, path, parse, sep) 
          })

setMethod("h2o.importFile", signature(object="H2OClient", path="character", key="missing", parse="ANY", sep="ANY"), 
          function(object, path, parse, sep) { 
            # if(!file.exists(path)) stop("File does not exist!")
            h2o.importFolder(object, path, parse, sep)[[1]]
          })

setMethod("h2o.importFile", signature(object="H2OClient", path="character", key="character", parse="ANY", sep="ANY"), 
          function(object, path, key, parse, sep) {
            # if(!file.exists(path)) stop("File does not exist!")
            # h2o.importURL(object, paste("file:///", normalizePath(path), sep=""), key, parse, sep) 
            h2o.importURL(object, paste("file:///", path, sep=""), key, parse, sep)
          })

setMethod("h2o.importHDFS", signature(object="H2OClient", path="character", parse="ANY", sep="ANY"),
          function(object, path, parse, sep) {
            if(!(missing(parse) || class(parse) == "logical"))
              stop(paste("parse cannot be of class", class(parse)))
            if(!(missing(sep) || class(sep) == "character"))
              stop(paste("sep cannot be of class", class(sep)))
            
            res = h2o.__remoteSend(object, h2o.__PAGE_IMPORTHDFS, path=path)
            myData = vector("list", res$num_succeeded)
            if(length(res$failed) > 0) {
              for(i in 1:res$num_failed) 
                cat(res$failed[[i]]$file, "failed to import")
            }
            if(res$num_succeeded > 0) {
              for(i in 1:res$num_succeeded) {
                rawData = new("H2ORawData", h2o=object, key=res$succeeded[[i]]$key)
                if(parse) {
                  cat("Parsing key", res$succeeded[[i]]$key, "\n")
                  myData[[i]] = h2o.parseRaw(data=rawData, key="", sep=sep)
                }
                else myData[[i]] = rawData
              }
            }
            if(res$num_succeeded == 1) myData[[1]] else myData
          })

setMethod("h2o.parseRaw", signature(data="H2ORawData", key="character", header="logical", sep="character", col.names="H2OParsedData"), 
          function(data, key, header, sep, col.names) {
            sepAscii = ifelse(sep == "", sep, strtoi(charToRaw(sep), 16L))
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_PARSE, source_key=data@key, destination_key=key, header=as.numeric(header), header_from_file=col.names@key, separator=sepAscii)
            while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
            parsedData = new("H2OParsedData", h2o=data@h2o, key=res$destination_key)
          })

setMethod("h2o.parseRaw", signature(data="H2ORawData", key="ANY", header="logical", sep="ANY", col.names="H2OParsedData"), 
          function(data, key, header, sep, col.names) {
            if(!(missing(key) || class(key) == "character"))
              stop(paste("key cannot be of class", class(key)))
            if(!(missing(sep) || class(sep) == "character"))
              stop(paste("sep cannot be of class", class(sep)))
            h2o.parseRaw(data, key, header, sep, col.names)
          })

setMethod("h2o.parseRaw", signature(data="H2ORawData", key="ANY", header="logical", sep="ANY", col.names="missing"), 
          function(data, key, header, sep) {
            temp = new("H2OParsedData", h2o=new("H2OClient"), key="")
            h2o.parseRaw(data, key, header, sep, temp)
          })

setMethod("h2o.parseRaw", signature(data="H2ORawData", key="ANY", header="missing", sep="ANY", col.names="H2OParsedData"), 
          function(data, key, sep, col.names) {
            if(!(missing(key) || class(key) == "character"))
              stop(paste("key cannot be of class", class(key)))
            if(!(missing(sep) || class(sep) == "character"))
              stop(paste("sep cannot be of class", class(sep)))
            sepAscii = ifelse(sep == "", sep, strtoi(charToRaw(sep), 16L))
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_PARSE, source_key=data@key, destination_key=key, header_from_file=col.names@key, separator=sepAscii)
            while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
            parsedData = new("H2OParsedData", h2o=data@h2o, key=res$destination_key)
          })

setMethod("h2o.parseRaw", signature(data="H2ORawData", key="ANY", header="missing", sep="ANY", col.names="missing"), 
          function(data, key, sep) {
            temp = new("H2OParsedData", h2o=new("H2OClient"), key="")
            h2o.parseRaw(data=data, key=key, sep=sep, col.names=temp)
          })
      
setMethod("h2o.setColNames", signature(data="H2OParsedData", col.names="H2OParsedData"),
          function(data, col.names) { res = h2o.__remoteSend(data@h2o, h2o.__PAGE_COLNAMES, target=data@key, source=col.names@key) })