# Unique methods to H2O
# H2O client management operations
h2o.startLauncher <- function() {
  myOS = Sys.info()["sysname"]
  
  if(myOS == "Windows") verPath = paste(Sys.getenv("APPDATA"), "h2o", sep="/")
  else verPath = paste(Sys.getenv("HOME"), "Library/Application Support/h2o", sep="/")
  myFiles = list.files(verPath)
  if(length(myFiles) == 0) stop("Cannot find location of H2O launcher. Please check that your H2O installation is complete.")
  # TODO: Must trim myFiles so all have format 1.2.3.45678.txt (use regexpr)!
  
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

h2o.checkClient <- function(object) {
  if(class(object) != "H2OClient") stop("object must be of class H2OClient")
  
  myURL = paste("http://", object@ip, ":", object@port, sep="")
  if(!url.exists(myURL)) {
    print("H2O is not running yet, launching it now.")
    h2o.startLauncher()
    invisible(readline("Start H2O, then hit <Return> to continue: "))
    if(!url.exists(myURL)) stop("H2O failed to start, stopping execution.")
  } else { 
    cat("Successfully connected to", myURL, "\n")
    if("h2oRClient" %in% rownames(installed.packages()) && (pv=packageVersion("h2oRClient")) != (sv=h2o.__version(object)))
      warning(paste("Version mismatch! Server running H2O version", sv, "but R package is version", pv))
  }
}

h2o.ls <- function(object, pattern = "") {
  if(class(object) != "H2OClient") stop("object must be of class H2OClient")
  if(!is.character(pattern)) stop("pattern must be of class character")
  
  res = h2o.__remoteSend(object, h2o.__PAGE_VIEWALL, filter=pattern)
  if(length(res$keys) == 0) return(list())
  myList = lapply(res$keys, function(y) c(y$key, y$value_size_bytes))
  temp = data.frame(matrix(unlist(myList), nrow = res$num_keys, ncol=2, byrow = TRUE))
  colnames(temp) = c("Key", "Bytesize")
  temp$Key = as.character(temp$Key)
  temp$Bytesize = as.numeric(as.character(temp$Bytesize))
  return(temp)
}

h2o.rm <- function(object, keys) {
  if(class(object) != "H2OClient") stop("object must be of class H2OClient")
  if(!is.character(keys)) stop("keys must be of class character")
  
  for(i in 1:length(keys))
    h2o.__remoteSend(object, h2o.__PAGE_REMOVE, key=keys[[i]])
}

h2o.assign <- function(data, key) {
  if(class(data) != "H2OParsedData") stop("data must be of class H2OParsedData")
  if(!is.character(key)) stop("key must be of class character")
  if(length(key) == 0) stop("key cannot be an empty string!")
  if(key == data@key) stop(paste("Destination key must differ from data key", data@key))
  
  res = h2o.__exec2_dest_key(data@h2o, data@key, key)
  data@key = key
  return(data)
}

h2o.importFolder <- function(object, path, pattern = "", key = "", parse = TRUE, header, sep = "", col.names) {
  if(class(object) != "H2OClient") stop("object must be of class H2OClient")
  if(!is.character(path)) stop("path must be of class character")
  if(nchar(path) == 0) stop("path must be a non-empty string")
  if(!is.character(pattern)) stop("pattern must be of class character")
  if(!is.character(key)) stop("key must be of class character")
  if(!is.logical(parse)) stop("parse must be of class logical")
  
  # if(!file.exists(path)) stop("Directory does not exist!")
  # res = h2o.__remoteSend(object, h2o.__PAGE_IMPORTFILES2, path=normalizePath(path))
  res = h2o.__remoteSend(object, h2o.__PAGE_IMPORTFILES2, path=path)
  if(length(res$fails) > 0) {
    for(i in 1:length(res$fails)) 
      cat(res$fails[[i]], "failed to import")
  }
  
  # Return only the files that successfully imported
  if(length(res$files) > 0) {
    if(parse) {
      regPath = paste(path, pattern, sep="/")
      srcKey = ifelse(length(res$keys) == 1, res$keys[[1]], paste("*", regPath, "*", sep=""))
      rawData = new("H2ORawData", h2o=object, key=srcKey)
      h2o.parseRaw(data=rawData, key=key, header=header, sep=sep, col.names=col.names) 
    } else {
      myData = lapply(res$keys, function(x) { new("H2ORawData", h2o=object, key=x) })
      if(length(res$keys) == 1) myData[[1]] else myData
    }
  } else stop("All files failed to import!")
}

h2o.importFile <- function(object, path, key = "", parse = TRUE, header, sep = "", col.names) {
  h2o.importFolder(object, path, pattern = "", key, parse, header, sep, col.names)
}

h2o.uploadFile <- function(object, path, key = "", parse = TRUE, header, sep = "", col.names, silent = TRUE) {
  if(class(object) != "H2OClient") stop("object must be of class H2OClient")
  if(!is.character(path)) stop("path must be of class character")
  if(nchar(path) == 0) stop("path must be a non-empty string")
  if(!is.character(key)) stop("key must be of class character")
  if(!is.logical(parse)) stop("parse must be of class logical")
  if(!is.logical(silent)) stop("silent must be of class logical")
  
  url = paste("http://", object@ip, ":", object@port, "/2/PostFile.json", sep="")
  url = paste(url, "?key=", path, sep="")
  if(file.exists(h2o.__getCommandLog())) h2o.__logIt(url, NULL, "Command")
  if(silent)
    temp = postForm(url, .params = list(fileData = fileUpload(normalizePath(path))))
  else
    temp = postForm(url, .params = list(fileData = fileUpload(normalizePath(path))), .opts = list(verbose = TRUE))
  rawData = new("H2ORawData", h2o=object, key=path)
  if(parse) parsedData = h2o.parseRaw(data=rawData, key=key, header=header, sep=sep, col.names=col.names) else rawData
}

h2o.parseRaw <- function(data, key = "", header, sep = "", col.names) {
  if(class(data) != "H2ORawData") stop("data must be of class H2ORawData")
  if(!is.character(key)) stop("key must be of class character")
  if(!(missing(header) || is.logical(header))) stop(paste("header cannot be of class", class(header)))
  if(!is.character(sep)) stop("sep must be of class character")
  if(!(missing(col.names) || class(col.names) == "H2OParsedData")) stop(paste("col.names cannot be of class", class(col.names)))
  
  # If both header and column names missing, then let H2O guess if header exists
  sepAscii = ifelse(sep == "", sep, strtoi(charToRaw(sep), 16L))
  if(missing(header) && missing(col.names))
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_PARSE2, source_key=data@key, destination_key=key, separator=sepAscii)
  else if(missing(header) && !missing(col.names))
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_PARSE2, source_key=data@key, destination_key=key, separator=sepAscii, header=1, header_from_file=col.names@key)
  else if(!missing(header) && missing(col.names))
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_PARSE2, source_key=data@key, destination_key=key, separator=sepAscii, header=as.numeric(header))
  else
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_PARSE2, source_key=data@key, destination_key=key, separator=sepAscii, header=as.numeric(header), header_from_file=col.names@key)
  on.exit(h2o.__cancelJob(data@h2o, res$job_key))
  while(h2o.__poll(data@h2o, res$job_key) != -1) { Sys.sleep(1) }
  parsedData = new("H2OParsedData", h2o=data@h2o, key=res$destination_key)
}

h2o.importURL <- function(object, path, pattern = "", key = "", parse = TRUE, header, sep = "", col.names) {
  stop("This function has been deprecated in FluidVecs. Please use h2o.importFile with a http:// prefix instead.")
}

h2o.importHDFS <- function(object, path, pattern = "", key = "", parse = TRUE, header, sep = "", col.names) {
  stop("This function has been deprecated in FluidVecs. Please use h2o.importFolder with a hdfs:// prefix instead.")
}

setGeneric("h2o<-", function(x, value) { standardGeneric("h2o<-") })
setMethod("h2o<-", signature(x="H2OParsedData", value="H2OParsedData"), function(x, value) {
  res = h2o.__exec2_dest_key(x@h2o, value@key, x@key); return(x)
})

setMethod("h2o<-", signature(x="H2OParsedData", value="numeric"), function(x, value) {
  res = h2o.__exec2_dest_key(x@h2o, paste("c(", paste(value, collapse=","), ")", sep=""), x@key); return(x)
})
          
#-------------------------------- ValueArray -----------------------------------#
# Data import operations
h2o.importFolder.VA <- function(object, path, pattern = "", key = "", parse = TRUE, header, sep = "", col.names) {
  if(class(object) != "H2OClient") stop("object must be of class H2OClient")
  if(!is.character(path)) stop("path must be of class character")
  if(nchar(path) == 0) stop("path must be a non-empty string")
  if(!is.character(key)) stop("key must be of class character")
  if(!is.logical(parse)) stop("parse must be of class logical")
  
  res = h2o.__remoteSend(object, h2o.__PAGE_IMPORTFILES, path=path)
  if(length(res$fails) > 0) {
    for(i in 1:length(res$fails)) 
      cat(res$fails[[i]], "failed to import")
  }
  
  # Return only the files that successfully imported
  if(length(res$files) > 0) {
    if(parse) {
      regPath = paste(path, pattern, sep="/")
      srcKey = ifelse(length(res$keys) == 1, res$keys[1], paste("*", regPath, "*", sep=""))
      rawData = new("H2ORawDataVA", h2o=object, key=srcKey)
      h2o.parseRaw.VA(data=rawData, key=key, header=header, sep=sep, col.names=col.names) 
    } else {
      myData = lapply(res$keys, function(x) { new("H2ORawDataVA", h2o=object, key=x) })
      if(length(res$keys) == 1) myData[[1]] else myData
    }
  } else stop("All files failed to import!")
}

h2o.importFile.VA <- function(object, path, key, parse = TRUE, header, sep = "", col.names) {
  if(class(object) != "H2OClient") stop("object must be of class H2OClient")
  if(!is.character(path)) stop("path must be of class character")
  if(nchar(path) == 0) stop("path must be a non-empty string")
  if(!(missing(key) || is.character(key))) stop(paste("key cannot be of class", class(key)))
  if(!is.logical(parse)) stop("parse must be of class logical")
  
  if(missing(key))
    h2o.importFolder.VA(object, path, pattern = "", key = "", parse, header, sep, col.names = col.names)
  else
    h2o.importURL.VA(object, paste("file:///", path, sep=""), key, parse, header, sep, col.names = col.names)
}

h2o.importHDFS.VA <- function(object, path, pattern = "", key = "", parse = TRUE, header, sep = "", col.names) {
  if(class(object) != "H2OClient") stop("object must be of class H2OClient")
  if(!is.character(path)) stop("path must be of class character")
  if(nchar(path) == 0) stop("path must be a non-empty string")
  if(!is.character(key)) stop("key must be of class character")
  if(!is.logical(parse)) stop("parse must be of class logical")
  
  res = h2o.__remoteSend(object, h2o.__PAGE_IMPORTHDFS, path=path)
  if(length(res$failed) > 0) {
    for(i in 1:res$num_failed) 
      cat(res$failed[[i]]$file, "failed to import")
  }
  
  # Return only the files that successfully imported
  if(res$num_succeeded > 0) {
    if(parse) {
      regPath = paste(path, pattern, sep="/")
      srcKey = ifelse(res$num_succeeded == 1, res$succeeded[[1]]$key, paste("*", regPath, "*", sep=""))
      rawData = new("H2ORawDataVA", h2o=object, key=srcKey)
      h2o.parseRaw.VA(data=rawData, key=key, header=header, sep=sep, col.names=col.names) 
    } else {
      myData = lapply(res$succeeded, function(x) { new("H2ORawDataVA", h2o=object, key=x$key) })
      if(res$num_succeeded == 1) myData[[1]] else myData
    }
  } else stop("All files failed to import!")
}

h2o.exportHDFS <- function(object, path) {
  if(inherits(object, "H2OModelVA")) stop("h2o.exportHDFS does not work under VA")
  else if(!inherits(object, "H2OModel")) stop("object must be an H2O model")
  if(!is.character(path)) stop("path must be of class character")
  if(nchar(path) == 0) stop("path must be a non-empty string")
  
  res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_EXPORTHDFS, source_key = object@key, path = path)
}

h2o.uploadFile.VA <- function(object, path, key = "", parse = TRUE, header, sep = "", col.names, silent = TRUE) {
  if(class(object) != "H2OClient") stop("object must be of class H2OClient")
  if(!is.character(path)) stop("path must be of class character")
  if(nchar(path) == 0) stop("path must be a non-empty string")
  if(!is.character(key)) stop("key must be of class character")
  if(!is.logical(parse)) stop("parse must be of class logical")
  if(!is.logical(silent)) stop("silent must be of class logical")
  
  url = paste("http://", object@ip, ":", object@port, "/PostFile.json", sep="")
  url = paste(url, "?key=", path, sep="")
  if(silent)
    temp = postForm(url, .params = list(fileData = fileUpload(normalizePath(path))))
  else
    temp = postForm(url, .params = list(fileData = fileUpload(normalizePath(path))), .opts = list(verbose = TRUE))
  rawData = new("H2ORawDataVA", h2o=object, key=path)
  if(parse) parsedData = h2o.parseRaw.VA(data=rawData, key=key, header=header, sep=sep, col.names=col.names) else rawData
}

h2o.importURL.VA <- function(object, path, key = "", parse = TRUE, header, sep = "", col.names) {
  if(class(object) != "H2OClient") stop("object must be of class H2OClient")
  if(!is.character(path)) stop("path must be of class character")
  if(nchar(path) == 0) stop("path must be a non-empty string")
  if(!is.character(key)) stop("key must be of class character")
  if(!is.logical(parse)) stop("parse must be of class logical")
  
  destKey = ifelse(parse, "", key)
  res = h2o.__remoteSend(object, h2o.__PAGE_IMPORTURL, url=path, key=destKey)
  rawData = new("H2ORawDataVA", h2o=object, key=res$key)
  if(parse) parsedData = h2o.parseRaw.VA(data=rawData, key=key, header=header, sep=sep, col.names=col.names) else rawData
}

h2o.parseRaw.VA <- function(data, key = "", header, sep = "", col.names) {
  if(class(data) != "H2ORawDataVA") stop("data must be of class H2ORawData")
  if(!is.character(key)) stop("key must be of class character")
  if(!(missing(header) || is.logical(header))) stop(paste("header cannot be of class", class(header)))
  if(!is.character(sep)) stop("sep must be of class character")
  if(!(missing(col.names) || class(col.names) == "H2OParsedDataVA")) stop(paste("col.names cannot be of class", class(col.names)))
  
  # If both header and column names missing, then let H2O guess if header exists
  sepAscii = ifelse(sep == "", sep, strtoi(charToRaw(sep), 16L))
  if(missing(header) && missing(col.names))
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_PARSE, source_key=data@key, destination_key=key, separator=sepAscii)
  else if(missing(header) && !missing(col.names))
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_PARSE, source_key=data@key, destination_key=key, separator=sepAscii, header=1, header_from_file=col.names@key)
  else if(!missing(header) && missing(col.names))
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_PARSE, source_key=data@key, destination_key=key, separator=sepAscii, header=as.numeric(header))
  else
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_PARSE, source_key=data@key, destination_key=key, separator=sepAscii, header=as.numeric(header), header_from_file=col.names@key)
  on.exit(h2o.__cancelJob(data@h2o, res$response$redirect_request_args$job))
  while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
  parsedData = new("H2OParsedDataVA", h2o=data@h2o, key=res$destination_key)
}

setMethod("colnames<-", signature(x="H2OParsedDataVA", value="H2OParsedDataVA"), 
  function(x, value) { h2o.__remoteSend(x@h2o, h2o.__PAGE_COLNAMES, target=x@key, source=value@key); return(x) })

setMethod("colnames<-", signature(x="H2OParsedDataVA", value="character"),
  function(x, value) {
    if(length(value) != ncol(x)) stop("Mismatched column dimensions!")
    stop("Unimplemented"); return(x)
})

# ----------------------- Log helper ----------------------- #
h2o.logAndEcho <- function(conn, message) {
  if (class(conn) != "H2OClient")
      stop("conn must be an H2OClient")
  if (class(message) != "character")
      stop("message must be a character string")
  
  res = h2o.__remoteSend(conn, h2o.__PAGE_LOG_AND_ECHO, message=message)
  echo_message = res$message
  return (echo_message)
}
