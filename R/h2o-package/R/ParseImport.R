# Unique methods to H2O
# H2O client management operations

.readableTime <- function(epochTimeMillis) {
  days = epochTimeMillis/(24*60*60*1000)
  hours = (days-trunc(days))*24
  minutes = (hours-trunc(hours))*60
  seconds = (minutes-trunc(minutes))*60
  milliseconds = (seconds-trunc(seconds))*1000
  durationVector = trunc(c(days,hours,minutes,seconds,milliseconds))
  names(durationVector) = c("days","hours","minutes","seconds","milliseconds")
  if(length(durationVector[durationVector > 0]) > 1)
    showVec <- durationVector[durationVector > 0][1:2]
  else
    showVec <- durationVector[durationVector > 0]
  x1 = as.numeric(showVec)
  x2 = names(showVec)
  return(paste(x1,x2))
}

h2o.clusterInfo <- function(client) {
  if(missing(client) || class(client) != "H2OClient") stop("client must be a H2OClient object")

  .h2o.__checkUp(client)
  myURL = paste("http://", client@ip, ":", client@port, "/", .h2o.__PAGE_CLOUD, sep = "")

  res = NULL
  {
    res = fromJSON(postForm(myURL, .params = list(quiet="true", skip_ticks="true"), style = "POST", .opts = curlOptions(useragent=R.version.string)))

    nodeInfo = res$nodes
    numCPU = sum(sapply(nodeInfo,function(x) as.numeric(x['num_cpus'])))

    if (numCPU == 0) {
      # If the cloud hasn't been up for a few seconds yet, then query again.
      # Sometimes the heartbeat info with cores and memory hasn't had a chance
      # to post it's information yet.
      threeSeconds = 3
      Sys.sleep(threeSeconds)
      res = fromJSON(postForm(myURL, style = "POST", .opts = curlOptions(useragent=R.version.string)))
    }  
  }
  
  nodeInfo = res$nodes
  maxMem = sum(sapply(nodeInfo,function(x) as.numeric(x['max_mem_bytes']))) / (1024 * 1024 * 1024)
  numCPU = sum(sapply(nodeInfo,function(x) as.numeric(x['num_cpus'])))
  allowedCPU = sum(sapply(nodeInfo,function(x) as.numeric(x['cpus_allowed'])))
  clusterHealth = all(sapply(nodeInfo,function(x) as.logical(x['num_cpus']))==TRUE)
  
  cat("R is connected to H2O cluster:\n")
  cat("    H2O cluster uptime:        ", .readableTime(as.numeric(res$cloud_uptime_millis)), "\n")
  cat("    H2O cluster version:       ", res$version, "\n")
  cat("    H2O cluster name:          ", res$cloud_name, "\n")
  cat("    H2O cluster total nodes:   ", res$cloud_size, "\n")
  cat("    H2O cluster total memory:  ", sprintf("%.2f GB", maxMem), "\n")
  cat("    H2O cluster total cores:   ", numCPU, "\n")
  cat("    H2O cluster allowed cores: ", allowedCPU, "\n")
  cat("    H2O cluster healthy:       ", clusterHealth, "\n")
  
  cpusLimited = sapply(nodeInfo, function(x) { x[['num_cpus']] > 1 && x[['nthreads']] != 1 && x[['cpus_allowed']] == 1 })
  if(any(cpusLimited))
    warning("Number of CPU cores allowed is limited to 1 on some nodes.  To remove this limit, set environment variable 'OPENBLAS_MAIN_FREE=1' before starting R.")
}

h2o.ls <- function(object, pattern = "") {
  if(class(object) != "H2OClient") stop("object must be of class H2OClient")
  if(!is.character(pattern)) stop("pattern must be of class character")
  
  i = 0
  myList = list()
  page_keys = .MAX_INSPECT_ROW_VIEW

  # Need to pull all keys from every page in StoreView
  while(page_keys == .MAX_INSPECT_ROW_VIEW) {
    res = .h2o.__remoteSend(object, .h2o.__PAGE_VIEWALL, filter=pattern, offset=i*.MAX_INSPECT_ROW_VIEW, view=.MAX_INSPECT_ROW_VIEW)
    if(length(res$keys) == 0) return(myList)
    temp = lapply(res$keys, function(y) c(y$key, y$value_size_bytes))

    i = i + 1
    myList = c(myList, temp)
    page_keys = res$num_keys
  }
  tot_keys = page_keys + (i-1)*.MAX_INSPECT_ROW_VIEW

  temp = data.frame(matrix(unlist(myList), nrow=tot_keys, ncol=2, byrow = TRUE))
  colnames(temp) = c("Key", "Bytesize")
  temp$Key = as.character(temp$Key)
  temp$Bytesize = as.numeric(as.character(temp$Bytesize))
  return(temp)
}

h2o.rm <- function(object, keys) {
  if(class(object) != "H2OClient") stop("object must be of class H2OClient")
  if(!is.character(keys)) stop("keys must be of class character")

  for(i in 1:length(keys))
    .h2o.__remoteSend(object, .h2o.__PAGE_REMOVE, key=keys[[i]])
}

h2o.assign <- function(data, key) {
  if(class(data) != "H2OParsedData") stop("data must be of class H2OParsedData")
  if(!is.character(key)) stop("key must be of class character")
  if(nchar(key) == 0) stop("key cannot be an empty string")
  if(key == data@key) stop(paste("Destination key must differ from data key", data@key))
  .h2o.exec2(expr = data@key, h2o = data@h2o, dest_key = key)
}

h2o.createFrame <- function(object, key, rows = 10000, cols = 10, seed, randomize = TRUE, value = 0, real_range = 100, categorical_fraction = 0.2, factors = 100, integer_fraction = 0.2, integer_range = 100, binary_fraction = 0.1, binary_ones_fraction = 0.02, missing_fraction = 0.01, response_factors = 2, has_response = FALSE) {
  if(class(object) != "H2OClient") stop("object must be of class H2OClient")
  if(!is.character(key)) stop("key must be a character string")
  if(!is.numeric(rows)) stop("rows must be a numeric value")
  if(!is.numeric(cols)) stop("cols must be a numeric value")
  if(!missing(seed) && !is.numeric(seed)) stop("seed must be a numeric value")
  if(!is.logical(randomize)) stop("randomize must be a logical value")
  if(!is.numeric(value)) stop("value must be a numeric value")
  if(!is.numeric(real_range)) stop("real_range must be a numeric value")
  if(!is.numeric(categorical_fraction)) stop("categorical_fraction must be a numeric value")
  if(!is.numeric(factors)) stop("factors must be a numeric value")
  if(!is.numeric(integer_fraction)) stop("integer_fraction must be a numeric value")
  if(!is.numeric(integer_range)) stop("integer_range must be a numeric value")
  if(!is.numeric(missing_fraction)) stop("missing_fraction must be a numeric value")
  if(!is.numeric(response_factors)) stop("response_factors must be a numeric value")
  if(!is.numeric(binary_fraction)) stop("binary_fraction must be a numeric value")
  if(!is.numeric(binary_ones_fraction)) stop("binary_ones_fraction must be a numeric value")
  if(!is.logical(has_response)) stop("has_response must be a logical value")

  if(missing(seed))
    res <- .h2o.__remoteSend(object, .h2o.__PAGE_CreateFrame, key = key, rows = rows, cols = cols, randomize = as.numeric(randomize), value = value, real_range = real_range,
                             categorical_fraction = categorical_fraction, factors = factors, integer_fraction = integer_fraction, integer_range = integer_range, binary_fraction = binary_fraction, 
                             binary_ones_fraction = binary_ones_fraction, missing_fraction = missing_fraction, response_factors = response_factors, has_response = as.numeric(has_response))
  else
    res <- .h2o.__remoteSend(object, .h2o.__PAGE_CreateFrame, key = key, rows = rows, cols = cols, seed = seed, randomize = as.numeric(randomize), value = value, real_range = real_range,
                           categorical_fraction = categorical_fraction, factors = factors, integer_fraction = integer_fraction, integer_range = integer_range, binary_fraction = binary_fraction, 
                           binary_ones_fraction = binary_ones_fraction, missing_fraction = missing_fraction, response_factors = response_factors, has_response = as.numeric(has_response))
  .h2o.exec2(expr = key, h2o = object, dest_key = key)
}

h2o.interaction <- function(data, key=NULL, factors, pairwise, max_factors, min_occurrence) {
  if(class(data) != "H2OParsedData") stop("data must be of class H2OParsedData")
  if(missing(factors)) stop("factors must be specified")
  if(!is.logical(pairwise)) stop("pairwise must be a boolean value")
  if(missing(max_factors)) stop("max_factors must be specified")
  if(missing(min_occurrence)) stop("min_occurrence must be specified")

  if (is.list(factors)) {
    res <- lapply(factors, function(factor) h2o.interaction(data, key=NULL, factor, pairwise, max_factors, min_occurrence))
    if (!is.null(key)) {
      old <- cbind.H2OParsedData(res)
      new <- h2o.assign(old, key)
      h2o.rm(data@h2o, old@key)
      return(new)
    } else {
      return(cbind.H2OParsedData(res))
    }
  }

  if(!is.numeric(factors)) factors <- match(factors,colnames(data))
  if(!is.numeric(factors)) stop("factors must be a numeric value")
  if(is.null(factors)) stop("factors not found")
  if(max_factors < 1) stop("max_factors cannot be < 1")
  if(!is.numeric(max_factors)) stop("max_factors must be a numeric value")
  if(min_occurrence < 1) stop("min_occurrence cannot be < 1")
  if(!is.numeric(min_occurrence)) stop("min_occurrence must be a numeric value")

  factors <- factors - 1 # make 0-based for Java
  res <- .h2o.__remoteSend(data@h2o, .h2o.__PAGE_Interaction, source = data@key, destination_key = key, factors = factors, pairwise = as.numeric(pairwise), max_factors = max_factors, min_occurrence = min_occurrence)
  h2o.getFrame(data@h2o, res$destination_key)
}

h2o.rebalance <- function(data, chunks, key) {
  if(class(data) != "H2OParsedData") stop("data must be of class H2OParsedData")
  if(!is.numeric(chunks)) stop("chunks must be a numeric value")
  if(chunks < 1) stop("chunks cannot be < 1")
  if(missing(key))
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_ReBalance, source = data@key, chunks = chunks)
  else
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_ReBalance, source = data@key, after = key, chunks = chunks)
  h2o.getFrame(data@h2o, res$after)
}

h2o.splitFrame <- function(data, ratios = 0.75, shuffle = FALSE) {
  if(class(data) != "H2OParsedData") stop("data must be of class H2OParsedData")
  if(!is.numeric(ratios)) stop("ratios must be numeric")
  if(any(ratios < 0 | ratios > 1)) stop("ratios must be between 0 and 1 exclusive")
  if(sum(ratios) >= 1) stop("sum of ratios must be strictly less than 1")
  if(!is.logical(shuffle)) stop("shuffle must be a logical value")

  res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_SplitFrame, source = data@key, ratios = ratios, shuffle = as.numeric(shuffle))
  lapply(res$split_keys, function(key) { .h2o.exec2(expr = key, h2o = data@h2o, dest_key = key) })
}

h2o.nFoldExtractor <- function(data, nfolds, fold_to_extract) {
  if(class(data) != "H2OParsedData") stop("data must be of class H2OParsedData")
  if(!is.numeric(nfolds)) stop("nfolds must be numeric")
  if(nfolds <= 1) stop("nfolds must be greater or equal to 2")
  if(!is.numeric(fold_to_extract)) stop("fold_to_extract must be numeric")
  if(fold_to_extract < 1) stop("fold_to_extract must be greater or equal to 1")
  if(fold_to_extract > nfolds) stop("fold_to_extract must be less or equal to nfolds")

  res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_NFoldExtractor, source = data@key, nfolds = nfolds, afold = fold_to_extract - 1 ) #R indexing is 1-based, Java is 0-based
  lapply(res$split_keys, function(key) { .h2o.exec2(expr = key, h2o = data@h2o, dest_key = key) })
}

h2o.insertMissingValues <- function(data, fraction = 0.01, seed = -1) {
  if(class(data) != "H2OParsedData") stop("data must be of class H2OParsedData")
  if(!is.numeric(fraction)) stop("fraction must be numeric")
  if(any(fraction <= 0 | fraction > 1)) stop("fraction must be in (0,1]")
  if(!missing(seed) && !is.numeric(seed)) stop("seed must be numeric")
  
  if(missing(seed) || seed == -1)
    res <- .h2o.__remoteSend(data@h2o, .h2o.__PAGE_MissingVals, key = data@key, missing_fraction = fraction)
  else
    res <- .h2o.__remoteSend(data@h2o, .h2o.__PAGE_MissingVals, key = data@key, seed = seed, missing_fraction = fraction)
  .h2o.exec2(expr = data@key, h2o = data@h2o, dest_key = data@key)
}

# ----------------------------------- File Import Operations --------------------------------- #
# WARNING: You must give the FULL file/folder path name! Relative paths are taken with respect to the H2O server directory
# ----------------------------------- Import Folder --------------------------------- #  
h2o.importFolder <- function(object, path, pattern = "", key = "", parse = TRUE, header, header_with_hash, sep = "", col.names, parser_type = "AUTO") {
  if(class(object) != "H2OClient") stop("object must be of class H2OClient")
  if(!is.character(path)) stop("path must be of class character")
  if(nchar(path) == 0) stop("path must be a non-empty string")
  if(!is.character(pattern)) stop("pattern must be of class character")
  if(!is.character(key)) stop("key must be of class character")
  if(nchar(key) > 0 && regexpr("^[a-zA-Z_][a-zA-Z0-9_.]*$", key)[1] == -1)
    stop("key must match the regular expression '^[a-zA-Z_][a-zA-Z0-9_.]*$'")
  if(!is.logical(parse)) stop("parse must be of class logical")
  
  # if(!file.exists(path)) stop("Directory does not exist!")
  # res = .h2o.__remoteSend(object, .h2o.__PAGE_IMPORTFILES2, path=normalizePath(path))
  res <- .h2o.__remoteSend(object, .h2o.__PAGE_IMPORTFILES2, path=path)
  if(length(res$fails) > 0) {
    for(i in 1:length(res$fails)) 
      cat(res$fails[[i]], "failed to import")
  }
  
  # Return only the files that successfully imported
  if(length(res$files) > 0) {
    if(parse) {
      if(substr(path, nchar(path), nchar(path)) == .Platform$file.sep)
        path <- substr(path, 1, nchar(path)-1)
      ## Removed regPath that relies on front end input, takes backend input to create regPath
      ## There is no pattern option for main h2o.importFile function
      if(pattern=="") {regPath <- sub(pattern = basename(res$files[[1]]), replacement = "", x = res$files[[1]])
      } else {regPath <- paste(path, pattern, sep = .Platform$file.sep)}
      srcKey  <- ifelse(length(res$keys) == 1, res$keys[[1]], paste("*", regPath, "*", sep=""))
      rawData <- new("H2ORawData", h2o=object, key=srcKey)
      h2o.parseRaw(data=rawData, key=key, header=header, header_with_hash=header_with_hash, sep=sep, col.names=col.names, parser_type = parser_type)
    } else {
      myData <- lapply(res$keys, function(x) { new("H2ORawData", h2o=object, key=x) })
      if(length(res$keys) == 1) myData[[1]] else myData
    }
  } else stop("All files failed to import!")
}

# ----------------------------------- Import File --------------------------------- #
h2o.importFile <- function(object, path, key = "", parse = TRUE, header, header_with_hash, sep = "", col.names, parser_type = "AUTO") {
  h2o.importFolder(object, path, pattern = "", key, parse, header, header_with_hash, sep, col.names, parser_type = parser_type)
}

# ----------------------------------- Import URL --------------------------------- #
h2o.importURL <- function(object, path, key = "", parse = TRUE, header, header_with_hash, sep = "", col.names, parser_type = "AUTO") {
  print("This function has been deprecated. In the future, please use h2o.importFile with a http:// prefix instead.")
  h2o.importFile(object, path, key, parse, header, header_with_hash, sep, col.names, parser_type = parser_type)
}

# ----------------------------------- Import HDFS --------------------------------- #
h2o.importHDFS <- function(object, path, pattern = "", key = "", parse = TRUE, header, header_with_hash, sep = "", col.names, parser_type = "AUTO") {
  print("This function has been deprecated. In the future, please use h2o.importFolder with a hdfs:// prefix instead.")
  h2o.importFolder(object, path, pattern, key, parse, header, header_with_hash, sep, col.names, parser_type = parser_type)
}

# ----------------------------------- Upload File --------------------------------- #
h2o.uploadFile <- function(object, path, key = "", parse = TRUE, header, header_with_hash, sep = "", col.names, silent = TRUE, parser_type = "AUTO") {
  if(class(object) != "H2OClient") stop("object must be of class H2OClient")
  if(!is.character(path)) stop("path must be of class character")
  if(nchar(path) == 0) stop("path must be a non-empty string")
  if(!is.character(key)) stop("key must be of class character")
  if(nchar(key) > 0 && regexpr("^[a-zA-Z_][a-zA-Z0-9_.]*$", key)[1] == -1)
    stop("key must match the regular expression '^[a-zA-Z_][a-zA-Z0-9_.]*$'")
  if(!is.logical(parse)) stop("parse must be of class logical")
  if(!is.logical(silent)) stop("silent must be of class logical")
  
  url = paste("http://", object@ip, ":", object@port, "/2/PostFile.json", sep="")
  url = paste(url, "?key=", URLencode(path), sep="")
  if(file.exists(h2o.getLogPath("Command"))) .h2o.__logIt(url, NULL, "Command")
  if(silent)
    temp = postForm(url, .params = list(fileData = fileUpload(normalizePath(path))), .opts = curlOptions(useragent=R.version.string))
  else
    temp = postForm(url, .params = list(fileData = fileUpload(normalizePath(path))), .opts = curlOptions(verbose = TRUE, useragent=R.version.string))
  rawData = new("H2ORawData", h2o=object, key=path)
  if(parse) parsedData = h2o.parseRaw(data=rawData, key=key, header=header, header_with_hash=header_with_hash, sep=sep, col.names=col.names, parser_type = parser_type) else rawData
}

# ----------------------------------- File Parse Operations --------------------------------- #
h2o.parseRaw <- function(data, key = "", header, header_with_hash, sep = "", col.names, parser_type = "AUTO") {
  if(class(data) != "H2ORawData") stop("data must be of class H2ORawData")
  if(!is.character(key)) stop("key must be of class character")
  if(nchar(key) > 0 && regexpr("^[a-zA-Z_][a-zA-Z0-9_.]*$", key)[1] == -1)
    stop("key must match the regular expression '^[a-zA-Z_][a-zA-Z0-9_.]*$'")
  if(!(missing(header) || is.logical(header))) stop(paste("header cannot be of class", class(header)))
  if(!(missing(header_with_hash) || is.logical(header_with_hash))) stop(paste("header_with_hash cannot be of class", class(header_with_hash)))
  if(!is.character(sep)) stop("sep must be of class character")
  if(!(missing(col.names) || class(col.names) == "H2OParsedData")) stop(paste("col.names cannot be of class", class(col.names)))
  
  # If both header and column names missing, then let H2O guess if header exists
  sepAscii <- ifelse(sep == "", sep, strtoi(charToRaw(sep), 16L))
  if(missing(header) && missing(header_with_hash) && missing(col.names))
    res <- .h2o.__remoteSend(data@h2o, .h2o.__PAGE_PARSE2, source_key=data@key, destination_key=key, separator=sepAscii, parser_type=parser_type)
  else if(missing(header) && !missing(header_with_hash) && missing(col.names))
    res <- .h2o.__remoteSend(data@h2o, .h2o.__PAGE_PARSE2, source_key=data@key, destination_key=key, separator=sepAscii, header_with_hash=as.numeric(header_with_hash), parser_type=parser_type)
  else if(missing(header) && missing(header_with_hash) && !missing(col.names))
    res <- .h2o.__remoteSend(data@h2o, .h2o.__PAGE_PARSE2, source_key=data@key, destination_key=key, separator=sepAscii, header=1, header_from_file=col.names@key, parser_type=parser_type)
  else if(missing(header) && !missing(header_with_hash) && !missing(col.names))
    res <- .h2o.__remoteSend(data@h2o, .h2o.__PAGE_PARSE2, source_key=data@key, destination_key=key, separator=sepAscii, header_with_hash=as.numeric(header_with_hash), header_from_file=col.names@key, parser_type=parser_type)
  else if(!missing(header) && missing(header_with_hash) && missing(col.names))
    res <- .h2o.__remoteSend(data@h2o, .h2o.__PAGE_PARSE2, source_key=data@key, destination_key=key, separator=sepAscii, header=as.numeric(header), parser_type=parser_type)
  else if(!missing(header) && !missing(header_with_hash) && missing(col.names))
    res <- .h2o.__remoteSend(data@h2o, .h2o.__PAGE_PARSE2, source_key=data@key, destination_key=key, separator=sepAscii, header=as.numeric(header), header_with_hash=as.numeric(header_with_hash), parser_type=parser_type)
  else if(!missing(header) && missing(header_with_hash) && !missing(col.names))
    res <- .h2o.__remoteSend(data@h2o, .h2o.__PAGE_PARSE2, source_key=data@key, destination_key=key, separator=sepAscii, header=as.numeric(header), header_from_file=col.names@key, parser_type=parser_type)
  else  #(!missing(header) && !missing(header_with_hash) && !missing(col.names))
    res <- .h2o.__remoteSend(data@h2o, .h2o.__PAGE_PARSE2, source_key=data@key, destination_key=key, separator=sepAscii, header=as.numeric(header), header_with_hash=as.numeric(header_with_hash), header_from_file=col.names@key, parser_type=parser_type)
  
  # on.exit(.h2o.__cancelJob(data@h2o, res$job_key))
  .h2o.__waitOnJob(data@h2o, res$job_key)
  .h2o.exec2(expr = res$destination_key, h2o = data@h2o, dest_key = res$destination_key)
#  parsedData <- new("H2OParsedData", h2o=data@h2o, key=res$destination_key, col_names = .getColNames(res), nrows = .getRows(res), ncols = .getCols(res))
}
      
#-------------------------------- Miscellaneous -----------------------------------#
h2o.exportFile <- function(data, path, force = FALSE) {
    canHandle = FALSE
    if (class(data) == "H2OParsedData") { canHandle = TRUE }
    if (! canHandle) {
        stop("h2o.exportFile only works on H2OParsedData frames")
    }
    if(!is.character(path)) stop("path must be of class character")
    if(nchar(path) == 0) stop("path must be a non-empty string")
    if(!is.logical(force)) stop("force must be of class logical")
    
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_EXPORTFILES, src_key = data@key, path = path, force = as.numeric(force))
}

h2o.downloadCSV <- function(data, filename, quiet = FALSE) {
  if( missing(data)) stop('Must specify data')
  if(class(data) != "H2OParsedData") stop('data must be an H2OParsedData object')
  if( missing(filename) ) stop('Must specify filename')
  if(!is.character(filename)) stop("filename must be a character string")
  if(!is.logical(quiet)) stop("quiet must be a logical value")
  
  csv_url <- paste('http://', data@h2o@ip, ':', data@h2o@port, '/2/DownloadDataset?src_key=', data@key, sep='')
  download.file(csv_url, destfile = filename, mode = "w", cacheOK = FALSE, quiet = quiet)
}

# ----------------------------------- Work in Progress --------------------------------- #
# setGeneric("h2o<-", function(x, value) { standardGeneric("h2o<-") })
# setMethod("h2o<-", signature(x="H2OParsedData", value="H2OParsedData"), function(x, value) {
#   res = .h2o.__exec2_dest_key(x@h2o, value@key, x@key); return(x)
# })
# 
# setMethod("h2o<-", signature(x="H2OParsedData", value="numeric"), function(x, value) {
#   res = .h2o.__exec2_dest_key(x@h2o, paste("c(", paste(value, collapse=","), ")", sep=""), x@key); return(x)
# })

# ----------------------- Log helper ----------------------- #
h2o.logAndEcho <- function(conn, message) {
  if(class(conn) != "H2OClient") stop("conn must be an H2OClient")
  if(!is.character(message)) stop("message must be a character string")
  
  res = .h2o.__remoteSend(conn, .h2o.__PAGE_LOG_AND_ECHO, message=message)
  echo_message = res$message
  return(echo_message)
}

h2o.downloadAllLogs <- function(client, dirname = ".", filename = NULL) {
  if(class(client) != "H2OClient") stop("client must be of class H2OClient")
  if(!is.character(dirname)) stop("dirname must be of class character")
  if(!is.null(filename)) {
    if(!is.character(filename)) stop("filename must be of class character")
    else if(nchar(filename) == 0) stop("filename must be a non-empty string")
  }
  
  url = paste("http://", client@ip, ":", client@port, "/", .h2o.__DOWNLOAD_LOGS, sep="")
  if(!file.exists(dirname)) dir.create(dirname)
  
  cat("Downloading H2O logs from server...\n")
  h = basicHeaderGatherer()
  tempfile = getBinaryURL(url, headerfunction = h$update, verbose = TRUE)
  
  # Get filename from HTTP header of response
  if(is.null(filename)) {
    # temp = strsplit(as.character(Sys.time()), " ")[[1]]
    # myDate = gsub("-", "", temp[1]); myTime = gsub(":", "", temp[2])
    # file_name = paste("h2ologs_", myDate, "_", myTime, ".zip", sep="")
    atch = h$value()[["Content-Disposition"]]
    ind = regexpr("filename=", atch)[[1]]
    if(ind == -1) stop("Header corrupted: Expected attachment filename in Content-Disposition")
    filename = substr(atch, ind+nchar("filename="), nchar(atch))
  }
  myPath = paste(normalizePath(dirname), filename, sep = .Platform$file.sep)
  
  cat("Writing H2O logs to", myPath, "\n")
  # download.file(url, destfile = myPath)
  writeBin(tempfile, myPath)
}

# ------------------- Show H2O recommended columns to ignore ----------------------------------------------------
h2o.ignoreColumns <- function(data, max_na = 0.2) {
  if(missing(data)) stop('Must specify object')
  if(class(data) != 'H2OParsedData') stop('object not a h2o data type')
  
  res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_INSPECT2, src_key=data@key)
  
  numRows = res$numRows
  naThreshold = numRows * max_na
  cardinalityThreshold = numRows
  
  columns = res$cols
  foo <- function(col){
    if(col$type != 'Enum'){ #If Numeric Column
      # If min=max, only one value in entire column, if naCnt higher than 20% of entries
      if(col$min==col$max || col$naCnt >= naThreshold) col$name
    } else{ #If Categorical Column
      if(col$cardinality==cardinalityThreshold || col$naCnt >= naThreshold) col$name
    }
  }
  ignore = sapply(columns, foo)
  unlist(ignore)
}


# ------------------- Save H2O Model to Disk ----------------------------------------------------
h2o.saveModel <- function(object, dir="", name="",save_cv=TRUE, force=FALSE) {
    if(missing(object)) stop('Must specify object')
    if(!inherits(object,'H2OModel')) stop('object must be an H2O model')
    if(!is.character(dir)) stop('path must be of class character')
    if(!is.character(name)) stop('name must be of class character')
    #filename taken out because models save in own directory with its cross-valid models
    #if(!is.character(filename)) stop('filename must be of class character')
    if(!is.logical(force)) stop('force must be either TRUE or FALSE')
    if(!is.logical(save_cv)) stop('save_cv must be either TRUE or FALSE')
    if(nchar(name) == 0) name = object@key

    force = ifelse(force==TRUE, 1, 0)
    save_cv = ifelse(save_cv==TRUE, 1, 0)
    # Create a model directory for each model saved that will include main model
    # any cross validation models and a meta text file with all the model names listed
    model_dir <- paste(dir, name, sep=.Platform$file.sep)
    #dir.create(model_dir,showWarnings = F)
    
    # Save main model
    path <- paste(model_dir, sep=.Platform$file.sep)
    res <- .h2o.__remoteSend(object@data@h2o, .h2o.__PAGE_SaveModel, model=object@key, path=path, force=force, save_cv=save_cv)
    
    # Save all cross validation models
    if (.hasSlot(object, "xval")) {
      xval_keys <- sapply(object@xval,function(model) model@key )
      if(save_cv & !(length(xval_keys)==0)) {
        save_cv <- TRUE
#        for (xval_key in xval_keys) .h2o.__remoteSend(object@data@h2o, .h2o.__PAGE_SaveModel, model=xval_key, path=paste(model_dir, xval_key, sep=.Platform$file.sep), force=force)
      } else {
        save_cv <- FALSE # do not save CV results if they do not exist
      }
    } else {
      save_cv <- FALSE # if no xval slot (Naive Bayes) no CV models
    }    
    res$path
}

# ------------------- Save All H2O Model to Disk --------------------------------------------------

h2o.saveAll <- function(object, dir="", save_cv=TRUE, force=FALSE) {
  if(missing(object)) stop('Must specify object')
  if(class(object) != 'H2OClient') stop('object must be of class H2OClient')
  if(!is.logical(save_cv)) stop('save_cv needs to be a boolean')
  if(!is.logical(force)) stop('force needs to be a boolean')
  
  ## Grab all the model keys in H2O
  res = .h2o.__remoteSend(client = object, page = .h2o.__PAGE_ALLMODELS)
  keys = names(res$models)
  
  ## Delete Duplicate Keys (this will avoid saving cross validation models multiple times for non-GLM models)
  duplicates = {}
  for(key in keys) { dups = grep(pattern = paste(key, "_", sep = ""), x = keys)
    duplicates = append(x = duplicates, values = dups)
  }
  keys = if(length(duplicates) > 0) keys[-duplicates] else keys
  
  ## Create H2OModel objects in R (To grab the cross validation models)
  models = lapply(keys, function(model_key) h2o.getModel(h2o = object, key = model_key))
  m_path = sapply(models, function(model_obj) h2o.saveModel(model_obj, dir=dir, save_cv=save_cv, force=force) )
  m_path
}


# ------------------- Load H2O Model from Disk ----------------------------------------------------
h2o.loadModel <- function(object, path="") {
    if(missing(object)) stop('Must specify object')
    if(class(object) != 'H2OClient') stop('object must be of class H2OClient')
    if(!is.character(path)) stop('path must be of class character')

    # Load all model_names into H2O
    res = .h2o.__remoteSend(object, .h2o.__PAGE_LoadModel, path = path)
    modelKey = res$model$"_key"
    h2o.getModel(object, modelKey)
}

# ------------------- Load All H2O Model in a directory from Disk -----------------------------------------------
h2o.loadAll <- function(object, dir="") {
  if(missing(object)) stop('Must specify object')
  if(class(object) != 'H2OClient') stop('object must be of class H2OClient')
  if(!is.character(dir)) stop('dir must be of class character')
    
  model_dirs = setdiff(list.dirs(dir), dir)
  model_objs = {}
  for(model_dir in model_dirs) {
    print(paste("Loading ", basename(model_dir), "....",sep = ""))
    temp_model = h2o.loadModel(object, path = model_dir)
    model_objs = append(x = model_objs, values = temp_model)
  }
  
  model_objs
}

# ------------------- Remove vector from a data frame -----------------------------------------------

h2o.removeVecs <- function(data, cols) {
  if (missing(data)) stop('Must specify data object')
  if (class(data) != 'H2OParsedData') stop('object must be of class H2OParsedData')
  verified_cols <- .verify_datacols(data = data, cols = cols)
  inds <- verified_cols$cols_ind - 1
  .h2o.__remoteSend(data@h2o,.h2o.__PAGE_RemoveVec, source=data@key, cols=inds)
  data <- h2o.getFrame(h2o = data@h2o, key = data@key)
}

# ------------- Return the indices of the top or bottom values of column(s) -----------------

h2o.order <- function(data, cols, n = 5, decreasing = T) {
  if (missing(data)) stop('Must specify data object')
  if (class(data) != 'H2OParsedData') stop('object must be of class H2OParsedData')
  if (!is.numeric(n)) stop('n must be a integer')
  if (!is.logical(decreasing)) stop('decreasing must be a boolean')
  if (missing(cols)) cols <- 1:ncol(data)
  rev <- if (decreasing) 1 else 0
  
  verified_cols <- .verify_datacols(data = data, cols = cols)
  inds <- verified_cols$cols_ind - 1
  res <- .h2o.__remoteSend(data@h2o,.h2o.__PAGE_Order, source=data@key, cols=inds, n = n, rev = rev)
  h2o.getFrame(data@h2o, res$destination_key)
}