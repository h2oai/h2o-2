# Hack to get around Exec.json always dumping to same Result.hex key
# TODO: Need better way to manage temporary/intermediate values in calculations! Right now, overwriting occurs silently
pkg.env = new.env()
pkg.env$result_count = 0
pkg.env$temp_count = 0
pkg.env$IS_LOGGING = FALSE
TEMP_KEY = "Last.value"
RESULT_MAX = 200
LOGICAL_OPERATORS = c("==", ">", "<", "!=", ">=", "<=", "&", "|", "&&", "||", "!", "is.na")

# Initialize functions for R logging
myPath = paste(Sys.getenv("HOME"), "Library/Application Support/h2o", sep="/")
if(Sys.info()["sysname"] == "Windows")
  myPath = paste(Sys.getenv("APPDATA"), "h2o", sep="/")
pkg.env$h2o.__LOG_COMMAND = paste(myPath, "h2o_commands.log", sep="/")
pkg.env$h2o.__LOG_ERROR = paste(myPath, "h2o_error_json.log", sep="/")
h2o.__getCommandLog <- function() { return(pkg.env$h2o.__LOG_COMMAND)}
h2o.__getErrorLog <- function() { return(pkg.env$h2o.__LOG_ERROR)}
h2o.__changeCommandLog <- function(path) { 
    cmd <- paste(path, 'commands.log', sep='/') 
    assign("h2o.__LOG_COMMAND", cmd, envir = pkg.env)
    }
h2o.__changeErrorLog <- function(path) { 
    cmd <- paste(path, 'errors.log', sep='/')
    assign("h2o.__LOG_ERROR", cmd, envir = pkg.env)
    }
h2o.__startLogging     <- function() { assign("IS_LOGGING", TRUE, envir = pkg.env) }
h2o.__stopLogging      <- function() { assign("IS_LOGGING", FALSE, envir = pkg.env) }
h2o.__clearLogs        <- function() { unlink(pkg.env$h2o.__LOG_COMMAND); unlink(pkg.env$h2o.__LOG_ERROR) }
h2o.__openCmdLog       <- function() {
  myOS = Sys.info()["sysname"]
  if(myOS == "Windows") shell.exec(paste("open '", pkg.env$h2o.__LOG_COMMAND, "'", sep="")) 
  else system(paste("open '", pkg.env$h2o.__LOG_COMMAND, "'", sep=""))
}
h2o.__openErrLog <- function() {
  myOS = Sys.info()["sysname"]
  if(myOS == "Windows") shell.exec(paste("open '", pkg.env$h2o.__LOG_ERROR, "'", sep="")) 
  else system(paste("open '", pkg.env$h2o.__LOG_ERROR, "'", sep=""))
}

h2o.__logIt <- function(m, tmp, commandOrErr) {
  #m is a url if commandOrErr == "Command"
  if(is.null(tmp) || is.null(get("tmp"))) s <- m
  else {
    tmp <- get("tmp"); nams = names(tmp)
    if(length(nams) != length(tmp)) {
        if (is.null(nams) && commandOrErr != "Command") nams = "[WARN/ERROR]"
    }
    s <- rep(" ", max(length(tmp), length(nams)))
    for(i in seq_along(tmp)){
      s[i] <- paste(nams[i], ": ", tmp[[i]], sep="", collapse = " ")
    }
    s <- paste(m, ' \t', paste(s, collapse=", "))
  }
  if (commandOrErr != "Command") s <- paste(s, '\n')
  write(s, file = ifelse(commandOrErr == "Command", pkg.env$h2o.__LOG_COMMAND, pkg.env$h2o.__LOG_ERROR), append = TRUE)
}

# Internal functions & declarations
h2o.__PAGE_CANCEL = "Cancel.json"
h2o.__PAGE_CLOUD = "Cloud.json"
h2o.__PAGE_COLNAMES = "SetColumnNames.json"
h2o.__PAGE_GET = "GetVector.json"
h2o.__PAGE_IMPORTURL = "ImportUrl.json"
h2o.__PAGE_IMPORTFILES = "ImportFiles.json"
h2o.__PAGE_IMPORTHDFS = "ImportHdfs.json"
h2o.__PAGE_EXPORTHDFS = "ExportHdfs.json"
h2o.__PAGE_INSPECT = "Inspect.json"
h2o.__PAGE_JOBS = "Jobs.json"
h2o.__PAGE_PARSE = "Parse.json"
h2o.__PAGE_PREDICT = "GeneratePredictionsPage.json"
h2o.__PAGE_PUT = "PutVector.json"
h2o.__PAGE_REMOVE = "Remove.json"
h2o.__PAGE_REMOVEALL = "2/RemoveAll.json"
h2o.__PAGE_SUMMARY = "SummaryPage.json"
h2o.__PAGE_VIEWALL = "StoreView.json"
h2o.__DOWNLOAD_LOGS = "LogDownload.json"

h2o.__PAGE_GLM = "GLM.json"
h2o.__PAGE_GLMProgress = "GLMProgressPage.json"
h2o.__PAGE_GLMGrid = "GLMGrid.json"
h2o.__PAGE_GLMGridProgress = "GLMGridProgress.json"
h2o.__PAGE_KMEANS = "KMeans.json"
h2o.__PAGE_KMAPPLY = "KMeansApply.json"
h2o.__PAGE_KMSCORE = "KMeansScore.json"
h2o.__PAGE_RF  = "RF.json"
h2o.__PAGE_RFVIEW = "RFView.json"
h2o.__PAGE_RFTREEVIEW = "RFTreeView.json"

h2o.__PAGE_EXEC2 = "2/Exec2.json"
h2o.__PAGE_IMPORTFILES2 = "2/ImportFiles2.json"
h2o.__PAGE_INSPECT2 = "2/Inspect2.json"
h2o.__PAGE_PARSE2 = "2/Parse2.json"
h2o.__PAGE_PREDICT2 = "2/Predict.json"
h2o.__PAGE_SUMMARY2 = "2/SummaryPage2.json"
h2o.__PAGE_LOG_AND_ECHO = "2/LogAndEcho.json"
h2o.__HACK_LEVELS = "2/Levels.json"

h2o.__PAGE_DRF = "2/DRF.json"
h2o.__PAGE_DRFProgress = "2/DRFProgressPage.json"
h2o.__PAGE_DRFModelView = "2/DRFModelView.json"
h2o.__PAGE_GBM = "2/GBM.json"
h2o.__PAGE_GBMProgress = "2/GBMProgressPage.json"
h2o.__PAGE_GRIDSEARCH = "2/GridSearchProgress.json"
h2o.__PAGE_GBMModelView = "2/GBMModelView.json"
h2o.__PAGE_GLM2 = "2/GLM2.json"
h2o.__PAGE_GLM2Progress = "2/GLMProgress.json"
h2o.__PAGE_GLMModelView = "2/GLMModelView.json"
h2o.__PAGE_GLMValidView = "2/GLMValidationView.json"
h2o.__PAGE_GLM2GridView = "2/GLMGridView.json"
h2o.__PAGE_KMEANS2 = "2/KMeans2.json"
h2o.__PAGE_KM2Progress = "2/KMeans2Progress.json"
h2o.__PAGE_KM2ModelView = "2/KMeans2ModelView.json"
h2o.__PAGE_NN = "2/NeuralNet.json"
h2o.__PAGE_NNProgress = "2/NeuralNetProgress.json"
h2o.__PAGE_PCA = "2/PCA.json"
h2o.__PAGE_PCASCORE = "2/PCAScore.json"
h2o.__PAGE_PCAProgress = "2/PCAProgressPage.json"
h2o.__PAGE_PCAModelView = "2/PCAModelView.json"

h2o.__remoteSend <- function(client, page, ...) {
  h2o.__checkClientHealth(client)
  ip = client@ip
  port = client@port
  myURL = paste("http://", ip, ":", port, "/", page, sep="")

  # Log list of parameters sent to H2O
  if(pkg.env$IS_LOGGING) {
    h2o.__logIt(myURL, list(...), "Command")
  }
  
  # Sends the given arguments as URL arguments to the given page on the specified server
  #
  # Re-enable POST since we found the bug in NanoHTTPD which was causing POST
  # payloads to be dropped.
  #
  temp = postForm(myURL, style = "POST", ...)
  
  # The GET code that we used temporarily while NanoHTTPD POST was known to be busted.
  #
  #if(length(list(...)) == 0)
  #  temp = getURLContent(myURL)
  #else
  #  temp = getForm(myURL, ..., .checkParams = FALSE)   # Some H2O params overlap with Curl params
  
  # after = gsub("\\\\\\\"NaN\\\\\\\"", "NaN", temp[1]) 
  # after = gsub("NaN", "\"NaN\"", after)
  # after = gsub("-Infinity", "\"-Inf\"", temp[1])
  # after = gsub("Infinity", "\"Inf\"", after)
  after = gsub('"Infinity"', '"Inf"', temp[1])
  after = gsub('"-Infinity"', '"-Inf"', after)
  res = fromJSON(after)

  if (!is.null(res$error)) {
    if(pkg.env$IS_LOGGING) h2o.__writeToFile(res, pkg.env$h2o.__LOG_ERROR)
    stop(paste(myURL," returned the following error:\n", h2o.__formatError(res$error)))
  }
  res
}

h2o.__cloudSick <- function(node_name = NULL, client) {
  url <- paste("http://", client@ip, ":", client@port, "/Cloud.html", sep = "")
  m1 <- "Attempting to execute action on an unhealthy cluster!\n"
  m2 <- ifelse(node_name != NULL, paste("The sick node is identified to be: ", node_name, "\n", sep = "", collapse = ""), "")
  m3 <- paste("Check cloud status here: ", url, sep = "", collapse = "")
  m <- paste(m1, m2, "\n", m3, sep = "")
  stop(m)
}

h2o.__checkClientHealth <- function(client) {
  grabCloudStatus <- function(client) {
    ip <- client@ip
    port <- client@port
    url <- paste("http://", ip, ":", port, "/", h2o.__PAGE_CLOUD, sep = "")
    if(!url.exists(url)) stop(paste("H2O connection has been severed. Instance no longer up at address ", ip, ":", port, "/", sep = "", collapse = ""))
    fromJSON(getURLContent(url))
  }
  checker <- function(node, client) {
    status <- node$node_healthy
    elapsed <- node$elapsed_time
    nport <- unlist(strsplit(node$name, ":"))[2]
    if(!status) h2o.__cloudSick(node_name = node$name, client = client)
    if(elapsed > 45000) h2o.__cloudSick(node_name = NULL, client = client)
    if(elapsed > 10000) {
        Sys.sleep(5)
        lapply(grabCloudStatus(client)$nodes, checker, client)
    }
    return(0)
  }
  cloudStatus <- grabCloudStatus(client)
  if(!cloudStatus$cloud_healthy) h2o.__cloudSick(node_name = NULL, client = client)
  lapply(cloudStatus$nodes, checker, client)
  return(0)
}

h2o.__writeToFile <- function(res, fileName) {
  formatVector = function(vec) {
    result = rep(" ", length(vec))
    nams = names(vec)
    for(i in 1:length(vec))
      result[i] = paste(nams[i], ": ", vec[i], sep="")
    paste(result, collapse="\n")
  }
  
  cat("Writing JSON response to", fileName, "\n")
  temp = strsplit(as.character(Sys.time()), " ")[[1]]
  # myDate = gsub("-", "", temp[1]); myTime = gsub(":", "", temp[2])
  write(paste(temp[1], temp[2], '\t', formatVector(unlist(res))), file = fileName, append = TRUE)
  # writeLines(unlist(lapply(res$response, paste, collapse=" ")), fileConn)
}

h2o.__formatError <- function(error,prefix="  ") {
  result = ""
  items = strsplit(error,"\n")[[1]];
  for (i in 1:length(items))
    result = paste(result,prefix,items[i],"\n",sep="")
  result
}

h2o.__dumpLogs <- function(client) {
  ip = client@ip
  port = client@port
  
  # Sends the given arguments as URL arguments to the given page on the specified server
  url = paste("http://", ip, ":", port, "/", h2o.__DOWNLOAD_LOGS, sep="")
  temp = strsplit(as.character(Sys.time()), " ")[[1]]
  myDate = gsub("-", "", temp[1]); myTime = gsub(":", "", temp[2])
  myFile = paste("h2ologs_", myDate, "_", myTime, ".zip", sep="")
  errorFolder = "h2o_error_logs"
  
  if(!file.exists(errorFolder)) dir.create(errorFolder)
  download.file(url, destfile = paste(getwd(), "h2o_error_logs", myFile, sep="/"))
}

h2o.__poll <- function(client, keyName) {
  res = h2o.__remoteSend(client, h2o.__PAGE_JOBS)
  res = res$jobs
  if(length(res) == 0) stop("No jobs found in queue")
  prog = NULL
  for(i in 1:length(res)) {
    if(res[[i]]$key == keyName)
      prog = res[[i]]
  }
  if(is.null(prog)) stop("Job key ", keyName, " not found in job queue")
  # if(prog$end_time == -1 || prog$progress == -2.0) stop("Job key ", keyName, " has been cancelled")
  if(!is.null(prog$result$val) && prog$result$val == "CANCELLED") stop("Job key ", keyName, " was cancelled by user")
  else if(!is.null(prog$result$exception) && prog$result$exception == 1) stop(prog$result$val)
  prog$progress
}

h2o.__allDone <- function(client) {
  res = h2o.__remoteSend(client, h2o.__PAGE_JOBS)
  notDone = lapply(res$jobs, function(x) { !(x$progress == -1.0 || x$cancelled) })
  !any(unlist(notDone))
}

h2o.__pollAll <- function(client, timeout) {
  start = Sys.time()
  while(!h2o.__allDone(client)) {
    Sys.sleep(1)
    if(as.numeric(difftime(Sys.time(), start)) > timeout)
      stop("Timeout reached! Check if any jobs have frozen in H2O.")
  }
}

h2o.__isDone <- function(client, algo, resH) {
  if(!algo %in% c("GBM", "KM", "RF1", "RF2", "NN", "GLM1", "GLM2", "GLM1Grid", "PCA")) stop(algo, " is not a supported algorithm")
  version = ifelse(algo %in% c("RF1", "GLM1", "GLM1Grid"), 1, 2)
  page = switch(algo, GBM = h2o.__PAGE_GBMProgress, KM = h2o.__PAGE_KM2Progress, RF1 = h2o.__PAGE_RFVIEW, 
                RF2 = h2o.__PAGE_DRFProgress, NN = h2o.__PAGE_NNProgress, GLM1 = h2o.__PAGE_GLMProgress, 
                GLM1Grid = h2o.__PAGE_GLMGridProgress, GLM2 = h2o.__PAGE_GLM2Progress, PCA = h2o.__PAGE_PCAProgress)
  
  if(version == 1) {
    job_key = resH$response$redirect_request_args$job
    dest_key = resH$destination_key
    if(algo == "RF1")
      res = h2o.__remoteSend(client, page, model_key = dest_key, data_key = resH$data_key, response_variable = resH$response$redirect_request_args$response_variable)
    else
      res = h2o.__remoteSend(client, page, job = job_key, destination_key = dest_key)
    if(res$response$status == "error") stop(res$error)
    res$response$status != "poll"
  } else {
    job_key = resH$job_key; dest_key = resH$destination_key
    res = h2o.__remoteSend(client, page, job_key = job_key, destination_key = dest_key)
    if(res$response_info$status == "error") stop(res$error)
    
    if(!is.null(res$response_info$redirect_url)) {
      ind = regexpr("\\?", res$response_info$redirect_url)[1]
      url = ifelse(ind > 1, substr(res$response_info$redirect_url, 1, ind-1), res$response_info$redirect_url)
      !(res$response_info$status == "poll" || (res$response_info$status == "redirect" && url == page))
    } else
      res$response_info$status == "done"
  }
}

h2o.__cancelJob <- function(client, keyName) {
  res = h2o.__remoteSend(client, h2o.__PAGE_JOBS)
  res = res$jobs
  if(length(res) == 0) stop("No jobs found in queue")
  prog = NULL
  for(i in 1:length(res)) {
    if(res[[i]]$key == keyName) {
      prog = res[[i]]; break
    }
  }
  if(is.null(prog)) stop("Job key ", keyName, " not found in job queue")
  if(!(prog$cancelled || prog$progress == -1.0 || prog$progress == -2.0 || prog$end_time == -1)) {
    h2o.__remoteSend(client, h2o.__PAGE_CANCEL, key=keyName)
    cat("Job key", keyName, "was cancelled by user")
  }
}

h2o.__uniqID <- function(prefix = "") {
  if("uuid" %in% installed.packages()[,1]) {
    library(uuid)
    temp = UUIDgenerate()
  } else {
    hex_digits <- c(as.character(0:9), letters[1:6])
    y_digits <- hex_digits[9:12]
    temp = paste(
      paste(sample(hex_digits, 8, replace=TRUE), collapse='', sep=''),
      paste(sample(hex_digits, 4, replace=TRUE), collapse='', sep=''),
      paste('4', paste(sample(hex_digits, 3, replace=TRUE), collapse='', sep=''), collapse='', sep=''),
      paste(sample(y_digits,1), paste(sample(hex_digits, 3, replace=TRUE), collapse='', sep=''), collapse='', sep = ''),
      paste(sample(hex_digits, 12, replace=TRUE), collapse='', sep=''), sep='-')
  }
  temp = gsub("-", "", temp)
  paste(prefix, temp, sep="_")
}

# Check if key_env$key exists in H2O and remove if it does
# h2o.__finalizer <- function(key_env) {
#   if("h2o" %in% ls(key_env) && "key" %in% ls(key_env) && class(key_env$h2o) == "H2OClient" && class(key_env$key) == "character" && key_env$key != "") {
#     res = h2o.__remoteSend(key_env$h2o, h2o.__PAGE_VIEWALL, filter=key_env$key)
#     if(length(res$keys) != 0)
#       h2o.__remoteSend(key_env$h2o, h2o.__PAGE_REMOVE, key=key_env$key)
#   }
# }

h2o.__checkForFactors <- function(object) {
    if(class(object) != "H2OParsedData") return(FALSE)
    any.factor(object)
}

h2o.__version <- function(client) {
  res = h2o.__remoteSend(client, h2o.__PAGE_CLOUD)
  res$version
}

h2o.__getFamily <- function(family, link, tweedie.var.p = 0, tweedie.link.p = 1-tweedie.var.p) {
  if(family == "tweedie")
    return(tweedie(var.power = tweedie.var.p, link.power = tweedie.link.p))
  
  if(missing(link)) {
    switch(family,
           gaussian = gaussian(),
           binomial = binomial(),
           poisson = poisson(),
           gamma = gamma())
  } else {
    switch(family,
           gaussian = gaussian(link),
           binomial = binomial(link),
           poisson = poisson(link),
           gamma = gamma(link))
  }
}

#------------------------------------ FluidVecs -----------------------------------------#
h2o.__exec2 <- function(client, expr) {
  destKey = paste(TEMP_KEY, ".", pkg.env$temp_count, sep="")
  pkg.env$temp_count = (pkg.env$temp_count + 1) %% RESULT_MAX
  h2o.__exec2_dest_key(client, expr, destKey)
  # h2o.__exec2_dest_key(client, expr, TEMP_KEY)
}

h2o.__exec2_dest_key <- function(client, expr, destKey) {
  type = tryCatch({ typeof(expr) }, error = function(e) { "expr" })
  if (type != "character")
    expr = deparse(substitute(expr))
  expr = paste(destKey, "=", expr)
  res = h2o.__remoteSend(client, h2o.__PAGE_EXEC2, str=expr)
  if(!is.null(res$response$status) && res$response$status == "error") stop("H2O returned an error!")
  res$dest_key = destKey
  return(res)
}

h2o.__unop2 <- function(op, x) {
  if(missing(x)) stop("Must specify data set")
  if(!(class(x) %in% c("H2OParsedData","H2OParsedDataVA"))) stop(cat("\nData must be an H2O data set. Got ", class(x), "\n"))
    
  expr = paste(op, "(", x@key, ")", sep = "")
  res = h2o.__exec2(x@h2o, expr)
  if(res$num_rows == 0 && res$num_cols == 0)
    return(ifelse(op %in% LOGICAL_OPERATORS, as.logical(res$scalar), res$scalar))
  if(op %in% LOGICAL_OPERATORS)
    new("H2OParsedData", h2o=x@h2o, key=res$dest_key, logic=TRUE)
  else
    new("H2OParsedData", h2o=x@h2o, key=res$dest_key, logic=FALSE)
}

h2o.__binop2 <- function(op, x, y) {
  # if(!((ncol(x) == 1 || class(x) == "numeric") && (ncol(y) == 1 || class(y) == "numeric")))
  #  stop("Can only operate on single column vectors")
  LHS = ifelse(class(x) == "H2OParsedData", x@key, x)

  if((class(x) == "H2OParsedData" || class(y) == "H2OParsedData") & !( op %in% c('==', '!='))) {
    anyFactorsX <- h2o.__checkForFactors(x)
    anyFactorsY <- h2o.__checkForFactors(y)
    anyFactors <- any(c(anyFactorsX, anyFactorsY))
    if(anyFactors) warning("Operation not meaningful for factors.")
  }

  RHS = ifelse(class(y) == "H2OParsedData", y@key, y)
  expr = paste(LHS, op, RHS)
  if(class(x) == "H2OParsedData") myClient = x@h2o
  else myClient = y@h2o
  res = h2o.__exec2(myClient, expr)

  if(res$num_rows == 0 && res$num_cols == 0)
    return(ifelse(op %in% LOGICAL_OPERATORS, as.logical(res$scalar), res$scalar))
  if(op %in% LOGICAL_OPERATORS)
    new("H2OParsedData", h2o=myClient, key=res$dest_key, logic=TRUE)
  else
    new("H2OParsedData", h2o=myClient, key=res$dest_key, logic=FALSE)
}
