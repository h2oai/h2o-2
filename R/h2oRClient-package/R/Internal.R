# Hack to get around Exec.json always dumping to same Result.hex key
pkg.env = new.env()
pkg.env$result_count = 0
RESULT_MAX = 100
LOGICAL_OPERATORS = c("==", ">", "<", "!=", ">=", "<=")

# Internal functions & declarations
h2o.__PAGE_CLOUD = "Cloud.json"
h2o.__PAGE_EXEC = "Exec.json"
h2o.__PAGE_GET = "GetVector.json"
h2o.__PAGE_IMPORTURL = "ImportUrl.json"
h2o.__PAGE_IMPORTFILES = "ImportFiles.json"
h2o.__PAGE_IMPORTHDFS = "ImportHdfs.json"
h2o.__PAGE_INSPECT = "Inspect.json"
h2o.__PAGE_INSPECT2 = "2/Inspect2.json"
h2o.__PAGE_JOBS = "Jobs.json"
h2o.__PAGE_PARSE = "Parse.json"
h2o.__PAGE_PUT = "PutVector.json"
h2o.__PAGE_REMOVE = "Remove.json"
h2o.__PAGE_VIEWALL = "StoreView.json"
h2o.__DOWNLOAD_LOGS = "LogDownload.json"

h2o.__PAGE_SUMMARY = "SummaryPage.json"
h2o.__PAGE_SUMMARY2 = "2/SummaryPage2.json"
h2o.__PAGE_PREDICT = "GeneratePredictionsPage.json"
h2o.__PAGE_PREDICT2 = "2/Predict.json"
h2o.__PAGE_COLNAMES = "SetColumnNames.json"
h2o.__PAGE_PCA = "PCA.json"
h2o.__PAGE_PCASCORE = "PCAScore.json"
h2o.__PAGE_GLM = "GLM.json"
h2o.__PAGE_KMEANS = "KMeans.json"
h2o.__PAGE_KMAPPLY = "KMeansApply.json"
h2o.__PAGE_KMSCORE = "KMeansScore.json"
h2o.__PAGE_RF  = "RF.json"
h2o.__PAGE_RFVIEW = "RFView.json"
h2o.__PAGE_RFTREEVIEW = "RFTreeView.json"
h2o.__PAGE_GLMGrid = "GLMGrid.json"
h2o.__PAGE_GLMGridProgress = "GLMGridProgress.json"
h2o.__PAGE_GBM = "2/GBM.json"
h2o.__PAGE_GBMGrid = "2/GBMGrid.json"
h2o.__PAGE_GBMModelView = "2/GBMModelView.json"

h2o.__PAGE_GLM2 = "GLM2.json"
h2o.__PAGE_GLMModelView = "GLMModelView.json"
h2o.__PAGE_GLMValidView = "GLMValidationView.json"
h2o.__PAGE_FVEXEC = "DataManip.json"     # This is temporary until FluidVec Exec query is finished!

h2o.__remoteSend <- function(client, page, ...) {
  ip = client@ip
  port = client@port
  
  # if(IS_LOGGING) {
    # print(substitute(list(...)))
    # temp = deparse(substitute(list(...)))
  # }
  
  #TODO (Spencer): Create "commands.log" using: list(...)
  # Sends the given arguments as URL arguments to the given page on the specified server
  url = paste("http://", ip, ":", port, "/", page, sep="")
  temp = postForm(url, style = "POST", ...)
  # after = gsub("NaN", "\"NaN\"", temp[1])
  after = gsub("\\\\\\\"NaN\\\\\\\"", "NaN", temp[1])    # TODO: Don't escape NaN in the JSON!
  after = gsub("NaN", "\"NaN\"", after)
  after = gsub("-Infinity", "\"-Inf\"", after)
  after = gsub("Infinity", "\"Inf\"", after)
  res = fromJSON(after)
  
  if (!is.null(res$error)) {
    temp = strsplit(as.character(Sys.time()), " ")[[1]]
    myDate = gsub("-", "", temp[1]); myTime = gsub(":", "", temp[2])
    errorFolder = "h2o_error_logs"
    if(!file.exists(errorFolder)) dir.create(errorFolder)
    h2o.__writeToFile(res, paste(errorFolder, "/", "h2oerror_json_", myDate, "_", myTime, ".log", sep=""))
    stop(paste(url," returned the following error:\n", h2o.__formatError(res$error)))
  }
  res
}

h2o.__writeToFile <- function(res, fileName) {
  cat("Writing JSON response to", fileName)
  fileConn = file(fileName)
  
  formatVector = function(vec) {
    result = rep(" ", length(vec))
    for(i in 1:length(vec))
      result[i] = paste(names(vec)[i], ": ", vec[i], sep="")
    paste(result, collapse="\n")
  }
  
  writeLines(formatVector(unlist(res)), fileConn)
  # writeLines(unlist(lapply(res$response, paste, collapse=" ")), fileConn)
  close(fileConn)
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
  if(prog$cancelled) stop("Job key ", keyName, " has been cancelled")
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

h2o.__exec <- function(client, expr) {
  type = tryCatch({ typeof(expr) }, error = function(e) { "expr" })
  if (type != "character")
    expr = deparse(substitute(expr))
  destKey = paste("Result_", pkg.env$result_count, ".hex", sep="")
  res = h2o.__remoteSend(client, h2o.__PAGE_EXEC, expression=expr, destination_key=destKey)
  pkg.env$result_count = (pkg.env$result_count + 1) %% RESULT_MAX
  res$key
}

h2o.__exec_dest_key <- function(client, expr, destKey) {
  type = tryCatch({ typeof(expr) }, error = function(e) { "expr" })
  if (type != "character")
    expr = deparse(substitute(expr))
  res = h2o.__remoteSend(client, h2o.__PAGE_EXEC, expression=expr, destination_key=destKey)
  pkg.env$result_count = (pkg.env$result_count + 1) %% RESULT_MAX
  res$key
}

h2o.__operator <- function(op, x, y) {
  if(!((ncol(x) == 1 || class(x) == "numeric") && (ncol(y) == 1 || class(y) == "numeric")))
    stop("Can only operate on single column vectors")
  LHS = ifelse(class(x) == "H2OParsedData", h2o.__escape(x@key), x)
  RHS = ifelse(class(y) == "H2OParsedData", h2o.__escape(y@key), y)
  expr = paste(LHS, op, RHS)
  if(class(x) == "H2OParsedData") myClient = x@h2o
  else myClient = y@h2o
  res = h2o.__exec(myClient, expr)
  if(op %in% LOGICAL_OPERATORS)
    new("H2OLogicalData", h2o=myClient, key=res)
  else
    new("H2OParsedData", h2o=myClient, key=res)
}

h2o.__escape <- function(key) {
  key_esc = key
  myOS = Sys.info()["sysname"]
  if(myOS == "Windows")
    key_esc = gsub("\\\\", "\\\\\\\\", key)
    
  paste("|", key_esc, "|", sep="")
}

h2o.__func <- function(fname, x, type) {
  if(ncol(x) != 1) stop("Can only operate on single column vectors")
  expr = paste(fname, "(", h2o.__escape(x@key), ")", sep="")
  res = h2o.__exec(x@h2o, expr)
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT, key=res)
  
  if(type == "Number")
    res$rows[[1]]$'0'
  else if(type == "Vector")
    new("H2OParsedData", h2o=x@h2o, key=res$key)
  else res
}

# Check if key_env$key exists in H2O and remove if it does
h2o.__finalizer <- function(key_env) {
  if("h2o" %in% ls(key_env) && "key" %in% ls(key_env) && class(key_env$h2o) == "H2OClient" && class(key_env$key) == "character" && key_env$key != "") {
    res = h2o.__remoteSend(key_env$h2o, h2o.__PAGE_VIEWALL, filter=key_env$key)
    if(length(res$keys) != 0)
      h2o.__remoteSend(key_env$h2o, h2o.__PAGE_REMOVE, key=key_env$key)
  }
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
