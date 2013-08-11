if(!"RCurl" %in% rownames(installed.packages())) install.packages(RCurl)
if(!"rjson" %in% rownames(installed.packages())) install.packages(rjson)

library(RCurl)
library(rjson)

# Internal functions & declarations
h2o.__PAGE_CLOUD = "Cloud.json"
h2o.__PAGE_EXEC = "Exec.json"
h2o.__PAGE_GET = "GetVector.json"
h2o.__PAGE_IMPORTURL = "ImportUrl.json"
h2o.__PAGE_IMPORTFILES = "ImportFiles.json"
h2o.__PAGE_IMPORTHDFS = "ImportHdfs.json"
h2o.__PAGE_INSPECT = "Inspect.json"
h2o.__PAGE_JOBS = "Jobs.json"
h2o.__PAGE_PARSE = "Parse.json"
h2o.__PAGE_PUT = "PutVector.json"
h2o.__PAGE_REMOVE = "Remove.json"
h2o.__PAGE_VIEWALL = "StoreView.json"

h2o.__PAGE_SUMMARY = "SummaryPage.json"
h2o.__PAGE_PREDICT = "GeneratePredictionsPage.json"
h2o.__PAGE_GLM = "GLM.json"
h2o.__PAGE_KMEANS = "KMeans.json"
h2o.__PAGE_KMAPPLY = "KMeansApply.json"
h2o.__PAGE_KMSCORE = "KMeansScore.json"
h2o.__PAGE_RF  = "RF.json"
h2o.__PAGE_RFVIEW = "RFView.json"
h2o.__PAGE_RFTREEVIEW = "RFTreeView.json"
h2o.__PAGE_GLMGrid = "GLMGrid.json"
h2o.__PAGE_GLMGridProgress = "GLMGridProgress.json"

h2o.__remoteSend <- function(client, page, ...) {
  ip = client@ip
  port = client@port
  
  # Sends the given arguments as URL arguments to the given page on the specified server
  url = paste("http://", ip, ":", port, "/", page, sep="")
  temp = postForm(url, style = "POST", ...)
  after = gsub("NaN", "\"NaN\"", temp[1])
  # after = gsub("Inf", "\"Inf\"", after)
  after = gsub("Infinity", "\"Inf\"", after)
  res = fromJSON(after)
  
  if (!is.null(res$error)) {
    myTime = gsub(":", "-", date()); myTime = gsub(" ", "_", myTime)
    errorFolder = "h2o_error_logs"
    if(!file.exists(errorFolder)) dir.create(errorFolder)
    h2o.__writeToFile(res, paste(errorFolder, "/", "error_json_", myTime, ".log", sep=""))
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

h2o.__exec <- function(client, expr) {
  type = tryCatch({ typeof(expr) }, error = function(e) { "expr" })
  if (type != "character")
    expr = deparse(substitute(expr))
  res = h2o.__remoteSend(client, h2o.__PAGE_EXEC, expression=expr)
  res$key
}

h2o.__operator <- function(op, x, y) {
  if(!((ncol(x) == 1 || class(x) == "numeric") && (ncol(y) == 1 || class(y) == "numeric")))
    stop("Can only operate on single column vectors")
  LHS = ifelse(class(x) == "H2OParsedData", x@key, x)
  RHS = ifelse(class(y) == "H2OParsedData", y@key, y)
  expr = paste(LHS, op, RHS)
  if(class(x) == "H2OParsedData") myClient = x@h2o
  else myClient = y@h2o
  res = h2o.__exec(myClient, expr)
  new("H2OParsedData", h2o=myClient, key=res)
}

h2o.__func <- function(fname, x, type) {
  if(ncol(x) != 1) stop("Can only operate on single column vectors")
  expr = paste(fname, "(", x@key, ")", sep="")
  res = h2o.__exec(x@h2o, expr)
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT, key=res)
  
  if(type == "Number")
    res$rows[[1]]$'0'
  else if(type == "Vector")
    new("H2OParsedData", h2o=x@h2o, key=res$key)
  else res
}

h2o.__version <- function(client) {
  res = h2o.__remoteSend(client, h2o.__PAGE_CLOUD)
  res$version
}
