# Hack to get around Exec.json always dumping to same Result.hex key
# TODO: Need better way to manage temporary/intermediate values in calculations! Right now, overwriting occurs silently
.pkg.env = new.env()
.pkg.env$result_count = 0
.pkg.env$temp_count = 0
.pkg.env$IS_LOGGING = FALSE

.TEMP_KEY = "Last.value"
.RESULT_MAX = 1000
.MAX_INSPECT_ROW_VIEW = 10000
.MAX_INSPECT_COL_VIEW = 10000
.LOGICAL_OPERATORS = c("==", ">", "<", "!=", ">=", "<=", "&", "|", "&&", "||", "!", "is.na")

"%p0%"   <- function(x,y) assign(deparse(substitute(x)), paste(x, y, sep = ""), parent.frame())  # paste0
"%p%"    <- function(x,y) assign(deparse(substitute(x)), paste(x, y), parent.frame()) # paste

# Initialize functions for R logging
.myPath = paste(Sys.getenv("HOME"), "Library", "Application Support", "h2o", sep=.Platform$file.sep)
if(.Platform$OS.type == "windows")
  .myPath = paste(Sys.getenv("APPDATA"), "h2o", sep=.Platform$file.sep)
  
.pkg.env$h2o.__LOG_COMMAND = paste(.myPath, "commands.log", sep=.Platform$file.sep)
.pkg.env$h2o.__LOG_ERROR = paste(.myPath, "errors.log", sep=.Platform$file.sep)

h2o.startLogging     <- function() {
  cmdDir <- normalizePath(dirname(.pkg.env$h2o.__LOG_COMMAND))
  errDir <- normalizePath(dirname(.pkg.env$h2o.__LOG_ERROR))
  if(!file.exists(cmdDir)) {
    warning(cmdDir, " directory does not exist. Creating it now...")
    dir.create(cmdDir, recursive = TRUE)
  }
  if(!file.exists(errDir)) {
    warning(errDir, " directory does not exist. Creating it now...")
    dir.create(errDir, recursive = TRUE)
  }
  
  cat("Appending to log file", .pkg.env$h2o.__LOG_COMMAND, "\n")
  cat("Appending to log file", .pkg.env$h2o.__LOG_ERROR, "\n")
  assign("IS_LOGGING", TRUE, envir = .pkg.env)
}
h2o.stopLogging      <- function() { cat("Logging stopped"); assign("IS_LOGGING", FALSE, envir = .pkg.env) }
h2o.clearLogs        <- function() { file.remove(.pkg.env$h2o.__LOG_COMMAND)
                                     file.remove(.pkg.env$h2o.__LOG_ERROR) }
h2o.getLogPath <- function(type) {
  if(missing(type) || !type %in% c("Command", "Error"))
    stop("type must be either 'Command' or 'Error'")
  switch(type, Command = .pkg.env$h2o.__LOG_COMMAND, Error = .pkg.env$h2o.__LOG_ERROR)
}

h2o.openLog <- function(type) {
  if(missing(type) || !type %in% c("Command", "Error"))
    stop("type must be either 'Command' or 'Error'")
  myFile = switch(type, Command = .pkg.env$h2o.__LOG_COMMAND, Error = .pkg.env$h2o.__LOG_ERROR)
  if(!file.exists(myFile)) stop(myFile, " does not exist")
    
  myOS = Sys.info()["sysname"]
  if(myOS == "Windows") shell.exec(paste("open '", myFile, "'", sep="")) 
  else system(paste("open '", myFile, "'", sep=""))
}

h2o.setLogPath <- function(path, type) {
  if(missing(path) || !is.character(path)) stop("path must be a character string")
  if(!file.exists(path)) stop(path, " directory does not exist")
  if(missing(type) || !type %in% c("Command", "Error"))
    stop("type must be either 'Command' or 'Error'")
  
  myVar = switch(type, Command = "h2o.__LOG_COMMAND", Error = "h2o.__LOG_ERROR")
  myFile = switch(type, Command = "commands.log", Error = "errors.log")
  cmd <- paste(path, myFile, sep = .Platform$file.sep)
  assign(myVar, cmd, envir = .pkg.env)
}

.h2o.__logIt <- function(m, tmp, commandOrErr, isPost = TRUE) {
  # m is a url if commandOrErr == "Command"
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
    s <- paste(m, "\n", paste(s, collapse = ", "), ifelse(nchar(s) > 0, "\n", ""))
  }
  # if(commandOrErr != "Command") s <- paste(s, '\n')
  h <- format(Sys.time(), format = "%a %b %d %X %Y %Z", tz = "GMT")
  if(commandOrErr == "Command")
    h <- paste(h, ifelse(isPost, "POST", "GET"), sep = "\n")
  s <- paste(h, "\n", s)
  
  myFile <- ifelse(commandOrErr == "Command", .pkg.env$h2o.__LOG_COMMAND, .pkg.env$h2o.__LOG_ERROR)
  myDir <- normalizePath(dirname(myFile))
  if(!file.exists(myDir)) stop(myDir, " directory does not exist")
  write(s, file = myFile, append = TRUE)
}

# Internal functions & declarations
.h2o.__PAGE_CANCEL = "Cancel.json"
.h2o.__PAGE_CLOUD = "Cloud.json"
.h2o.__PAGE_UP = "Up.json"
.h2o.__PAGE_JOBS = "Jobs.json"
.h2o.__PAGE_REMOVE = "Remove.json"
.h2o.__PAGE_REMOVEALL = "2/RemoveAll.json"
.h2o.__PAGE_SHUTDOWN = "Shutdown.json"
.h2o.__PAGE_VIEWALL = "StoreView.json"
.h2o.__DOWNLOAD_LOGS = "LogDownload.json"
.h2o.__DOMAIN_MAPPING = "2/DomainMapping.json"
.h2o.__SET_DOMAIN = "2/SetDomains.json"
.h2o.__PAGE_ALLMODELS = "2/Models.json"
.h2o.__GAINS <- "2/GainsLiftTable.json"

.h2o.__PAGE_IMPUTE= "2/Impute.json"
.h2o.__PAGE_EXEC2 = "2/Exec2.json"
.h2o.__PAGE_IMPORTFILES2 = "2/ImportFiles2.json"
.h2o.__PAGE_EXPORTFILES = "2/ExportFiles.json"
.h2o.__PAGE_INSPECT2 = "2/Inspect2.json"
.h2o.__PAGE_PARSE2 = "2/Parse2.json"
.h2o.__PAGE_PREDICT2 = "2/Predict.json"
.h2o.__PAGE_COXPHSURVFIT = "2/CoxPHSurvfit.json"
.h2o.__PAGE_GLMPREDICT2 = "2/GLMPredict.json"
.h2o.__PAGE_SUMMARY2 = "2/SummaryPage2.json"
.h2o.__PAGE_LOG_AND_ECHO = "2/LogAndEcho.json"
.h2o.__HACK_LEVELS2 = "2/Levels2.json"
.h2o.__HACK_SETCOLNAMES2 = "2/SetColumnNames2.json"
.h2o.__PAGE_CONFUSION = "2/ConfusionMatrix.json"
.h2o.__PAGE_AUC = "2/AUC.json"
.h2o.__PAGE_HITRATIO = "2/HitRatio.json"
.h2o.__PAGE_GAPSTAT = "2/GapStatistic.json"
.h2o.__PAGE_GAPSTATVIEW = "2/GapStatisticModelView.json"
.h2o.__PAGE_QUANTILES = "2/QuantilesPage.json"
.h2o.__PAGE_INSPECTOR = "2/Inspector.json"
.h2o.__PAGE_ANOMALY = "2/Anomaly.json"
.h2o.__PAGE_DEEPFEATURES = "2/DeepFeatures.json"
.h2o.__PAGE_SETTIMEZONE = "2/SetTimezone.json"
.h2o.__PAGE_GETTIMEZONE = "2/GetTimezone.json"
.h2o.__PAGE_LISTTIMEZONES = "2/ListTimezones.json"

.h2o.__PAGE_CoxPH = "2/CoxPH.json"
.h2o.__PAGE_CoxPHProgress = "2/CoxPHProgressPage.json"
.h2o.__PAGE_CoxPHModelView = "2/CoxPHModelView.json"
.h2o.__PAGE_DRF = "2/DRF.json"
.h2o.__PAGE_DRFProgress = "2/DRFProgressPage.json"
.h2o.__PAGE_DRFModelView = "2/DRFModelView.json"
.h2o.__PAGE_GBM = "2/GBM.json"
.h2o.__PAGE_GBMProgress = "2/GBMProgressPage.json"
.h2o.__PAGE_GRIDSEARCH = "2/GridSearchProgress.json"
.h2o.__PAGE_GBMModelView = "2/GBMModelView.json"
.h2o.__PAGE_GLM2 = "2/GLM2.json"
.h2o.__PAGE_GLM2Progress = "2/GLMProgress.json"
.h2o.__PAGE_GLMModelView = "2/GLMModelView.json"
.h2o.__PAGE_GLMValidView = "2/GLMValidationView.json"
.h2o.__PAGE_GLM2GridView = "2/GLMGridView.json"
.h2o.__PAGE_KMEANS2 = "2/KMeans2.json"
.h2o.__PAGE_KM2Progress = "2/KMeans2Progress.json"
.h2o.__PAGE_KM2ModelView = "2/KMeans2ModelView.json"
.h2o.__PAGE_DeepLearning = "2/DeepLearning.json"
.h2o.__PAGE_DeepLearningProgress = "2/DeepLearningProgressPage.json"
.h2o.__PAGE_DeepLearningModelView = "2/DeepLearningModelView.json"
.h2o.__PAGE_PCA = "2/PCA.json"
.h2o.__PAGE_PCASCORE = "2/PCAScore.json"
.h2o.__PAGE_PCAProgress = "2/PCAProgressPage.json"
.h2o.__PAGE_PCAModelView = "2/PCAModelView.json"
.h2o.__PAGE_SpeeDRF = "2/SpeeDRF.json"
.h2o.__PAGE_SpeeDRFProgress = "2/SpeeDRFProgressPage.json"
.h2o.__PAGE_SpeeDRFModelView = "2/SpeeDRFModelView.json"
.h2o.__PAGE_BAYES = "2/NaiveBayes.json"
.h2o.__PAGE_NBProgress = "2/NBProgressPage.json"
.h2o.__PAGE_NBModelView = "2/NBModelView.json"
.h2o.__PAGE_CreateFrame = "2/CreateFrame.json"
.h2o.__PAGE_Interaction = "2/Interaction.json"
.h2o.__PAGE_ReBalance = "2/ReBalance.json"
.h2o.__PAGE_SplitFrame = "2/FrameSplitPage.json"
.h2o.__PAGE_NFoldExtractor = "2/NFoldFrameExtractPage.json"
.h2o.__PAGE_MissingVals = "2/InsertMissingValues.json"
.h2o.__PAGE_SaveModel = "2/SaveModel.json"
.h2o.__PAGE_LoadModel = "2/LoadModel.json"
.h2o.__PAGE_RemoveVec = "2/RemoveVec.json"
.h2o.__PAGE_Order = "2/Order.json"

.h2o.__GLMMakeModel = "2/GLMMakeModel.json"

# client -- Connection object returned from h2o.init().
# page   -- URL to access within the H2O server.
# parms  -- List of parameters to send to the server.
.h2o.__remoteSendWithParms <- function(client, page, parms) {
  cmd = ".h2o.__remoteSend(client, page"

  for (i in 1:length(parms)) {
    thisparmname = names(parms)[i]
    cmd = sprintf("%s, %s=parms$%s", cmd, thisparmname, thisparmname)
  }

  cmd = sprintf("%s)", cmd)
  #cat(sprintf("TOM: cmd is %s\n", cmd))
  
  rv = eval(parse(text=cmd))
  return(rv)
}

.h2o.__remoteSend <- function(client, page, ...) {
  .h2o.__checkClientHealth(client)
  ip = client@ip
  port = client@port
  myURL = paste("http://", ip, ":", port, "/", page, sep="")

  # Sends the given arguments as URL arguments to the given page on the specified server
  #
  # Re-enable POST since we found the bug in NanoHTTPD which was causing POST
  # payloads to be dropped.
  #
  if(.pkg.env$IS_LOGGING) {
    # Log list of parameters sent to H2O
    .h2o.__logIt(myURL, list(...), "Command")
    
    hg = basicHeaderGatherer()
    tg = basicTextGatherer()
    postForm(myURL, style = "POST", .opts = curlOptions(headerfunction = hg$update, writefunc = tg[[1]], useragent=R.version.string), ...)
    temp = tg$value()
    
    # Log HTTP response from H2O
    hh <- hg$value()
    s <- paste(hh["Date"], "\nHTTP status code: ", hh["status"], "\n ", temp, sep = "")
    s <- paste(s, "\n\n------------------------------------------------------------------\n")
    
    cmdDir <- normalizePath(dirname(.pkg.env$h2o.__LOG_COMMAND))
    if(!file.exists(cmdDir)) stop(cmdDir, " directory does not exist")
    write(s, file = .pkg.env$h2o.__LOG_COMMAND, append = TRUE)
  } else
    temp = postForm(myURL, style = "POST", .opts = curlOptions(useragent=R.version.string), ...)
  
  # The GET code that we used temporarily while NanoHTTPD POST was known to be busted.
  #
  #if(length(list(...)) == 0)
  #  temp = getURLContent(myURL)
  #else
  #  temp = getForm(myURL, ..., .checkParams = FALSE)   # Some H2O params overlap with Curl params
  
  # after = gsub("\\\\\\\"NaN\\\\\\\"", "NaN", temp[1]) 
  # after = gsub("NaN", '"NaN"', after)
  after = gsub('"Infinity"', '"Inf"', temp[1])
  after = gsub('"-Infinity"', '"-Inf"', after)
  res = fromJSON(after)

  if(!is.null(res$error)) {
    if(.pkg.env$IS_LOGGING) .h2o.__writeToFile(res, .pkg.env$h2o.__LOG_ERROR)
    stop(paste(myURL," returned the following error:\n", .h2o.__formatError(res$error)))
  }
  res
}

.h2o.__checkUp <- function(client) {
  myURL   = paste("http://", client@ip, ":", client@port, sep = "")
  myUpURL = paste("http://", client@ip, ":", client@port, "/", .h2o.__PAGE_UP, sep = "")
  if(!url.exists(myUpURL)) stop("Cannot connect to H2O instance at ", myURL)
}

.h2o.__cloudSick <- function(node_name = NULL, client) {
  url <- paste("http://", client@ip, ":", client@port, "/Cloud.html", sep = "")
  m1 <- "Attempting to execute action on an unhealthy cluster!\n"
  m2 <- ifelse(node_name != NULL, paste("The sick node is identified to be: ", node_name, "\n", sep = "", collapse = ""), "")
  m3 <- paste("Check cloud status here: ", url, sep = "", collapse = "")
  m <- paste(m1, m2, "\n", m3, sep = "")
  warning(m)
}

.h2o.__checkClientHealth <- function(client) {
  grabCloudStatus <- function(client) {
    ip <- client@ip
    port <- client@port
    .h2o.__checkUp(client)
    url <- paste("http://", ip, ":", port, "/", .h2o.__PAGE_CLOUD, "?quiet=true&skip_ticks=true", sep = "")
    fromJSON(getURLContent(url))
  }
  checker <- function(node, client) {
    status <- node$node_healthy
    elapsed <- node$elapsed_time
    nport <- unlist(strsplit(node$name, ":"))[2]
    if(!status) .h2o.__cloudSick(node_name = node$name, client = client)
    if(elapsed > 60000) .h2o.__cloudSick(node_name = NULL, client = client)
    if(elapsed > 10000) {
        Sys.sleep(5)
        lapply(grabCloudStatus(client)$nodes, checker, client)
    }
    return(0)
  }
  cloudStatus <- grabCloudStatus(client)
  if(!cloudStatus$cloud_healthy) .h2o.__cloudSick(node_name = NULL, client = client)
  lapply(cloudStatus$nodes, checker, client)
  return(0)
}

#------------------------------------ Job Polling ------------------------------------#
.h2o.__poll <- function(client, keyName) {
  if(missing(client)) stop("client is missing!")
  if(class(client) != "H2OClient") stop("client must be a H2OClient object")
  if(missing(keyName)) stop("keyName is missing!")
  if(!is.character(keyName) || nchar(keyName) == 0) stop("keyName must be a non-empty string")
  
  res = .h2o.__remoteSend(client, .h2o.__PAGE_JOBS)
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
  if (prog$progress < 0 && (prog$end_time == "" || is.null(prog$end_time))) return(abs(prog$progress)/100)
  else return(prog$progress)
}

.h2o.__allDone <- function(client) {
  res = .h2o.__remoteSend(client, .h2o.__PAGE_JOBS)
  notDone = lapply(res$jobs, function(x) { !(x$progress == -1.0 || x$cancelled) })
  !any(unlist(notDone))
}

.h2o.__pollAll <- function(client, timeout) {
  start = Sys.time()
  while(!.h2o.__allDone(client)) {
    Sys.sleep(1)
    if(as.numeric(difftime(Sys.time(), start)) > timeout)
      stop("Timeout reached! Check if any jobs have frozen in H2O.")
  }
}

.h2o.__waitOnJob <- function(client, job_key, pollInterval = 1, progressBar = TRUE) {
  if(!is.character(job_key) || nchar(job_key) == 0) stop("job_key must be a non-empty string")
  if(progressBar) {
    pb = txtProgressBar(style = 3)
    tryCatch(while((prog = .h2o.__poll(client, job_key)) != -1) { Sys.sleep(pollInterval); setTxtProgressBar(pb, prog) },
             error = function(e) { cat("\nPolling fails:\n"); print(e) },
             finally = .h2o.__cancelJob(client, job_key))
    setTxtProgressBar(pb, 1.0); close(pb)
  } else
    tryCatch(while(.h2o.__poll(client, job_key) != -1) { Sys.sleep(pollInterval) }, 
             finally = .h2o.__cancelJob(client, job_key))
}

# For checking progress from each algorithm's progress page (no longer used)
# .h2o.__isDone <- function(client, algo, resH) {
#   if(!algo %in% c("GBM", "KM", "RF1", "RF2", "DeepLearning", "GLM1", "GLM2", "GLM1Grid", "PCA")) stop(algo, " is not a supported algorithm")
#   version = ifelse(algo %in% c("RF1", "GLM1", "GLM1Grid"), 1, 2)
#   page = switch(algo, GBM = .h2o.__PAGE_GBMProgress, KM = .h2o.__PAGE_KM2Progress, RF1 = .h2o.__PAGE_RFVIEW, 
#                 RF2 = .h2o.__PAGE_DRFProgress, DeepLearning = .h2o.__PAGE_DeepLearningProgress, GLM1 = .h2o.__PAGE_GLMProgress, 
#                 GLM1Grid = .h2o.__PAGE_GLMGridProgress, GLM2 = .h2o.__PAGE_GLM2Progress, PCA = .h2o.__PAGE_PCAProgress)
#   
#   if(version == 1) {
#     job_key = resH$response$redirect_request_args$job
#     dest_key = resH$destination_key
#     if(algo == "RF1")
#       res = .h2o.__remoteSend(client, page, model_key = dest_key, data_key = resH$data_key, response_variable = resH$response$redirect_request_args$response_variable)
#     else
#       res = .h2o.__remoteSend(client, page, job = job_key, destination_key = dest_key)
#     if(res$response$status == "error") stop(res$error)
#     res$response$status != "poll"
#   } else {
#     job_key = resH$job_key; dest_key = resH$destination_key
#     res = .h2o.__remoteSend(client, page, job_key = job_key, destination_key = dest_key)
#     if(res$response_info$status == "error") stop(res$error)
#     
#     if(!is.null(res$response_info$redirect_url)) {
#       ind = regexpr("\\?", res$response_info$redirect_url)[1]
#       url = ifelse(ind > 1, substr(res$response_info$redirect_url, 1, ind-1), res$response_info$redirect_url)
#       !(res$response_info$status == "poll" || (res$response_info$status == "redirect" && url == page))
#     } else
#       res$response_info$status == "done"
#   }
# }

.h2o.__cancelJob <- function(client, keyName) {
  res = .h2o.__remoteSend(client, .h2o.__PAGE_JOBS)
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
    .h2o.__remoteSend(client, .h2o.__PAGE_CANCEL, key=keyName)
    cat("Job key", keyName, "was cancelled by user\n")
  }
}

#------------------------------------ Exec2 ------------------------------------#
.h2o.__exec2 <- function(client, expr) {
  destKey = paste(.TEMP_KEY, ".", .pkg.env$temp_count, sep="")
  .pkg.env$temp_count <- (.pkg.env$temp_count + 1) %% .RESULT_MAX
  .h2o.__exec2_dest_key(client, expr, destKey)
  # .h2o.__exec2_dest_key(client, expr, .TEMP_KEY)
}

.h2o.__exec2_dest_key <- function(client, expr, destKey) {
  type = tryCatch({ typeof(expr) }, error = function(e) { "expr" })
  if (type != "character")
    expr <- deparse(substitute(expr))
  expr <- paste(destKey, "=", expr)
  res  <- .h2o.__remoteSend(client, .h2o.__PAGE_EXEC2, str=expr)
  if(!is.null(res$response$status) && res$response$status == "error") stop("H2O returned an error!")
  res$dest_key <- destKey
  return(res)
}


#'
#' Check if any item in the expression is an H2OParsedData object.
#'
#' Useful when trying to determine whether to unravel the expression, or can just ship it up to h2o with .h2o.exec2
.anyH2O<-
function(expr, envir) {
 l <- unlist(recursive = T, lapply(as.list(expr), .as_list))
 any( "H2OParsedData" == unlist(lapply(l, .eval_class, envir)))
}

#'
#' Ship a non-H2OParsedData involving expression to H2O.
#'
#' No need to do any fancy footwork here (handles arbitrary expressions like sum(c(1,2,3))
.h2o.exec2 <- function(h2o, expr, dest_key = "") {
  if (missing(h2o)) stop("Must specify an instance of h2o to operate on non-H2OParsedData objects!")
  if (dest_key == "")
    res <- .h2o.__exec2(h2o, expr)
  else
    res <- .h2o.__exec2_dest_key(h2o, expr, dest_key)
  key <- res$dest_key
  newFrame <- new("H2OParsedData", h2o = h2o, key = key, col_names = .getColNames(res), nrows = .getRows(res), ncols = .getCols(res), any_enum = .getAnyEnum(res))
  return(newFrame)
}

#'
#' Boolean if any of the returned column types are enum.
.getAnyEnum<-
function(json) {
  res <- unlist(lapply(json$cols, function(x) as.character(x$type)))
  any(res == "Enum")
}

#'
#' Get the column names out of the Exec2 JSON output.
.getColNames<-
function(json) {
  res <- unlist(lapply(json$cols, function(x) as.character(x$name)))
  if (is.null(res)) return("")
  res
}

.getRows<-
function(json) {
  json$num_rows
}

.getCols<-
function(json) {
  json$num_cols
}

#'
#' Check for assignment with `<-` or `=`
#'
.isAssignment<-
function(expr) {
  if (identical(expr, quote(`<-`)) || identical(expr, quote(`=`))) return(TRUE)
  return(FALSE)
}

#'
#' Get the class of the object from the envir.
#'
#' The environment is the parent frame (i.e. wherever h2o.exec is called from)
.eval_class<-
function(i, envir) {
  val <- tryCatch(class(get(as.character(i), envir)), error = function(e) {
    tryCatch(class(i), error = function(e) {
      return(NA)
    })
  })
}

#'
#' Helper function to recursively unfurl an expression into a list of statements/exprs/calls/names.
#'
.as_list<-
function(expr) {
  if (is.call(expr)) {
    return(lapply(as.list(expr), .as_list))
  }
  return(expr)
}

#'
#' Cast the expression list back to a call.
#'
.back_to_expr<-
function(some_expr_list) {
  if (!is.list(some_expr_list) && length(some_expr_list == 1)) return(some_expr_list)
  len <- length(some_expr_list)
  while(len > 1) {
    num_sub_lists <- 0
    if (length(some_expr_list[[len]]) == 1) {
      num_sub_lists <- 1
    } else {
      num_sub_lists <- length(unlist(some_expr_list[[len]])) / length(some_expr_list[[len]])
    }
    if (num_sub_lists > 1) {
      some_expr_list[[len]] <- .back_to_expr(some_expr_list[[len]])
    } else if (is.atomic(some_expr_list[[len]]) || is.name(some_expr_list[[len]])) {
      some_expr_list[[len]] <- some_expr_list[[len]]
    } else {
      some_expr_list[[len]] <- as.call(some_expr_list[[len]])
    }
    len <- len - 1
  }
  return(as.call(some_expr_list))
}

#'
#' Swap the variable with the key.
#'
#' Once there's a key available, set its columns to the COLNAMES variable in the .pkg.env  (used by .get_col_id)
#' Save the key as well!
.swap_with_key<-
function(object, envir) {
  assign("SERVER", get(as.character(object), envir = envir)@h2o, envir = .pkg.env)
  assign("CURKEY", get(as.character(object), envir = envir)@key, envir = .pkg.env)
  assign("CURS4",  as.character(object), envir = .pkg.env)
  if ( !exists("COLNAMES", .pkg.env)) {
    assign("COLNAMES", colnames(get(as.character(object), envir = envir)), .pkg.env)
  }
  object <- as.name(get(as.character(object), envir = envir)@key)
  return(object)
}

#'
#' Does the column id getting
#'
.get_col_id<-
function(ch, envir) {
  which(ch == .pkg.env$COLNAMES)
}

#'
#' Swap the column name with its index in the (H2O) data frame
#'
#' Calls .get_col_id to probe a variable in the .pkg.env environment
.swap_with_colid<-
function(object, envir) {
  object <- .get_col_id(as.character(eval(object, envir = envir)), envir)
}

.lookUp<-
function(object, envir = parent.frame()) {
  if (identical(envir, emptyenv())) {
    NULL
#    stop("No such variable name: ", object, call. = FALSE)
  } else if (exists(object, envir = envir, inherits = FALSE)) {
    envir
  } else {
    .lookUp(object, parent.env(envir))
  }
}

#'
#' Actually do the replacing of variable/column name with the h2o key names / indices
.replace_all<-
function(a_list, envir) {

  # Snoop for a column name and intercept it into .pkg.env$CURCOL
  if (length(a_list) == 3 || length(a_list) == 4) {
    # In the case of subsetting a column by a factor/character, we need to get the column name.
    if (identical(a_list[[1]], quote(`$`))) {

      # if subsetting with `$`, then column name is the 3rd in the list
      assign("CURCOL", tryCatch( eval(a_list[[3]], envir = envir),
        error = function(e) {
            return(a_list[[3]])
      } ), envir = .pkg.env)
    } else if (identical(a_list[[1]], quote(`[`))) {

      # if subsetting with `[`, then column name is the 4th in the list
      assign("CURCOL", tryCatch(eval(a_list[[4]], envir = envir),
        error = function(e) {
            return(a_list[[4]])
        }) , envir = .pkg.env)
    }
  }

  # Check if there is H2OParsedData object to sub out, grab their indices.
  idxs <- which( "H2OParsedData" == unlist(lapply(a_list, .eval_class, envir)))

  # Check if there are column names to sub out, grab their indices.
  idx2 <- which( "character" == unlist(lapply(a_list, .eval_class, envir)))

  idx3 <- which( "numeric" == unlist(lapply(a_list, .eval_class, envir)))
  if (length(idx3) == 0) {
    idx3 <- which( "integer" == unlist(lapply(a_list, .eval_class, envir)))
  }
  if (length(idx3) == 0) idx3 <- which( "double" == unlist(lapply(a_list, .eval_class, envir)))

  # If nothing to sub, return
  if (length(idxs) == 0 && length(idx2) == 0 && length(idx3) == 0) return(a_list)

  # Swap out keys
  if (length(idxs) != 0) {
    for (i in idxs) {
      if(length(a_list) == 1) {
        a_list <- .swap_with_key(a_list, envir)
      } else {
        a_list[[i]] <- .swap_with_key(a_list[[i]], envir)
      }
    }
  }

  # Swap out column names with indices OR swap out the enum with its domain mapping
  if (length(idx2) != 0) {
    for (i in idx2) {
      if (length(a_list) == 1) {

        # If the col_id comes back as null, swap with domain mapping.
        swap_in <- .swap_with_colid(eval(a_list, envir = envir), envir)
        if(length(swap_in) == 0) {
          colKey <- .h2o.exec2(as.character(.pkg.env$CURKEY), h2o = .pkg.env$SERVER, as.character(.pkg.env$CURKEY))[, as.character(.pkg.env$CURCOL)]
          a_list <- .getDomainMapping(colKey, a_list)$map
        } else {
          assign("CURCOL", swap_in, envir = .pkg.env)
          a_list <- swap_in
        }
      } else {
        a_list[[i]] <- .swap_with_colid(a_list[[i]], envir)
      }
    }
  }

  # Swap out instances of variables that are numeric (just eval them in place)
  if (length(idx3) != 0) {
    for (i in idx3) {
      if (length(a_list) == 1) {
        a_list <- eval(a_list, envir = envir)
      } else {
        a_list[[i]] <- eval(a_list[[i]], envir = envir)
      }
    }
  }
  return(a_list)
}

#'
#' Replace the R variable with a H2O key name.
#' Replace column names with their indices.
.replace_with_keys_helper<-
function(some_expr_list, envir) {

  #Loop over the length of the list
  len <- length(some_expr_list)
  i <- 1
  while(i <= len) {

    # Check if there are sub lists and recurse them
    num_sub_lists <- 0
    if (length(some_expr_list[[i]]) == 1) {
      num_sub_lists <- 1
    } else {
      num_sub_lists <- length(unlist(some_expr_list[[i]])) / length(some_expr_list[[i]])
    }
    if (num_sub_lists > 1) {

      # recurse on the sublist
      some_expr_list[[i]] <- .replace_with_keys_helper(some_expr_list[[i]], envir)
    } else {

      # replace the item in the list with the key name (or column index)
      some_expr_list[[i]] <- .replace_all(some_expr_list[[i]], envir)
    }
    i <- i + 1
  }
  return(some_expr_list)
}

#'
#' Process the LHS of an assignment.
#'
#' Swap out any H2OparsedData objects with their key names,
#' Swap out a '$' "slice" with a [,] slice
.process_assignment <- function(expr, envir) {
  l <- lapply(as.list(expr), .as_list)

  # Have a single column sliced out that we want to a) replace -OR- b) create
  if (identical(l[[1]], quote(`$`)) || identical(l[[1]], quote(`[`))) {
    l[[1]] <- quote(`[`)  # This handles both cases (unkown and known colnames... should just work!)
    cols <- .h2o.exec2(h2o = get(as.character(l[[2]]), envir = envir)@h2o, expr = get(as.character(l[[2]]), envir = envir)@key, dest_key = get(as.character(l[[2]]), envir = envir)@key)@col_names
    numCols <- length(cols)
    colname <- tryCatch(
        if(length(l) == 3) as.character(eval(l[[3]], envir = envir))
        else if (!is.list(l[[4]]) && length(l[[4]]) == 1) {
          as.character(eval(l[[4]], envir = envir))
        } else {
          as.character(eval(as.expression(.back_to_expr(l[[4]])), envir = envir))
        },
        error = function(e) {return(if(length(l) == 3) as.character(l[[3]]) else as.character(l[[4]]))})

    if (! (colname %in% cols)) {
      assign("NEWCOL", colname, envir = .pkg.env)
      assign("NUMCOLS", numCols, envir = .pkg.env)
      assign("FRAMEKEY", get(as.character(l[[2]]), envir = envir)@key, envir = .pkg.env)
      l[[4]] <- numCols + 1
    } else {
      if(length(l) == 3) l[[4]] <- l[[3]]
      assign("FRAMEKEY", get(as.character(l[[2]]), envir = envir)@key, envir = .pkg.env)
    }
    l[[3]] <- as.list(substitute(l[,1]))[[3]]
    l <- .replace_with_keys_helper(l, envir)
    return(as.name(as.character(as.expression(.back_to_expr(l)))))
  }
  return(as.character(expr))
}

#'
#' Front-end work for h2o.exec
#'
#' Discover the destination key (if there is one), the client, and sub in the actual key name for the R variable
#' that contains the pointer to the key in H2O.
.replace_with_keys<-
function(expr, envir = globalenv(), expr_only = FALSE) {
  dest_key <- ""
  assign("NEWCOL", "", envir = .pkg.env)
  assign("NUMCOLS", "", envir = .pkg.env)
  assign("FRAMEKEY", "", envir = .pkg.env)

  # Is this an assignment?
  if ( .isAssignment(as.list(expr)[[1]])) {

    # The destination key is the name that's being assigned to (covers both `<-` and `=`)
    dest_key <- .process_assignment(as.list(expr)[[2]], envir)

    # Don't bother with the assignment anymore, discard it and iterate down the RHS.
    expr <- as.list(expr)[[3]]
  }

  # Assign the dest_key if one was found to the .pkg.env for later use.
  assign("DESTKEY", dest_key, envir = .pkg.env)

  # list-ify the expression
  l <- lapply(as.list(expr), .as_list)

  if (length(l) == 1) {
    l <- unlist(.replace_all(l, envir))
  } else {

    # replace any R variable names with the key name in the cloud, also handles column names passed as strings
    l <- .replace_with_keys_helper(l, envir)

    # return the modified expression
    tryCatch(rm("COLNAMES", envir = .pkg.env), warning = function(w) { invisible(w)}, error = function(e) { invisible(e)})
    if (expr_only) return(.back_to_expr(l))
    as.name(as.character(as.expression(.back_to_expr(l))))
  }
}

.h2o.__unop2 <- function(op, x) {
  if(missing(x)) stop("Must specify data set")
  if(class(x) != "H2OParsedData") stop(cat("\nData must be an H2O data set. Got ", class(x), "\n"))

  expr <- paste(op, "(", x@key, ")", sep = "")
  res <- .h2o.__exec2(x@h2o, expr)
  if(res$num_rows == 0 && res$num_cols == 0) {
    if(op %in% .LOGICAL_OPERATORS) res$scalar <- as.logical(res$scalar)
    return(res$scalar)
  }

  res <- .h2o.exec2(expr = res$dest_key, h2o = x@h2o, dest_key = res$dest_key)
  res@logic <- op %in% .LOGICAL_OPERATORS
  res
}

.h2o.__binop2 <- function(op, x, y) {
  if(class(x) != "H2OParsedData" && length(x) != 1) stop("Unimplemented: x must be a scalar value")
  if(class(y) != "H2OParsedData" && length(y) != 1) stop("Unimplemented: y must be a scalar value")
  # if(!((ncol(x) == 1 || class(x) == "numeric") && (ncol(y) == 1 || class(y) == "numeric")))
  #  stop("Can only operate on single column vectors")
  if(class(x) == "H2OParsedData") LHS <- x@key else LHS <- x
  
  if((class(x) == "H2OParsedData" || class(y) == "H2OParsedData") && !( op %in% c('==', '!='))) {
    anyFactorsX <- .h2o.__checkForFactors(x)
    anyFactorsY <- .h2o.__checkForFactors(y)
    anyFactors <- any(c(anyFactorsX, anyFactorsY))
    if(anyFactors) warning("Operation not meaningful for factors.")
  }
  
  if(class(y) == "H2OParsedData") RHS <- y@key else RHS <- y
  expr <- paste(LHS, op, RHS)
  if(class(x) == "H2OParsedData") myClient = x@h2o
  else myClient <- y@h2o
  res <- .h2o.__exec2(myClient, expr)
  
  if(res$num_rows == 0 && res$num_cols == 0) {
    if(op %in% .LOGICAL_OPERATORS) res$scalar <- as.logical(res$scalar)
    return(res$scalar)
  }

  res <- .h2o.exec2(expr = res$dest_key, h2o = myClient, dest_key = res$dest_key)
  res@logic <- op %in% .LOGICAL_OPERATORS
  res
}

# Note: Currently only written to work with ifelse method
.h2o.__multop2 <- function(op, ...) {
  myInput = list(...)
  idx = which(sapply(myInput, function(x) { class(x) == "H2OParsedData" }))[1]
  if(is.na(idx)) stop("H2OClient not specified in any input parameter!")
  myClient = myInput[[idx]]@h2o
  
  myArgs = lapply(myInput, function(x) { if(class(x) == "H2OParsedData") x@key else x })
  expr = paste(op, "(", paste(myArgs, collapse = ","), ")", sep="")
  res = .h2o.__exec2(myClient, expr)
  if(res$num_rows == 0 && res$num_cols == 0)   # TODO: If logical operator, need to indicate
    res$scalar
  else {
    res = .h2o.exec2(res$dest_key, h2o = myClient, res$dest_key)
    res@logic = FALSE
    return(res)
  }
}

#------------------------------------ Utilities ------------------------------------#
.h2o.__writeToFile <- function(res, fileName) {
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

.h2o.__formatError <- function(error,prefix="  ") {
  result = ""
  items = strsplit(as.character(error),"\n")[[1]];
  for (i in 1:length(items))
    result = paste(result,prefix,items[i],"\n",sep="")
  result
}

.h2o.__uniqID <- function(prefix = "") {
  hex_digits <- c(as.character(0:9), letters[1:6])
  y_digits <- hex_digits[9:12]
  temp = paste(
    paste(sample(hex_digits, 8, replace=TRUE), collapse='', sep=''),
    paste(sample(hex_digits, 4, replace=TRUE), collapse='', sep=''),
    paste('4', paste(sample(hex_digits, 3, replace=TRUE), collapse='', sep=''), collapse='', sep=''),
    paste(sample(y_digits,1), paste(sample(hex_digits, 3, replace=TRUE), collapse='', sep=''), collapse='', sep = ''),
    paste(sample(hex_digits, 12, replace=TRUE), collapse='', sep=''), sep='-')
  temp = gsub("-", "", temp)
  paste(prefix, temp, sep="_")
}

#'
#' Fetch the JSON for a given model key.
#'
#' Grabs all of the JSON and returns it as a named list. Do this by using the 2/Inspector.json page, which provides
#' a redirect URL to the appropriate Model View page.
.fetchJSON <- function(h2o, key) {
  redirect_url <- .h2o.__remoteSend(h2o, .h2o.__PAGE_INSPECTOR, src_key = key)$response_info$redirect_url
  page <- strsplit(redirect_url, '\\?')[[1]][1]                         # returns a list of two items
  page <- paste(strsplit(page, '')[[1]][-1], sep = "", collapse = "")   # strip off the leading '/'
  key  <- strsplit(strsplit(redirect_url, '\\?')[[1]][2], '=')[[1]][2]  # split the second item into a list of two items
  if (grepl("GLMGrid", page)) .h2o.__remoteSend(client = h2o, page = page, grid_key = key)
  else .h2o.__remoteSend(client = h2o, page = page, '_modelKey' = key)
}

#'
#' Fetch the Model for the given key.
#'
#' Fetch all of the json that the key can get!
doNotCallThisMethod...Unsupported<-
function(h2o, key) {
  warning("This method is not supported ... do not expect it to give you anything reasonable!")
  if ( ! (key %in% h2o.ls(h2o)$Key)) stop( paste("The h2o instance at ", h2o@ip, ":", h2o@port, " does not have key: \"", key, "\"", sep = ""))
  .fetchJSON(h2o, key)
}

.check.exists <- function(h2o, key) {
  keys <- as.data.frame(h2o.ls(h2o))[,1]
  key %in% keys
}

#'
#' Fetch the model from the key
h2o.getModel <- function(h2o, key) {
  json <- .fetchJSON(h2o, key)
  algo <- model.type <- names(json)[3]
  response   <- json[[model.type]]

  # Special cases: glm_model, grid, pca_model, modelw

  if(algo == "glm_model") {
      model <- .h2o.get.glm(h2o, as.character(key), TRUE)
      return(model)
  }
  if (algo == "grid") return(.h2o.get.glm.grid(h2o, key, TRUE, h2o.getFrame(h2o, response$"_dataKey")))
  if(algo == "deeplearning_model"){
    params <- json[[model.type]]$model_info$job
  } else if (algo == "nb_model") {
    params <- json[[model.type]]$job
  } else {
    params <- json[[model.type]]$parameters #.fill.params(model.type, json)
  }
  params$h2o <- h2o
  model_obj  <- switch(algo, gbm_model = "H2OGBMModel", drf_model = "H2ODRFModel", deeplearning_model = "H2ODeepLearningModel", speedrf_model = "H2OSpeeDRFModel", model= "H2OKMeansModel", glm_model = "H2OGLMModel", nb_model = "H2ONBModel", pca_model = "H2OPCAModel")
  results_fun <- switch(algo, gbm_model = .h2o.__getGBMResults,
                              drf_model = .h2o.__getDRFResults,
                              #glm_model = .get.glm.model, #.h2o.glm.get_model, #.h2o.__getGLM2Results,
                              deeplearning_model = .h2o.__getDeepLearningResults,
                              speedrf_model = .h2o.__getSpeeDRFResults,
                              model = .h2o.__getKM2Results,
                              nb_model = .h2o.__getNBResults)

  if(!is.null(response$warnings))
      tmp <- lapply(response$warnings, warning)
  job_key    <- params$job_key #response$job_key
  dest_key   <- key #params$destination_key

  train_fr   <- new("H2OParsedData", key = "NA")
  if(!is.null(response$"_dataKey") && .check.exists(h2o, response$"_dataKey")) {
    train_fr <- h2o.getFrame(h2o, response$"_dataKey") } else {
      train_fr@h2o <- h2o
    }
  params$importance <- !is.null(params$varimp)
  if(!is.null(params$family) && model.type == "gbm_model") {
    if(params$classification == "false") {params$distribution <- "gaussian"
      } else {
        if(length(params$'_distribution') > 2) params$distribution <- "multinomial" else params$distribution <- "bernoulli"
      }
    }
  if(algo == "model") {
    newModel <- new(model_obj, key = dest_key, data = train_fr, model = results_fun(json[[model.type]], train_fr, params))
    return(newModel)
  }

  if(algo == "pca_model") {
    return(.get.pca.results(train_fr, json[[model.type]], key, params))
  }
  if(algo == "nb_model"){
    modelOrig<- results_fun(json[[model.type]])
  } else {
    modelOrig<- results_fun(json[[model.type]], params)
  }
  res_xval <- list()
  if (algo == "gbm_model") algo <- "GBM"
  if (algo == "drf_model") algo <- "RF"
  if (algo == "deeplearning_model") algo <- "DeepLearning"
  if (algo == "speedrf_model") algo <- "SpeeDRF"
  if (algo == "glm_model") algo <- "GLM"
  if (algo == "nb_model") algo <- "NaiveBayes"
  if (algo %in% c("GBM", "RF", "DeepLearning", "SpeeDRF", "GLM") && !is.null(params$n_folds)) res_xval <- .h2o.crossvalidation(algo, train_fr, json[[model.type]], params$n_folds, params)
  if (is.null(params$validation)) {
    if (algo == "DeepLearning" && !is.null(modelOrig$validationKey)) {
      valid <- .h2o.exec2(h2o = h2o, expr = modelOrig$validationKey, dest_key = modelOrig$validationKey)
    } else {
      valid <- new("H2OParsedData", key=as.character(NA))
    }
  } else {
    valid <- .h2o.exec2(h2o = h2o, expr = params$validation$"_key", dest_key = params$validation$"_key")
  }
  if(algo == "NaiveBayes") {
    new(model_obj, key=dest_key, data=train_fr, model=modelOrig)
  } else {
  new(model_obj, key=dest_key, data=train_fr, model=modelOrig, valid=valid, xval=res_xval)
  }
}

.get.glm.params <- function(h2o, key) {
  res <- .h2o.__remoteSend(client = h2o, page = .h2o.__PAGE_GLMModelView, '_modelKey' = key)
  params <- res$glm_model$parameters
  params$h2o <- h2o
  params
}

.get_model_params <- function(h2o, key) {
  json <- .fetchJSON(h2o, key)
  algo <- model.type <- names(json)[3]
  if (algo == "grid") return("")
  params <- json[[model.type]]$parameters
  params$h2o <- h2o
  params
}

#'
#' Get the reference to a frame with the given key.
h2o.getFrame <- function(h2o, key) {
  if (missing(key) || is.null(key)) {
     warning( paste("The h2o instance at ", h2o@ip, ":", h2o@port, " missing key!", sep = ""))
        return(new("H2OParsedData", h2o = h2o, key = "NA"))
  }
  if ( ! (key %in% h2o.ls(h2o)$Key)) {
    warning( paste("The h2o instance at ", h2o@ip, ":", h2o@port, " does not have key: \"", key, "\"", sep = ""))
    return(new("H2OParsedData", h2o = h2o, key = "NA"))
  }
  .h2o.exec2(expr = key, h2o = h2o, dest_key = key)
}

# Check if key_env$key exists in H2O and remove if it does
# .h2o.__finalizer <- function(key_env) {
#   if("h2o" %in% ls(key_env) && "key" %in% ls(key_env) && class(key_env$h2o) == "H2OClient" && class(key_env$key) == "character" && key_env$key != "") {
#     res = .h2o.__remoteSend(key_env$h2o, .h2o.__PAGE_VIEWALL, filter=key_env$key)
#     if(length(res$keys) != 0)
#       .h2o.__remoteSend(key_env$h2o, .h2o.__PAGE_REMOVE, key=key_env$key)
#   }
# }

.h2o.__checkForFactors <- function(object) {
  if(class(object) != "H2OParsedData") return(FALSE)
  h2o.anyFactor(object)
}

.h2o.__version <- function(client) {
  res = .h2o.__remoteSendWithParms(client, .h2o.__PAGE_CLOUD, list(quiet="true", skip_ticks="true"))
  res$version
}

.h2o.__getFamily <- function(family, link, tweedie.var.p = 0, tweedie.link.p = 1-tweedie.var.p) {
  if(family == "tweedie")
    return(tweedie(var.power = tweedie.var.p, link.power = tweedie.link.p))
  
  if(missing(link)) {
    switch(family,
           "gaussian" = gaussian(),
           "binomial" = binomial(),
           "poisson" = poisson(),
           "gamma" = Gamma())
  } else {
    switch(family,
           "gaussian" = gaussian(link),
           "binomial" = binomial(link),
           "poisson" = poisson(link),
           "gamma" = Gamma(link))
  }
}
