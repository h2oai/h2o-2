library('RCurl');
library('rjson');
library('xts');

# R <-> H2O Interop Layer
#
# This is a new version that will eventually be packaged. Comments are yet missing, will be added soon.

is.defined <- function(x) {
  # !is.null is ugly :)
  return (!is.null(x))
}

# Public functions & declarations -------------------------------------------------------------------------------------

# Determines the server and port to which the interop layer always connects
h2o.SERVER = "localhost:54321"

# If verbose, messages will be printed when data is requested, and / or received from the server
h2o.VERBOSE = TRUE

# Default maximal number of rows that will be fetched from the server. There is no guarantee that this number of rows
# will actually be fetched as the server has a final say in the matter
h2o.MAX_GET_ROWS = 200000

# Executes the given expression on the server and returns the result. The expression can either be a string, or can be
# an unquoted expression, which will be automatically strigified by the function. This is a shorthand for an h2o.exec
# call followed by h2o.get of the result key. See the h2o.get function for details on the other arguments.  
h2o <- function(expr,maxRows = h2o.MAX_GET_ROWS, forceDataFrame = FALSE) {
  type = tryCatch({ typeof(expr) }, error = function(e) { "expr" })
  if (type != "character")
    expr = deparse(substitute(expr))
  keyName = h2o.exec(expr)
  h2o.get(keyName, maxRows, forceDataFrame)
}

# Executes the given expression on the server and returns the name of the key in which the result is stored. The
# expression can either be a string, or an unquoted expression which is automatically quoted by the function.
#
# *** Please note that under the current implementation the result key is always "Result" and gets rewritten with
# each new execution.
h2o.exec <- function(expr) {
  type = tryCatch({ typeof(expr) }, error = function(e) { "expr" })
  if (type != "character")
    expr = deparse(substitute(expr))
  h2o.__printIfVerbose("  Executing expression ",expr)
  res = h2o.__remoteSend(h2o.__PAGE_EXEC,expression=expr)
  res$key
}

# Returns the key of given name as R data. The key can either be a string, or a name which is then automatically
# transferred to a string by the function. maxRows argument specifies how many rows at most should be fetched
# from the server. However there is no guarantee, that this many rows will be fetched as the server has the final
# word on this matter. If returned value has multiple columns, an R dataframe is returned. For a single column,
# a simple vector is created unless the forceDataFrame argument is set to TRUE.
h2o.get <- function(keyName, maxRows = h2o.MAX_GET_ROWS, forceDataFrame = FALSE) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  h2o.__printIfVerbose("  Getting key ",keyName)
  res = h2o.__remoteSend(h2o.__PAGE_GET, key = keyName, maxRows = as.character(maxRows))
  h2o.__convertToRData(res,forceDataFrame = forceDataFrame)
}

# Puts the given vector to H2O cloud under given key name. As usual key name can either be a string or a literal.
# Returns the key name. 
#
# *** Please note that under current implementation only single column vectors can be stored. This behavior is to
# change with the new ValueArray classes in H2O. 
h2o.put <- function(keyName, value) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  h2o.__printIfVerbose("  Putting a vector of ",length(value)," to key ",keyName)
  res = h2o.__remoteSend(h2o.__PAGE_PUT,Key = keyName, Value = paste(value,sep="",collapse=" "))
  res$Key
}

h2o.poll <- function(keyName) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  res = h2o.__remoteSend(h2o.__PAGE_JOBS)
  res = res$jobs
  if(length(res) == 0) {
    print(res)
    stop("No jobs found in queue")
  }
  prog = NULL
  for(i in 1:length(res)) {
    if(res[[i]]$key == keyName)
      prog = res[[i]]
  }
  if(is.null(prog)) {
    print(res)
    stop(cat("Hex key", keyName, "not found in job queue"))
  }
  prog$progress
}

h2o.poll_rf <- function(keyName) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  res = h2o.__remoteSend(h2o.__PAGE_JOBS)
  res = res$jobs
  for(i in 1:length(res)) {
    if(res[[i]]$destination_key == keyName)
      prog = res[[i]]
  }
  prog$progress
}

h2o.job_time <- function(keyName) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  res = h2o.__remoteSend(h2o.__PAGE_JOBS)
  res = res$jobs
  for(i in 1:length(res)) {
    if(res[[i]]$destination_key == keyName)
      prog = res[[i]]
  }
  if(prog$progress != -1)
    print("Task is not finished")
  else if(prog$cancelled == "true")
    print("Task was cancelled")
  else {
    start <- .parseISO8601(unlist(strsplit(prog$start_time, "-0700", fixed = TRUE)))
    end <- .parseISO8601(unlist(strsplit(prog$end_time, "-0700", fixed = TRUE)))
    difftime(end$first.time, start$first.time, units = "secs")
  }
}

# Inspects the given key on H2O cloud. Key can be either a string or a literal which will be translated to a string.
# Returns a list with key name (key), value type (type), number of rows in the value (num_rows), number of columns (num_cols),
# size of a single row in bytes (rowSize) and total size in bytes of the value (size). Also list of all columns
# with their names, offsets, types, scales, min, max, means and variances is returned in a data frame (cols).
h2o.inspect <- function(keyName) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  h2o.__printIfVerbose("  Inspecting key ",keyName)
  res = h2o.__remoteSend(h2o.__PAGE_INSPECT, key = keyName)
  result = list()
  result$key = res$key
  result$type = res$type
  result$num_rows = res$num_rows
  result$num_cols = res$num_cols
  result$rowSize = res$row_size
  result$size = res$value_size_bytes
  h2o.__printIfVerbose("  key has ",res$num_cols," columns and ",res$num_rows," rows")  
  # given correct inspect JSON response, converts its columns to a dataframe
  extract <- function(from,what) {
    result = NULL
    for (i in 1:length(from)) 
      result = c(result,from[[i]][what]);
    result;    
  }
  res = res$cols;
  result$cols = data.frame(name = as.character(extract(res,"name")),
                              offset = as.numeric(extract(res,"offset")),
                              type = as.character(extract(res,"type")),
                              size = as.numeric(extract(res,"size")),
                              base = as.numeric(extract(res,"base")),
                              scale = as.numeric(extract(res,"scale")),
                              min = as.numeric(extract(res,"min")),
                              max = as.numeric(extract(res,"max")),
                              nmiss = as.numeric(extract(res,"num_missing_values")),
                              mean = as.numeric(extract(res,"mean")),
                              var = as.numeric(extract(res,"variance")))
  result
}

# Deletes the given key (string or literal) from the cloud. Returns the deleted key name. 
h2o.remove <- function(keyName) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  h2o.__printIfVerbose("  Removing key ",keyName)
  res = h2o.__remoteSend(h2o.__PAGE_REMOVE, key = keyName)
  res$key
}

# Imports the given URL to the cloud. The URL must be visible from the node to which the interop is connecting to.
# If parse is set to TRUE the imported file will also be parsed so that it can be immediately used in R. In this
# case the specified key name is actually the name of the parsed value. The url itself is used as name for the
# unparsed key, which is *NOT* deleted. 
h2o.importUrl <- function(keyName, url, parse = TRUE) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  uploadKey = keyName
  h2o.__printIfVerbose("  Importing url ",url," to key ",uploadKey)
  res = h2o.__remoteSend(h2o.__PAGE_IMPORT, key = uploadKey, url = url)
  if (parse) {
    h2o.__printIfVerbose("  parsing key ",uploadKey," to key ",keyName)
    res = h2o.__remoteSend(h2o.__PAGE_PARSE, source_key = uploadKey, destination_key = paste(keyName,".hex",sep=""))    
  } 
  # res
  while(h2o.poll(res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
}

# Imports a file local to the server the interop is connecting to. Other arguments are the same as for the importUrl
# function.
h2o.importFile <- function(keyName, fileName, parse = TRUE) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  h2o.importUrl(keyName,paste("file:///",fileName,sep=""),parse = parse)
}

# shorthands ----------------------------------------------------------------------------------------------------------

# Shorthand for a value slice expression. This is the same as running h2o(slice(keyName, startRow, length)). Returns 
# length number of rows from startRow of the given key. Length of -1 (default) returns all the elements in the key
# after the start row. Not all elements might be required as the server may decide to send fewer lines for memory
# reasons. Other arguments are the same as for the h2o.get() function. 
h2o.slice <- function(keyName, startRow, length=-1, forceDataFrame = FALSE) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  if (length == -1)
    expr = paste("slice(",keyName,",",startRow,")",sep="")
  else
    expr = paste("slice(",keyName,",",startRow,",",length,")",sep="")
  h2o.__printIfVerbose("  slicing key ",keyName," - expression ",expr)
  resultKey = h2o.exec(expr)
  h2o.get(resultKey,h2o.MAX_GET_ROWS, forceDataFrame)
}

# Shorthand for a filter function in H2O. Submitted with the key to be filtered and an expressing to determine whether
# a row should be applied or not. Other arguments are the same as for h2o.get() function.
# Example: h2o.filter(haha, haha$gaga < 5) returns all rows of haha in which the column gaga hs value smaller than 5.
h2o.filter <- function(keyName, expr, maxRows = h2o.MAX_GET_ROWS, forceDataFrame = FALSE) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  type = tryCatch({ typeof(expr) }, error = function(e) { "expr" })
  if (type != "character")
    expr = deparse(substitute(expr))
  expr = paste("filter(",keyName,",",expr,")")
  h2o.__printIfVerbose("  filtering key ",keyName," - expression ",expr)
  resultKey = h2o.exec(expr)
  h2o.get(resultKey,maxRows,forceDataFrame)
}

# GLM function. This should be rewiewed by someone who actually understands the GLM:-D
# Please note that the x and negX arguments cannot be specified without quotes as lists are expected. 
# h2o.glm = function(keyName, y, case="1.0", x = "", negX = "", family = "gaussian", xval = 0, threshold = 0.5, norm = "NONE", lambda = 0.1, rho = 1.0, alpha = 1.0) {
h2o.glm = function(keyName, y, case = "1.0", x = "", negX = "", family = "gaussian", xval = 0, threshold = 0.5, lambda = 1.0e-5, rho = 1.0, alpha = 0.5, nfolds = 10) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  type = tryCatch({ typeof(y) }, error = function(e) { "expr" })
  if (type != "character")
    y = deparse(substitute(y))
  type = tryCatch({ typeof(family) }, error = function(e) { "expr" })
  if (type != "character")
    family = deparse(substitute(family))
  type = tryCatch({ typeof(norm) }, error = function(e) { "expr" })
  if (type != "character")
    norm = deparse(substitute(norm))
  x = paste(x,sep="",collapse=",")
  negX = paste(negX,sep="",collapse=",")
  h2o.__printIfVerbose("  running GLM on vector ",keyName," response column ",y)
  res = h2o.__remoteSend(h2o.__PAGE_GLM, key = keyName, y = y, case=case, x = x, "-x" = negX, family = family, xval = xval, threshold = threshold, norm = norm, lambda = lambda, rho = rho, alpha = alpha)
  while(h2o.poll(res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
  h2o.__printModel(res$destination_key, type = "GLM")
}

# K-means function.
h2o.kmeans = function(keyName, k = 5, epsilon = 1.0E-6, normalize = 0) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if(type != "character")
    keyName = deparse(substitute(keyName))
  type = tryCatch({ typeof(k) }, error = function(e) { "expr" })
  if(type != "character")
    k = deparse(substitute(k))

  h2o.__printIfVerbose("  running ", k, "-means on vector ", keyName)
  res = h2o.__remoteSend(h2o.__PAGE_KMEANS, source_key = keyName, k = k, epsilon = epsilon, normalize = normalize)
  while(h2o.poll(res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
  # h2o.__printModel(res$destination_key, type = "KM")
  
  result = list()
  result$key = res$destination_key
  result$source_key = keyName
  result$centers = h2o.__printModel(res$destination_key, type = "KM")
  result
  
  # h2o.__printIfVerbose("  applying ", res$destination_key, " to data ", dataKey)
  # res = h2o.__remoteSend(h2o.__PAGE_KMAPPLY, model_key = result$model_key, data_key = dataKey, destination_key = destKey)
  # while(h2o.poll(res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
}

h2o.km_apply = function(modelKey, dataKey, destKey) {
  type = tryCatch({ typeof(modelKey) }, error = function(e) { "expr" })
  if(type != "character")
    modelKey = deparse(substitute(modelKey))
  type = tryCatch({ typeof(dataKey) }, error = function(e) { "expr" })
  if(type != "character")
    dataKey = deparse(substitute(dataKey))
  type = tryCatch({ typeof(destKey) }, error = function(e) { "expr" })
  if(type != "character")
    destKey = deparse(substitute(destKey))
  
  h2o.__printIfVerbose("  applying ", modelKey, " to data ", dataKey)
  res = h2o.__remoteSend(h2o.__PAGE_KMAPPLY, model_key = modelKey, data_key = dataKey, destination_key = destKey)
  while(h2o.poll(res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
  h2o.__printIfVerbose("  Inspecting key ", destKey)
  res = h2o.__remoteSend(h2o.__PAGE_INSPECT, key = destKey)
  res$key
}

# RF function. 
h2o.rf = function(keyName, ntree="", class = "", negX = "", family = "gaussian", xval = 0, threshold = 0.5, norm = "NONE", lambda = 0.1, rho = 1.0, alpha = 1.0) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  type = tryCatch({ typeof(ntree) }, error = function(e) { "expr" })
  if (type != "character")
    Y = deparse(substitute(ntree))
  type = tryCatch({ typeof(class) }, error = function(e) { "expr" })
 
  h2o.__printIfVerbose("  running RF on vector ",keyName," class column ",class, " number of trees ", ntree)
  res = h2o.__remoteSend(h2o.__PAGE_RF, data_key = keyName, ntree = ntree, class = class)
  # res
  while(h2o.poll_rf(res$response$redirect_request_args$model_key) != -1) { Sys.sleep(1) }
  h2o.inspect_rf(res$model_key, res$data_key)
}

h2o.inspect_rf = function(modelkey, datakey, oob_err = 1) {
  type = tryCatch({ typeof(modelkey) }, error = function(e) { "expr" })
  if (type != "character")
    modelkey = deparse(substitute(modelkey))
  type = tryCatch({ typeof(datakey) }, error = function(e) { "expr" })
  if (type != "character")
    datakey  = deparse(substitute(datakey))
  
  h2o.__printIfVerbose("  Inspecting model key ",modelkey, " for data key ", datakey)
  res = h2o.__remoteSend(h2o.__PAGE_RFVIEW, model_key = modelkey, data_key = datakey, out_of_bag_error_estimate = oob_err)
  result = list()
  result$data_key = res$data_key
  result$model_key = res$model_key
  result$num_trees = res$ntree
  result$confusion_matrix = res$confusion_matrix
  result$trees = res$trees
  result
}



# Internal functions & declarations -----------------------------------------------------------------------------------

h2o.__PAGE_EXEC = "Exec.json"
h2o.__PAGE_GET = "GetVector.json"
h2o.__PAGE_PUT = "PutVector.json"
h2o.__PAGE_INSPECT = "Inspect.json"
h2o.__PAGE_REMOVE = "Remove.json"
h2o.__PAGE_IMPORT = "ImportUrl.json"
h2o.__PAGE_PARSE = "Parse.json"
h2o.__PAGE_GLM = "GLM.json"
h2o.__PAGE_KMEANS = "KMeans.json"
h2o.__PAGE_KMAPPLY = "KMeansApply.json"
h2o.__PAGE_RF  = "RF.json"
h2o.__PAGE_RFVIEW = "RFView.json"
h2o.__PAGE_JOBS = "Jobs.json"

h2o.__printIfVerbose <- function(...) {
  if (h2o.VERBOSE == TRUE)
    cat(paste(...,"\n",sep=""))
}

h2o.__remoteSend <- function(page,...) {
  # Sends the given arguments as URL arguments to the given page on the specified server
  # h2o.__printIfVerbose(page)
  url = paste(h2o.SERVER,page,sep="/")
  # res = fromJSON(postForm(url, style = "POST", ...))
  temp = postForm(url, style = "POST", ...)
  after = gsub("NaN", "\"NaN\"", temp[1])
  after = gsub("Inf", "\"Inf\"", after)
  res = fromJSON(after)
  if (is.defined(res$error))
    stop(paste(url," returned the following error:\n",h2o.__formatError(res$error)))
  res    
}

h2o.__formatError <- function(error,prefix="  ") {
  result = ""
  items = strsplit(error,"\n")[[1]];
  for (i in 1:length(items))
    result = paste(result,prefix,items[i],"\n",sep="")
  result
}

h2o.__convertToRData <- function(res,forceDataFrame=FALSE) {
  # converts the given response to an R vector or dataframe. Vector is used when there is only one column, otherwise dataframe is used. 
  if ((res$num_cols == 0) || (res$num_rows == 0)) {
    h2o.__printIfVerbose("  empty result, returning null")
    NULL
  } else if (!forceDataFrame && (length(res$columns) == 1)) {
    h2o.__printIfVerbose("  converting returned ",res$num_cols," columns and ",res$sent_rows," rows to an R vector")
    as.numeric(res$columns[[1]]$contents)
  } else {
    h2o.__printIfVerbose("  converting returned ",res$num_cols," columns and ",res$sent_rows," rows to an R data frame")
    r = data.frame()
    rows = res$sent_rows
    for (i in 1:length(res$columns)) {
      col = res$columns[[i]]
      name = as.character(col$name)
      r[1:rows, name] <- as.numeric(col$contents)
    }
    r
  }
}

h2o.__printModel <- function(keyName, type) {
  h2o.__printIfVerbose("  Inspecting key ",keyName)
  res = h2o.__remoteSend(h2o.__PAGE_INSPECT, key = keyName)
  if(type == "GLM") {
    res = res$GLMModel
    result = list()
    result$key = res$model_key
    result$dof = res$dof
    result$coef = data.frame(res$coefficients)
    colnames(result$coef) = c(res$column_names, "Intercept")
    result$norm_coef = data.frame(res$normalized_coefficients)
    colnames(result$norm_coef) = c(res$column_names, "Intercept")
    result$params = res$GLMParams
    result
  }
  else if(type == "KM") { res$KMeansModel$clusters }
}

# h2o.rf <- function(key,ntree, depth=30,model=FALSE,gini=1,seed=42,wait=TRUE) {
#   if (model==FALSE)
#     model = paste(key,"_model",sep="")
#   H2O._printIfVerbose("  executing RF on ",key,", ",ntree," trees , maxDepth ",depth,", gini ",gini,", seed ",seed,", model key ",model)
#   res = H2O._remoteSend("RF.json",Key=key, ntree=ntree, depth=depth, gini=gini, seed=seed, modelKey=model)
#   if (H2O._isError(res)) {
#     H2O._printError(res$Error,prefix="  ")
#     NULL
#   } else {
#     H2O._printIfVerbose("    task for building ",res$ntree," from data ",res$dataKey)
#     H2O._printIfVerbose("    model key: ",res$modelKey)
#     res$modelKey
#   }
# }
# 
# h2o.rfView <- function(dataKey, modelKey) {
#   H2O._printIfVerbose("  RF model request for data ",dataKey," and model ",modelKey)
#   res = H2O._remoteSend("RFView.json",dataKey=dataKey, modelKey=modelKey)
#   if (H2O._isError(res)) {
#     H2O._printError(res$Error,prefix="  ")
#     NULL
#   } else {
#     res 
#   }  
# }

