library('RCurl');
library('rjson');

# Class definitions
setClass("H2OClient", representation(ip="character", port="numeric"), prototype(ip="127.0.0.1", port=54321))
setClass("H2ORawData", representation(h2o="H2OClient", key="character"))
setClass("H2OParsedData", representation(h2o="H2OClient", key="character"))
setClass("H2OGLMModel", representation(key="character", data="H2OParsedData", glm="list"))
setClass("H2OKMeansModel", representation(key="character", data="H2OParsedData", km="list"))
setClass("H2ORForestModel", representation(key="character", data="H2OParsedData", rf="list"))

# Class display functions
setMethod("show", "H2OClient", function(object) {
  cat("IP Address:", object@ip, "\n")
  cat("Port      :", object@port, "\n")
})

setMethod("show", "H2ORawData", function(object) {
  print(object@h2o)
  cat("Raw Data Key:", object@key, "\n")
})

setMethod("show", "H2OParsedData", function(object) {
  print(object@h2o)
  cat("Parsed Data Key:", object@key, "\n")
})

setMethod("show", "H2OGLMModel", function(object) {
  print(object@data)
  cat("GLM Model Key:", object@key, "\n\nCoefficients:\n")
  
  model = object@glm
  print(round(model$coefficients,5))
  cat("\nDegrees of Freedom:", model$df.null, "Total (i.e. Null); ", model$df.residual, "Residual\n")
  cat("Null Deviance:    ", round(model$null.deviance,1), "\n")
  cat("Residual Deviance:", round(model$deviance,1), " AIC:", round(model$aic,1))
})

setMethod("show", "H2OKMeansModel", function(object) {
  print(object@data)
  cat("K-Means Model Key:", object@key)
  
  model = object@km
  cat("\n\nK-means clustering with", length(model$size), "clusters of sizes "); cat(model$size, sep=", ")
  cat("\n\nCluster means:\n"); print(model$centers)
  cat("\nClustering vector:\n"); print(model$cluster)  # summary(model$cluster) currently broken
  cat("\nWithin cluster sum of squares by cluster:\n"); print(model$withinss)
  cat("\nAvailable components:\n\n"); print(names(model))
})

setMethod("show", "H2ORForestModel", function(object) {
  print(object@data)
  cat("Random Forest Model Key:", object@key)
  
  model = object@rf
  cat("\n\nType of random forest:", model$type)
  cat("\nNumber of trees:", model$ntree)
  cat("\n\nOOB estimate of error rate: ", round(100*model$oob_err, 2), "%", sep = "")
  cat("\nConfusion matrix:\n"); print(model$confusion)
})

# Generic method definitions
setGeneric("importFile", function(object, path, key) { standardGeneric("importFile") })
setGeneric("importURL", function(object, path, key) { standardGeneric("importURL") })
setGeneric("h2o.glm", function(x, y, data, family, nfolds, alpha) { standardGeneric("h2o.glm") })
setGeneric("h2o.kmeans", function(data, centers, iter.max) { standardGeneric("h2o.kmeans") })
# setGeneric("h2o.randomForest", function(y, x_ignore, data, ntree) { standardGeneric("h2o.randomForest") })
setGeneric("h2o.randomForest", function(y, data, ntree) { standardGeneric("h2o.randomForest") })
setGeneric("h2o.getTree", function(forest, k) { standardGeneric("h2o.getTree") })

# Unique methods to H2O
setMethod("importURL", signature(object="H2OClient", path="character", key="character"),
          function(object, path, key) {
            res = h2o.__remoteSend(object, h2o.__PAGE_IMPORTURL, url=path)
            res = h2o.__remoteSend(object, h2o.__PAGE_PARSE, source_key=res$key, destination_key=key)
            while(h2o.__poll(object, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
            parsedData = new("H2OParsedData", h2o=object, key=res$destination_key)
          })

setMethod("importURL", signature(object="H2OClient", path="character", key="missing"),
          function(object, path) { importURL(object, path, "") })

setMethod("importFile", signature(object="H2OClient", path="character", key="missing"), 
          function(object, path) {
            res = h2o.__remoteSend(object, h2o.__PAGE_IMPORTFILES, path=normalizePath(path))
            res = h2o.__remoteSend(object, h2o.__PAGE_PARSE, source_key=res$key)
            while(h2o.__poll(object, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
            parsedData = new("H2OParsedData", h2o=object, key=res$destination_key)
          })

setMethod("importFile", signature(object="H2OClient", path="character", key="character"), 
  function(object, path, key) { importURL(object, paste0("file:///", normalizePath(path)), key) })

setMethod("h2o.glm", signature(x="character", y="character", data="H2OParsedData", family="character", nfolds="numeric", alpha="numeric"),
          function(x, y, data, family, nfolds, alpha) {
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLM, key = data@key, y = y, x = x, family = family, n_folds = nfolds, alpha = alpha)
            while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
            destKey = res$destination_key
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_INSPECT, key=res$destination_key)
            res = res$GLMModel
            
            result = list()
            result$coefficients = unlist(res$coefficients)
            result$rank = res$nCols
            result$family = family   # Need to convert to family object
            result$deviance = res$validations[[1]]$resDev
            result$aic = res$validations[[1]]$aic
            result$null.deviance = res$validations[[1]]$nullDev
            result$iter = res$iterations
            result$df.residual = res$dof
            result$df.null = res$dof + result$rank
            result$y = y
            result$x = x
            
            resGLMModel = new("H2OGLMModel", key=destKey, data=data, glm=result)
          })

setMethod("h2o.kmeans", signature(data="H2OParsedData", centers="numeric", iter.max="numeric"),
          function(data, centers, iter.max) {
            # Build K-means model
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_KMEANS, source_key=data@key, k=centers, max_iter=iter.max)
            while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
            destKey = res$destination_key
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_INSPECT, key=res$destination_key)
            res = res$KMeansModel            
            
            result = list()
            result$centers = do.call(rbind, res$clusters)
            rownames(result$centers) <- seq(1,nrow(result$centers))
            colnames(result$centers) <- colnames(summary(data))  # Need to make this more efficient (no need for summary call)
            
            # Apply model to data set
            scoreKey = paste0(strsplit(data@key, ".hex")[[1]], ".kmapply")
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_KMAPPLY, model_key=destKey, data_key=data@key, destination_key=scoreKey)
            while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
            result$cluster = new("H2OParsedData", h2o=data@h2o, key=res$destination_key)
            
            # Score model on data set
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_KMSCORE, model_key=destKey, key=data@key)
            res = res$score
            result$size = res$rows_per_cluster
            result$withinss = res$sqr_error_per_cluster
            result$tot.withinss = sum(result$withinss)
            # Need between-cluster sum of squares (or total sum of squares, since betweenss = totss-tot.withinss)!
            
            resKMModel = new("H2OKMeansModel", key=destKey, data=data, km=result)
          })

setMethod("h2o.kmeans", signature(data="H2OParsedData", centers="numeric", iter.max="missing"),
          function(data, centers) { h2o.kmeans(data, centers, 10) })

# setMethod("h2o.randomForest", signature(y="character", x_ignore="character", data="H2OParsedData", ntree="numeric"),
#          function(y, x_ignore, data, ntree) {
#           res = h2o.__remoteSend(data@h2o, h2o.__PAGE_RF, data_key=data@key, response_variable=y, ignore=x_ignore, ntree=ntree)
setMethod("h2o.randomForest", signature(y="character", data="H2OParsedData", ntree="numeric"),
          function(y, data, ntree) {
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_RF, data_key=data@key, response_variable=y, ntree=ntree)
            while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
            destKey = res$destination_key
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_RFVIEW, model_key=destKey, data_key=data@key, out_of_bag_error_estimate=1)
                        
            result = list()
            result$type = "classification"
            result$ntree = ntree
            result$oob_err = res$confusion_matrix$classification_error
            
            rf_matrix = cbind(matrix(unlist(res$trees$depth), nrow=3), matrix(unlist(res$trees$leaves), nrow=3))
            rownames(rf_matrix) = c("Min.", "Mean.", "Max.")
            colnames(rf_matrix) = c("Depth", "Leaves")
            result$forest = rf_matrix
            
            cf = res$confusion_matrix
            cf_matrix = cbind(matrix(unlist(cf$scores), nrow=length(cf$header)), unlist(cf$classes_errors))
            rownames(cf_matrix) = cf$header
            colnames(cf_matrix) = c(cf$header, "class.error")
            result$confusion = cf_matrix
            
            resRFModel = new("H2ORForestModel", key=destKey, data=data, rf=result)
          })

# setMethod("h2o.randomForest", signature(y="character", x_ignore="missing", data="H2OParsedData", ntree="numeric"),
#          function(y, data, ntree) { h2o.randomForest(y, "", data, ntree) })

setMethod("h2o.getTree", signature(forest="H2ORForestModel", k="numeric"),
          function(forest, k) {
            if(k < 1 || k > forest@rf$ntree)
              stop(paste("k must be between 1 and", forest@rf$ntree))
            res = h2o.__remoteSend(forest@data@h2o, h2o.__PAGE_RFTREEVIEW, model_key=forest@key, tree_number=k-1, data_key=forest@data@key)
            result = list()
            result$depth = res$depth
            result$leaves = res$leaves
            result
            # Need to edit Java to output more data! Also consider plotting?
          })

setMethod("summary", signature(object="H2OParsedData"),
          function(object) {
            res = h2o.__remoteSend(object@h2o, h2o.__PAGE_SUMMARY, key=object@key)
            res = res$summary$columns
            result = NULL
            cnames = NULL
            for(i in 1:length(res)) {
              cnames = c(cnames, paste0("      ", res[[i]]$name))
              if(res[[i]]$type == "number") {
                result = cbind(result, c(paste0("Min.   :", format(round(res[[i]]$min[1],3), nsmall = 3), "  "),
                       paste0("1st Qu.:", format(round(res[[i]]$percentiles$values[4],3), nsmall = 3), "  "),
                       paste0("Median :", format(round(res[[i]]$percentiles$values[6],3), nsmall = 3), "  "),
                       paste0("Mean   :", format(round(res[[i]]$mean,3), nsmall = 3), "  "),
                       paste0("3rd Qu.:", format(round(res[[i]]$percentiles$values[8],3), nsmall = 3), "  "),
                       paste0("Max.   :", format(round(res[[i]]$max[1],3), nsmall = 3), "  ")))
              }
              else if(res[[i]]$type == "enum") {
                col = matrix(rep("", 6), ncol=1)
                len = length(res[[i]]$histogram$bins)
                for(j in 1:min(6,len))
                  col[j] = paste0(res[[i]]$histogram$bin_names[len-j+1], ": ", res[[i]]$histogram$bins[len-j+1])
                result = cbind(result, col)
              }
            }
            result = as.table(result)
            rownames(result) <- rep("", 6)
            colnames(result) <- cnames
            result
          })

# setMethod("predict", signature(object="H2OGLMModel"), 
#          function(object) {
#            res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_PREDICT, model_key=object@key, key=object@data@key)
#            res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_INSPECT, key=res$response$redirect_request_args$key)
#            result = new("H2OParsedData", h2o=object@data@h2o, key=res$key)
#          })

# Internal functions & declarations
h2o.__PAGE_EXEC = "Exec.json"
h2o.__PAGE_GET = "GetVector.json"
h2o.__PAGE_IMPORTURL = "ImportUrl.json"
h2o.__PAGE_IMPORTFILES = "ImportFiles.json"
h2o.__PAGE_INSPECT = "Inspect.json"
h2o.__PAGE_JOBS = "Jobs.json"
h2o.__PAGE_PARSE = "Parse.json"
h2o.__PAGE_PUT = "PutVector.json"
h2o.__PAGE_REMOVE = "Remove.json"

h2o.__PAGE_SUMMARY = "SummaryPage.json"
h2o.__PAGE_PREDICT = "GeneratePredictionsPage.json"
h2o.__PAGE_GLM = "GLM.json"
h2o.__PAGE_KMEANS = "KMeans.json"
h2o.__PAGE_KMAPPLY = "KMeansApply.json"
h2o.__PAGE_KMSCORE = "KMeansScore.json"
h2o.__PAGE_RF  = "RF.json"
h2o.__PAGE_RFVIEW = "RFView.json"
h2o.__PAGE_RFTREEVIEW = "RFTreeView.json"

h2o.__remoteSend <- function(client, page, ...) {
  ip = client@ip
  port = client@port
  
  # Sends the given arguments as URL arguments to the given page on the specified server
  url = paste0("http://", ip, ":", port, "/", page)
  temp = postForm(url, style = "POST", ...)
  after = gsub("NaN", "\"NaN\"", temp[1])
  after = gsub("Inf", "\"Inf\"", after)
  res = fromJSON(after)
  
  if (!is.null(res$error)) {
    myTime = gsub(":", "-", date()); myTime = gsub(" ", "_", myTime)
    h2o.__writeToFile(res, paste0("error_json_", myTime, ".log"))
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
      result[i] = paste0(names(vec)[i], ": ", vec[i])
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
  prog$progress
}

h2o.__exec <- function(client, expr) {
  type = tryCatch({ typeof(expr) }, error = function(e) { "expr" })
  if (type != "character")
    expr = deparse(substitute(expr))
  res = h2o.__remoteSend(client, h2o.__PAGE_EXEC, expression=expr)
  res$key
}

h2o.__remove <- function(client, keyName) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  res = h2o.__remoteSend(client, h2o.__PAGE_REMOVE, key=keyName)
}