library('RCurl');
library('rjson');
library('xts');

# Class definitions
setClass("H2OClient", representation(ip="character", port="numeric"), prototype(ip="127.0.0.1", port=54321))
setClass("H2ORawData", representation(h2o="H2OClient", key="character"))
setClass("H2OParsedData", representation(h2o="H2OClient", key="character"))
setClass("H2OGLMModel", representation(key="character", data="H2OParsedData", glm="list"))
setClass("H2OKMeansModel", representation(key="character", data="H2OParsedData", km="list"))

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

# Generic definitions
setGeneric("importFile", function(object, path) { standardGeneric("importFile") })
# setGeneric("importURL", function(object, path) { standardGeneric("importURL") })
setGeneric("importURL", function(object, path, key) { standardGeneric("importURL") })
setGeneric("h2o.glm", function(x, y, data, family, nfolds, alpha) { standardGeneric("h2o.glm") })
setGeneric("h2o.kmeans", function(data, centers) { standardGeneric("h2o.kmeans") })
# setGeneric("h2o.kmeans", function(data, centers, iter.max = 10) { standardGeneric("h2o.kmeans") })

# Unique method definitions
setMethod("importFile", signature(object="H2OClient", path="character"), 
          function(object, path) {
            res = h2o.__remoteSend(object, h2o.__PAGE_IMPORTFILES, path=path)
            res = h2o.__remoteSend(object, h2o.__PAGE_PARSE, source_key=res$key)
            while(h2o.__poll(object, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
            parsedData = new("H2OParsedData", h2o=object, key=res$destination_key)
          })

setMethod("importURL", signature(object="H2OClient", path="character", key="character"),
          function(object, path, key) {
            res = h2o.__remoteSend(object, h2o.__PAGE_IMPORTURL, url=path)
            res = h2o.__remoteSend(object, h2o.__PAGE_PARSE, source_key=res$key, destination_key=key)
            while(h2o.__poll(object, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
            parsedData = new("H2OParsedData", h2o=object, key=res$destination_key)
          })

setMethod("importURL", signature(object="H2OClient", path="character", key="missing"),
          function(object, path) { importURL(object, path, "") })

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

setMethod("h2o.kmeans", signature(data="H2OParsedData", centers="numeric"),
          function(data, centers) {
            # Build k-means model
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_KMEANS, source_key=data@key, k=centers)
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
h2o.__PAGE_GLM = "GLM.json"
h2o.__PAGE_KMEANS = "KMeans.json"
h2o.__PAGE_KMAPPLY = "KMeansApply.json"
h2o.__PAGE_KMSCORE = "KMeansScore.json"
h2o.__PAGE_RF  = "RF.json"
h2o.__PAGE_RFVIEW = "RFView.json"

h2o.__remoteSend <- function(client, page, ...) {
  ip = client@ip
  port = client@port
  
  # Sends the given arguments as URL arguments to the given page on the specified server
  url = paste0("http://", ip, ":", port, "/", page)
  temp = postForm(url, style = "POST", ...)
  after = gsub("NaN", "\"NaN\"", temp[1])
  after = gsub("Inf", "\"Inf\"", after)
  res = fromJSON(after)
  
  if (!is.null(res$error))
    stop(paste(url," returned the following error:\n", h2o.__formatError(res$error)))
  res    
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