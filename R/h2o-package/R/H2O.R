if(!"RCurl" %in% rownames(installed.packages())) install.packages(RCurl)
if(!"rjson" %in% rownames(installed.packages())) install.packages(rjson)

library(RCurl)
library(rjson)

# Class definitions
setClass("H2OClient", representation(ip="character", port="numeric"), prototype(ip="127.0.0.1", port=54321))
setClass("H2ORawData", representation(h2o="H2OClient", key="character"))
setClass("H2OParsedData", representation(h2o="H2OClient", key="character"))
setClass("H2OGLMModel", representation(key="character", data="H2OParsedData", model="list"))
setClass("H2OKMeansModel", representation(key="character", data="H2OParsedData", model="list"))
setClass("H2ORForestModel", representation(key="character", data="H2OParsedData", model="list"))

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
  
  model = object@model
  print(round(model$coefficients,5))
  cat("\nDegrees of Freedom:", model$df.null, "Total (i.e. Null); ", model$df.residual, "Residual\n")
  cat("Null Deviance:    ", round(model$null.deviance,1), "\n")
  cat("Residual Deviance:", round(model$deviance,1), " AIC:", round(model$aic,1))
})

setMethod("show", "H2OKMeansModel", function(object) {
  print(object@data)
  cat("K-Means Model Key:", object@key)
  
  model = object@model
  cat("\n\nK-means clustering with", length(model$size), "clusters of sizes "); cat(model$size, sep=", ")
  cat("\n\nCluster means:\n"); print(model$centers)
  cat("\nClustering vector:\n"); print(model$cluster)  # summary(model$cluster) currently broken
  cat("\nWithin cluster sum of squares by cluster:\n"); print(model$withinss)
  cat("\nAvailable components:\n\n"); print(names(model))
})

setMethod("show", "H2ORForestModel", function(object) {
  print(object@data)
  cat("Random Forest Model Key:", object@key)
  
  model = object@model
  cat("\n\nType of random forest:", model$type)
  cat("\nNumber of trees:", model$ntree)
  cat("\n\nOOB estimate of error rate: ", round(100*model$oob_err, 2), "%", sep = "")
  cat("\nConfusion matrix:\n"); print(model$confusion)
})

setMethod("colnames", "H2OParsedData", function(x) {
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT, key=x@key)
  unlist(lapply(res$cols, function(y) y$name))
})

setMethod("names", "H2OParsedData", function(x) { colnames(x) })

setMethod("nrow", "H2OParsedData", function(x) { 
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT, key=x@key); res$num_rows })

setMethod("ncol", "H2OParsedData", function(x) {
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT, key=x@key); res$num_cols })

setMethod("summary", "H2OParsedData", function(object) {
  res = h2o.__remoteSend(object@h2o, h2o.__PAGE_SUMMARY, key=object@key)
  res = res$summary$columns
  result = NULL; cnames = NULL
  for(i in 1:length(res)) {
    cnames = c(cnames, paste("      ", res[[i]]$name, sep=""))
    if(res[[i]]$type == "number") {
      if(is.null(res[[i]]$percentiles))
        params = format(rep(round(as.numeric(res[[i]]$mean), 3), 6), nsmall = 3)
      else
        params = format(round(as.numeric(c(res[[i]]$min[1], res[[i]]$percentiles$values[4], res[[i]]$percentiles$values[6], res[[i]]$mean, res[[i]]$percentiles$values[8], res[[i]]$max[1])), 3), nsmall = 3)
      result = cbind(result, c(paste("Min.   :", params[1], "  ", sep=""), paste("1st Qu.:", params[2], "  ", sep=""),
                               paste("Median :", params[3], "  ", sep=""), paste("Mean   :", params[4], "  ", sep=""),
                               paste("3rd Qu.:", params[5], "  ", sep=""), paste("Max.   :", params[6], "  ", sep="")))                 
    }
    else if(res[[i]]$type == "enum") {
      col = matrix(rep("", 6), ncol=1)
      len = length(res[[i]]$histogram$bins)
      for(j in 1:min(6,len))
        col[j] = paste(res[[i]]$histogram$bin_names[len-j+1], ": ", res[[i]]$histogram$bins[len-j+1], sep="")
      result = cbind(result, col)
    }
  }
  result = as.table(result)
  rownames(result) <- rep("", 6)
  colnames(result) <- cnames
  result
})

setMethod("as.data.frame", "H2OParsedData", function(x) {
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT, key=x@key, offset=0, view=nrow(x))
  temp = unlist(lapply(res$rows, function(y) { y$row = NULL; y }))
  x.df = data.frame(matrix(temp, nrow = res$num_rows, byrow = TRUE))
  colnames(x.df) = unlist(lapply(res$cols, function(y) y$name))
  x.df
})

# Generic method definitions
setGeneric("h2o.checkClient", function(object) { standardGeneric("h2o.checkClient") })
setGeneric("h2o.ls", function(object) { standardGeneric("h2o.ls") })
# setGeneric("h2o.importFile", function(object, path, key = "", header = FALSE, parse = TRUE) { standardGeneric("h2o.importFile") })
setGeneric("h2o.importFile", function(object, path, key = "", parse = TRUE) { standardGeneric("h2o.importFile") })
setGeneric("h2o.importFolder", function(object, path, parse = TRUE) { standardGeneric("h2o.importFolder") })
# setGeneric("h2o.importURL", function(object, path, key="", header = FALSE, parse = TRUE) { standardGeneric("h2o.importURL") })
setGeneric("h2o.importURL", function(object, path, key = "", parse = TRUE) { standardGeneric("h2o.importURL") })
# setGeneric("h2o.importURL", function(object, path, key="") { standardGeneric("h2o.importURL") })
setGeneric("h2o.importHDFS", function(object, path, parse = TRUE) { standardGeneric("h2o.importHDFS") })
setGeneric("h2o.parseRaw", function(data, key = "") { standardGeneric("h2o.parseRaw") })
setGeneric("h2o.glm", function(x, y, data, family, nfolds = 10, alpha = 0.5, lambda = 1.0e-5) { standardGeneric("h2o.glm") })
setGeneric("h2o.kmeans", function(data, centers, cols = "", iter.max = 10) { standardGeneric("h2o.kmeans") })
setGeneric("h2o.randomForest", function(y, x_ignore = "", data, ntree, depth, classwt = as.numeric(NA)) { standardGeneric("h2o.randomForest") })
# setGeneric("h2o.randomForest", function(y, data, ntree, depth, classwt = as.numeric(NA)) { standardGeneric("h2o.randomForest") })
setGeneric("h2o.getTree", function(forest, k, plot = FALSE) { standardGeneric("h2o.getTree") })

# Unique methods to H2O
# H2O client management operations
setMethod("h2o.checkClient", signature(object="H2OClient"), function(object) { 
            myURL = paste("http://", object@ip, ":", object@port, sep="")
            if(!url.exists(myURL)) {
              print("H2O is not running yet, starting it now.")
              h2o.__startLauncher()
            } else { 
              cat("Successfully connected to", myURL, "\n")
              if("h2o" %in% rownames(installed.packages()) && (pv=packageVersion("h2o")) != (sv=h2o.__version(object)))
                warning(paste("Version mismatch! Server running H2O version", sv, "but R package is version", pv))
            }
          })

setMethod("h2o.ls", signature(object="H2OClient"), function(object) {
  res = h2o.__remoteSend(object, h2o.__PAGE_VIEWALL)
  myList = lapply(res$keys, function(y) c(y$key, y$value_size_bytes))
  temp = data.frame(matrix(unlist(myList), nrow = res$num_keys, byrow = TRUE))
  colnames(temp) = c("Key", "Bytesize")
  temp
})

# Data import operations
setMethod("h2o.importURL", signature(object="H2OClient", path="character", key="character", parse="logical"),
          function(object, path, key, parse) {
            destKey = ifelse(parse, "", key)
            res = h2o.__remoteSend(object, h2o.__PAGE_IMPORTURL, url=path, key=destKey)
            rawData = new("H2ORawData", h2o=object, key=res$key)
            if(parse) parsedData = h2o.parseRaw(rawData, key) else rawData
            })

setMethod("h2o.importURL", signature(object="H2OClient", path="character", key="character", parse="missing"),
          function(object, path, key, parse) { h2o.importURL(object, path, key, parse) })
          
setMethod("h2o.importURL", signature(object="H2OClient", path="character", key="missing", parse="logical"),
          function(object, path, key, parse) { h2o.importURL(object, path, key, parse) })

setMethod("h2o.importURL", signature(object="H2OClient", path="character", key="missing", parse="missing"),
          function(object, path, key, parse) { h2o.importURL(object, path, key, parse) })

setMethod("h2o.importFolder", signature(object="H2OClient", path="character", parse="logical"),
          function(object, path, parse) {
            if(!file.exists(path)) stop("Directory does not exist!")
            res = h2o.__remoteSend(object, h2o.__PAGE_IMPORTFILES, path=normalizePath(path))
            myKeys = res$keys
            myData = vector("list", length(myKeys))
            for(i in 1:length(myKeys)) {
              rawData = new("H2ORawData", h2o=object, key=myKeys[i])
              if(parse) {
                cat("Parsing key", myKeys[i], "\n")
                myData[[i]] = h2o.parseRaw(rawData, key="")
              }
              else myData[[i]] = rawData
            }
            myData
          })

setMethod("h2o.importFolder", signature(object="H2OClient", path="character", parse="missing"),
          function(object, path) { h2o.importFolder(object, path, parse = TRUE) })

setMethod("h2o.importFile", signature(object="H2OClient", path="character", key="missing", parse="logical"), 
          function(object, path, parse) { 
            if(!file.exists(path)) stop("File does not exist!")
            h2o.importFolder(object, path, parse)[[1]] })

setMethod("h2o.importFile", signature(object="H2OClient", path="character", key="missing", parse="missing"), 
          function(object, path) { h2o.importFile(object, path, parse = TRUE) })

setMethod("h2o.importFile", signature(object="H2OClient", path="character", key="character", parse="logical"), 
          function(object, path, key, parse) {
            if(!file.exists(path)) stop("File does not exist!")
            h2o.importURL(object, paste("file:///", normalizePath(path), sep=""), key, parse) })

setMethod("h2o.importFile", signature(object="H2OClient", path="character", key="character", parse="missing"), 
          function(object, path, key) { h2o.importFile(object, path, key, parse = TRUE) })

setMethod("h2o.importHDFS", signature(object="H2OClient", path="character", parse="logical"),
          function(object, path, parse) {
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
                  myData[[i]] = h2o.parseRaw(rawData, key="")
                }
                else myData[[i]] = rawData
              }
            }
            if(res$num_succeeded == 1) myData[[1]] else myData
          })

setMethod("h2o.importHDFS", signature(object="H2OClient", path="character", parse="missing"),
          function(object, path) { h2o.importHDFS(object, path, parse = TRUE) })

setMethod("h2o.parseRaw", signature(data="H2ORawData", key="character"), 
          function(data, key) {
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_PARSE, source_key=data@key, destination_key=key)
            while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
            parsedData = new("H2OParsedData", h2o=data@h2o, key=res$destination_key)
          })

setMethod("h2o.parseRaw", signature(data="H2ORawData", key="missing"), function(data, key) { h2o.parseRaw(data, key) })

# Model-building operations and algorithms
setMethod("h2o.glm", signature(x="character", y="character", data="H2OParsedData", family="character", nfolds="numeric", alpha="numeric", lambda="numeric"),
          function(x, y, data, family, nfolds, alpha, lambda) {
            # res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLM, key = data@key, y = y, x = paste(x, sep="", collapse=","), family = family, n_folds = nfolds, alpha = alpha, lambda = lambda)
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLM, key = data@key, y = y, x = paste(x, sep="", collapse=","), family = family, n_folds = nfolds, alpha = alpha, lambda = lambda, case_mode="=", case=1.0)
            # while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
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
            
            resGLMModel = new("H2OGLMModel", key=destKey, data=data, model=result)
            resGLMModel
          })

setMethod("h2o.glm", signature(x="character", y="character", data="H2OParsedData", family="character", nfolds="ANY", alpha="ANY", lambda="ANY"),
          function(x, y, data, family, nfolds, alpha, lambda) {
            if(!(missing(nfolds) || class(nfolds) == "numeric"))
              stop(paste("nfolds cannot be of class", class(nfolds)))
            else if(!(missing(alpha) || class(alpha) == "numeric"))
              stop(paste("alpha cannot be of class", class(alpha)))
            else if(!(missing(lambda) || class(lambda) == "numeric"))
              stop(paste("lambda cannot be of class", class(lambda)))
            h2o.glm(x, y, data, family, nfolds, alpha, lambda) 
            })

# setMethod("h2o.kmeans", signature(data="H2OParsedData", centers="numeric", iter.max="numeric"),
#          function(data, centers, iter.max) {
setMethod("h2o.kmeans", signature(data="H2OParsedData", centers="numeric", cols="character", iter.max="numeric"),
          function(data, centers, cols, iter.max) {
            # Build K-means model
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_KMEANS, source_key=data@key, k=centers, max_iter=iter.max, cols=paste(cols, sep="", collapse=","))
            while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
            destKey = res$destination_key
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_INSPECT, key=res$destination_key)
            res = res$KMeansModel
            result = list()
            if(typeof(res$clusters) == "double")
              result$centers = res$clusters
            else {
              result$centers = do.call(rbind, res$clusters)
              rownames(result$centers) <- seq(1,nrow(result$centers))
              if(cols[1] == "")
                colnames(result$centers) <- colnames(data)
              else {
                # mycols = unlist(strsplit(cols, split=","))
                if(length(grep("^[[:digit:]]*$", cols)) == length(cols))
                  colnames(result$centers) <- colnames(data)[as.numeric(cols)+1]
                else
                  colnames(result$centers) <- cols
              }
            }
            
            # Apply model to data set
            scoreKey = paste(strsplit(data@key, ".hex")[[1]], ".kmapply", sep="")
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
            
            resKMModel = new("H2OKMeansModel", key=destKey, data=data, model=result)
            resKMModel
          })

setMethod("h2o.kmeans", signature(data="H2OParsedData", centers="numeric", cols="ANY", iter.max="ANY"),
          function(data, centers, cols, iter.max) { 
            if(!(missing(cols) || class(cols) == "character" || class(cols) == "numeric"))
              stop(paste("cols cannot be of class", class(cols)))
            else if(!(missing(iter.max) || class(iter.max) == "numeric"))
              stop(paste("iter.max cannot be of class", class(iter.max)))
            h2o.kmeans(data, centers, as.character(cols), iter.max) 
            })

setMethod("h2o.randomForest", signature(y="character", x_ignore="character", data="H2OParsedData", ntree="numeric", depth="numeric", classwt="numeric"),
          function(y, x_ignore, data, ntree, depth, classwt) {
            # Set randomized model_key
            rand_model_key = paste("__RF_Model__", runif(n=1, max=1e10), sep="")
            
            # If no class weights, then default to all 1.0
            if(!any(is.na(classwt))) {
              myWeights = rep(NA, length(classwt))
              for(i in 1:length(classwt))
                myWeights[i] = paste(names(classwt)[i], classwt[i], sep="=")
              res = h2o.__remoteSend(data@h2o, h2o.__PAGE_RF, data_key=data@key, response_variable=y, ignore=paste(x_ignore, collapse=","), ntree=ntree, depth=depth, class_weights=paste(myWeights, collapse=","), model_key = rand_model_key)
            }
            else
              res = h2o.__remoteSend(data@h2o, h2o.__PAGE_RF, data_key=data@key, response_variable=y, ignore=paste(x_ignore, collapse=","), ntree=ntree, depth=depth, class_weights="", model_key = rand_model_key)
            while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
            destKey = res$destination_key
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_RFVIEW, model_key=destKey, data_key=data@key, out_of_bag_error_estimate=1)
            
            result = list()
            result$type = "classification"
            result$ntree = ntree
            result$oob_err = res$confusion_matrix$classification_error
            if(x_ignore[1] != "") result$x_ignore = paste(x_ignore, collapse = ", ")
            
            rf_matrix = cbind(matrix(unlist(res$trees$depth), nrow=3), matrix(unlist(res$trees$leaves), nrow=3))
            rownames(rf_matrix) = c("Min.", "Mean.", "Max.")
            colnames(rf_matrix) = c("Depth", "Leaves")
            result$forest = rf_matrix
            
            # Must check confusion matrix is finished calculating!
            cf = res$confusion_matrix
            cf_matrix = cbind(matrix(unlist(cf$scores), nrow=length(cf$header)), unlist(cf$classes_errors))
            rownames(cf_matrix) = cf$header
            colnames(cf_matrix) = c(cf$header, "class.error")
            result$confusion = cf_matrix
            
            resRFModel = new("H2ORForestModel", key=destKey, data=data, model=result)
            resRFModel
          })
            
setMethod("h2o.randomForest", signature(y="character", x_ignore="ANY", data="H2OParsedData", ntree="numeric", depth="numeric", classwt="ANY"),
          function(y, x_ignore, data, ntree, depth, classwt) {
            if(!(missing(x_ignore) || class(x_ignore) == "character" || class(x_ignore) == "numeric"))
               stop(paste("x_ignore cannot be of class", class(x_ignore)))
            if(!(missing(classwt) || class(classwt) == "numeric"))
              stop(paste("classwt cannot be of class", class(classwt)))
            h2o.randomForest(y, as.character(x_ignore), data, ntree, depth, classwt)
            })

setMethod("h2o.randomForest", signature(y="numeric", x_ignore="ANY", data="H2OParsedData", ntree="numeric", depth="numeric", classwt="ANY"),
          function(y, x_ignore, data, ntree, depth, classwt) {
            if(!(missing(x_ignore) || class(x_ignore) == "character" || class(x_ignore) == "numeric"))
              stop(paste("x_ignore cannot be of class", class(x_ignore)))
            if(!(missing(classwt) || class(classwt) == "numeric"))
              stop(paste("classwt cannot be of class", class(classwt)))
            h2o.randomForest(as.character(y), as.character(x_ignore), data, ntree, depth, classwt)
          })

setMethod("h2o.getTree", signature(forest="H2ORForestModel", k="numeric", plot="logical"),
          function(forest, k, plot) {
            if(k < 1 || k > forest@model$ntree)
              stop(paste("k must be between 1 and", forest@model$ntree))
            res = h2o.__remoteSend(forest@data@h2o, h2o.__PAGE_RFTREEVIEW, model_key=forest@key, tree_number=k-1, data_key=forest@data@key)
            if(plot) browseURL(paste0("http://", forest@data@h2o@ip, ":", forest@data@h2o@port, "/RFTreeView.html?model_key=", forest@key, "&data_key=", forest@data@key, "&tree_number=", k-1))
            
            result = list()
            result$depth = res$depth
            result$leaves = res$leaves
            result
          })

setMethod("h2o.getTree", signature(forest="H2ORForestModel", k="numeric", plot="missing"),
          function(forest, k) { h2o.getTree(forest, k, plot = FALSE) })

# setMethod("predict", signature(object="H2OGLMModel"), 
#          function(object) {
#            res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_PREDICT, model_key=object@key, data_key=object@data@key)
#            res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_INSPECT, key=res$response$redirect_request_args$key)
#            result = new("H2OParsedData", h2o=object@data@h2o, key=res$key)
#          })

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

h2o.__remove <- function(client, keyName) {
  type = tryCatch({ typeof(keyName) }, error = function(e) { "expr" })
  if (type != "character")
    keyName = deparse(substitute(keyName))
  res = h2o.__remoteSend(client, h2o.__PAGE_REMOVE, key=keyName)
}

h2o.__version <- function(client) {
  res = h2o.__remoteSend(client, h2o.__PAGE_CLOUD)
  res$version
}

h2o.__startLauncher <- function() {
  myOS = Sys.info()["sysname"]; myHome = Sys.getenv("HOME")
  
  if(myOS == "Windows") verPath = paste(myHome, "AppData/Roaming/h2o", sep="/")
  else verPath = paste(myHome, "Library/Application Support/h2o", sep="/")
  myFiles = list.files(verPath)
  if(length(myFiles) == 0) stop("Cannot find config file or folder")
  # Must trim myFiles so all have format 1.2.3.45678.txt (use regexpr)!
  
  # Get H2O with latest version number
  # If latest isn't working, maybe go down list to earliest until one executes?
  fileName = paste(verPath, tail(myFiles, n=1), sep="/")
  myVersion = strsplit(tail(myFiles, n=1), ".txt")[[1]]
  launchPath = readChar(fileName, file.info(fileName)$size)
  if(is.null(launchPath) || launchPath == "")
    stop(paste("No H2OLauncher.jar matching H2O version", myVersion, "found"))
  
  temp = getwd(); setwd(launchPath)
  if(myOS == "Windows") shell.exec("H2OLauncher.jar")
  else system(paste("open", launchPath))
  setwd(temp)
}