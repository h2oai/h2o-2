# Class definitions
setClass("H2OClient", representation(ip="character", port="numeric"), prototype(ip="127.0.0.1", port=54321))
setClass("H2ORawData", representation(h2o="H2OClient", key="character"))
setClass("H2OParsedData", representation(h2o="H2OClient", key="character"))
setClass("H2OGLMModel", representation(key="character", data="H2OParsedData", model="list"))
setClass("H2OKMeansModel", representation(key="character", data="H2OParsedData", model="list"))
setClass("H2ORForestModel", representation(key="character", data="H2OParsedData", model="list"))
setClass("H2OGLMGridModel", representation(key="character", data="H2OParsedData", model="list"))

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
  if(is.null(temp)) return(temp)
  x.df = data.frame(matrix(temp, nrow = res$num_rows, byrow = TRUE))
  colnames(x.df) = unlist(lapply(res$cols, function(y) y$name))
  x.df
})


setMethod("show", "H2OGLMGridModel", function(object) {
  print(object@data)
  cat("GLMGrid Model Key:", object@key, "\n\nSummary\n")
  
  model = object@model
  print(model$Summary)
  })
