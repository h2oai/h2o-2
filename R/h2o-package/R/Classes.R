# Class definitions
setClass("H2OClient", representation(ip="character", port="numeric"), prototype(ip="127.0.0.1", port=54321))
setClass("H2ORawData", representation(h2o="H2OClient", key="character"))
setClass("H2OParsedData", representation(h2o="H2OClient", key="character"))
setClass("H2OLogicalData", contains="H2OParsedData")
setClass("H2OModel", representation(key="character", data="H2OParsedData", model="list", "VIRTUAL"))
setClass("H2OGLMModel", contains="H2OModel")
setClass("H2OKMeansModel", contains="H2OModel")
setClass("H2ORForestModel", contains="H2OModel")
setClass("H2OGLMGridModel", contains="H2OModel")

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
  cat("\nDegrees of Freedom:", model$df.null, "Total (i.e. Null); ", model$df.residual, "Residual")
  cat("\nNull Deviance:    ", round(model$null.deviance,1))
  cat("\nResidual Deviance:", round(model$deviance,1), " AIC:", round(model$aic,1))
  
  cat("\n\nAvg Training Error:", model$training.err)
  cat("\nBest Threshold:", model$threshold, " AUC:", model$auc)
  cat("\n\nConfusion Matrix:\n"); print(model$confusion)
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

setMethod("+", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__operator("+", e1, e2) })
setMethod("-", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__operator("-", e1, e2) })
setMethod("*", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__operator("*", e1, e2) })
setMethod("/", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__operator("/", e1, e2) })
setMethod("%%", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__operator("%", e1, e2) })
setMethod("==", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__operator("==", e1, e2) })
setMethod(">", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__operator(">", e1, e2) })
setMethod("<", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__operator("<", e1, e2) })
setMethod("!=", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__operator("!=", e1, e2) })
setMethod(">=", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__operator(">=", e1, e2) })
setMethod("<=", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__operator("<=", e1, e2) })

setMethod("+", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__operator("+", e1, e2) })
setMethod("-", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__operator("-", e1, e2) })
setMethod("*", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__operator("*", e1, e2) })
setMethod("/", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__operator("/", e1, e2) })
setMethod("%%", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__operator("%", e1, e2) })
setMethod("==", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__operator("==", e1, e2) })
setMethod(">", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__operator(">", e1, e2) })
setMethod("<", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__operator("<", e1, e2) })
setMethod("!=", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__operator("!=", e1, e2) })
setMethod(">=", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__operator(">=", e1, e2) })
setMethod("<=", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__operator("<=", e1, e2) })

setMethod("+", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__operator("+", e1, e2) })
setMethod("-", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__operator("-", e1, e2) })
setMethod("*", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__operator("*", e1, e2) })
setMethod("/", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__operator("/", e1, e2) })
setMethod("%%", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__operator("%", e1, e2) })
setMethod("==", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__operator("==", e1, e2) })
setMethod(">", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__operator(">", e1, e2) })
setMethod("<", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__operator("<", e1, e2) })
setMethod("!=", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__operator("!=", e1, e2) })
setMethod(">=", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__operator(">=", e1, e2) })
setMethod("<=", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__operator("<=", e1, e2) })

setMethod("min", "H2OParsedData", function(x) { h2o.__func("min", x, "Number") })
setMethod("max", "H2OParsedData", function(x) { h2o.__func("max", x, "Number") })
setMethod("mean", "H2OParsedData", function(x) { h2o.__func("mean", x, "Number") })
setMethod("sum", "H2OParsedData", function(x) { h2o.__func("sum", x, "Number") })
setMethod("log2", "H2OParsedData", function(x) { h2o.__func("log", x, "Vector") })

setMethod("[", "H2OParsedData", function(x, i, j, ..., drop = TRUE) {
  # Currently, you can only select one column at a time
  if(!missing(j) && length(j) > 1) stop("Currently, can only select one column at a time")
  if(missing(i) && missing(j)) return(x)
  if(missing(i) && !missing(j)) {
    if(is.character(j)) return(do.call("$", c(x, j)))
    expr = paste(x@key, "[", j-1, "]", sep="")
  } else {
    if(class(i) == "H2OLogicalData") {
      opt = paste(x@key, i@key, sep=",")
      if(missing(j))
        expr = paste("filter(", opt, ")", sep="")
      else if(is.character(j))
        expr = paste("filter(", opt, ")$", j, sep="")
      else if(is.numeric(j))
        expr = paste("filter(", opt, ")[", j-1, "]", sep="")
      else stop("Rows must be numeric or column names")
    }
    else if(is.numeric(i)) {
      start = min(i); i_off = i - start + 1;
      opt = paste(x@key, start-1, max(i_off), sep=",")
      if(missing(j))
        expr = paste("slice(", opt, ")", sep="")
      else if(is.character(j))
        expr = paste("slice(", opt, ")$", j, sep="")
      else if(is.numeric(j))
        expr = paste("slice(", opt, ")[", j-1, "]", sep="")
    } else stop("Rows must be numeric or column names")
  }
  res = h2o.__exec(x@h2o, expr)
  new("H2OParsedData", h2o=x@h2o, key=res)
})

setMethod("$", "H2OParsedData", function(x, name) {
  myNames = names(x)
  if(!(name %in% myNames)) {
    print("Column", as.character(name), "not present in expression"); return(NULL)
  } else {
    # x[match(name, myNames)]
    expr = paste(x@key, "$", name, sep="")
    res = h2o.__exec(x@h2o, expr)
    new("H2OParsedData", h2o=x@h2o, key=res)
  }
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
      if(is.null(res[[i]]$min) || length(res[[i]]$min) == 0) res[[i]]$min = NaN
      if(is.null(res[[i]]$max) || length(res[[i]]$max) == 0) res[[i]]$max = NaN
      if(is.null(res[[i]]$mean) || length(res[[i]]$mean) == 0) res[[i]]$mean = NaN
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

setMethod("head", "H2OParsedData", function(x, n = 6L, ...) {
  if(n == 0 || !is.numeric(n)) stop("n must be a non-zero integer")
  n = round(n)
  if(n > 0) as.data.frame(x[1:n,])
  else as.data.frame(x[1:(nrow(x)+n),])
})

setMethod("tail", "H2OParsedData", function(x, n = 6L, ...) {
  if(n == 0 || !is.numeric(n)) stop("n must be a non-zero integer")
  n = round(n)
  if(n > 0) opt = paste(x@key, nrow(x)-n, sep=",")
  else opt = paste(x@key, abs(n), sep=",")
  res = h2o.__exec(x@h2o, paste("slice(", opt, ")", sep=""))
  as.data.frame(new("H2OParsedData", h2o=x@h2o, key=res))
})

setMethod("show", "H2OGLMGridModel", function(object) {
  print(object@data)
  cat("GLMGrid Model Key:", object@key, "\n\nSummary\n")
  
  model = object@model
  print(model$Summary)
  })
