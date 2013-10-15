# Class definitions
setClass("H2OClient", representation(ip="character", port="numeric"), prototype(ip="127.0.0.1", port=54321))
setClass("H2ORawData", representation(h2o="H2OClient", key="character"))
setClass("H2OParsedData", representation(h2o="H2OClient", key="character"))
setClass("H2OParsedData2", representation(h2o="H2OClient", key="character"))
setClass("H2OLogicalData", contains="H2OParsedData")
setClass("H2OModel", representation(key="character", data="H2OParsedData", model="list", "VIRTUAL"))
setClass("H2OGrid", representation(key="character", data="H2OParsedData", model="list", sumtable="list", "VIRTUAL"))

setClass("H2OGLMModel", contains="H2OModel", representation(xval="list"))
setClass("H2OGLMGrid", contains="H2OGrid")
setClass("H2OKMeansModel", contains="H2OModel")
setClass("H2ORForestModel", contains="H2OModel")
setClass("H2OPCAModel", contains="H2OModel")
setClass("H2OGBMModel", contains="H2OModel")
setClass("H2OGBMGrid", contains="H2OGrid")

MAX_INSPECT_VIEW = 10000

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

setMethod("show", "H2OParsedData2", function(object) {
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
  cat("Residual Deviance:", round(model$deviance,1), " AIC:", ifelse( is.numeric(model$aic), round(model$aic,1), 'NaN'), "\n")
  cat("Avg Training Error Rate:", round(model$train.err,5), "\n")
  
  # if(model$family == "binomial") {
  if(model$family$family == "binomial") {
    cat("AUC:", ifelse(is.numeric(model$auc), round(model$auc,5), 'NaN'), " Best Threshold:", round(model$threshold,5), "\n")
    cat("\nConfusion Matrix:\n"); print(model$confusion)
  }
    
  if(length(object@xval) > 0) {
    cat("\nCross-Validation Models:\n")
    # if(model$family == "binomial") {
    if(model$family$family == "binomial") {
      modelXval = t(sapply(object@xval, function(x) { c(x@model$threshold, x@model$auc, x@model$class.err) }))
      colnames(modelXval) = c("Best Threshold", "AUC", "Err(0)", "Err(1)")
    } else {
      modelXval = sapply(object@xval, function(x) { x@model$train.err })
      modelXval = data.frame(modelXval)
      colnames(modelXval) = c("Error")
    }
    rownames(modelXval) = paste("Model", 0:(nrow(modelXval)-1))
    print(modelXval)
  }
})

setMethod("show", "H2OGLMGrid", function(object) {
  print(object@data)
  cat("GLMGrid Model Key:", object@key, "\n")
  
  temp = data.frame(t(sapply(object@sumtable, c)))
  cat("\nSummary\n"); print(temp)
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

setMethod("show", "H2OPCAModel", function(object) {
  print(object@data)
  cat("PCA Model Key:", object@key)
  
  model = object@model
  cat("\n\nStandard deviations:\n", model$sdev)
  cat("\n\nRotation:\n"); print(model$rotation)
})

setMethod("show", "H2OGBMModel", function(object) {
  print(object@data)
  cat("GBM Model Key:", object@key)
  
  model = object@model
  cat("\n\nConfusion matrix:\n"); print(model$confusion)
  cat("\nMean Squared error by tree:\n"); print(model$err)
})

setMethod("show", "H2OGBMGrid", function(object) {
  print(object@data)
  cat("GBMGrid Model Key:", object@key, "\n")
  
  temp = data.frame(t(sapply(object@sumtable, c)))
  cat("\nSummary\n"); print(temp)
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
    expr = paste(h2o.__escape(x@key), "[", j-1, "]", sep="")
  } else {
    if(class(i) == "H2OLogicalData") {
      opt = paste(h2o.__escape(x@key), h2o.__escape(i@key), sep=",")
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
      opt = paste(h2o.__escape(x@key), start-1, max(i_off), sep=",")
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
    expr = paste(h2o.__escape(x@key), "$", name, sep="")
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

histograms <- function(object) {UseMethod("histograms", object)}
setMethod("histograms", "H2OParsedData2", function(object) {
  res = h2o.__remoteSend(object@h2o, h2o.__PAGE_SUMMARY2, source=object@key)
  list.of.bins <- lapply(res$summaries, function(res) {
    counts <- res$bins
    breaks <- seq(res$start, by=res$binsz, length.out=length(res$bins))
    cbind(counts,breaks)
  })
})

setMethod("summary", "H2OParsedData2", function(object) {
  res = h2o.__remoteSend(object@h2o, h2o.__PAGE_SUMMARY2, source=object@key)
  col.summaries = res$summaries
  col.names     = res$names
  col.means     = res$means
  col.results   = mapply(c, res$summaries, res$names, res$means, SIMPLIFY=FALSE)
  for (i in 1:length(col.results))
    names(col.results[[i]])[(length(col.results[[i]]) - 1) : length(col.results[[i]])] <- c('name', 'mean')
  result = NULL

  result <- sapply(col.results, function(res) {
    if(is.null(res$domains)) { # numeric column
      if(is.null(res$mins) || length(res$mins) == 0) res$mins = NaN
      if(is.null(res$maxs) || length(res$maxs) == 0) res$maxs = NaN
      if(is.null(res$percentileValues))
        params = format(rep(round(as.numeric(col.means[[i]]), 3), 6), nsmall = 3)
      else
        params = format(round(as.numeric(c(
          res$mins[1],
          res$percentileValues[4],
          res$percentileValues[6],
          res$mean,
          res$percentileValues[8],
          tail(res$maxs, 1))), 3), nsmall = 3)
      result = c(paste("Min.   :", params[1], "  ", sep=""), paste("1st Qu.:", params[2], "  ", sep=""),
                 paste("Median :", params[3], "  ", sep=""), paste("Mean   :", params[4], "  ", sep=""),
                 paste("3rd Qu.:", params[5], "  ", sep=""), paste("Max.   :", params[6], "  ", sep="")) 
    }
    else {
      domains <- res$domains[res$maxs + 1]
      counts <- res$bins[res$maxs + 1]
      width <- max(cbind(nchar(domains), nchar(counts)))
      result <- paste(domains,
                      mapply(function(x, y) { paste(rep(' ',width + 1 - nchar(x) - nchar(y)), collapse='') }, domains, counts),
                      ":",
                      counts,
                      " ",
                      sep='')
      result[6] <- NA
      result
    }
  })
  
  result = as.table(result)
  rownames(result) <- rep("", 6)
  colnames(result) <- col.names
  result
})

setMethod("summary", "H2OPCAModel", function(object) {
  # TODO: Save propVar and cumVar from the Java output instead of computing here
  myVar = object@model$sdev^2
  myProp = myVar/sum(myVar)
  result = rbind(object@model$sdev, myProp, cumsum(myProp))   # Need to limit decimal places to 4
  colnames(result) = paste("PC", seq(1, length(myVar)), sep="")
  rownames(result) = c("Standard deviation", "Proportion of Variance", "Cumulative Proportion")
  
  cat("Importance of components:\n")
  print(result)
})

setMethod("as.data.frame", "H2OParsedData", function(x) {
  url <- paste('http://', x@h2o@ip, ':', x@h2o@port, '/downloadCsv?src_key=', x@key, sep='')
  ttt <- getURL(url)
  read.csv(textConnection(ttt))
})

setMethod("head", "H2OParsedData", function(x, n = 6L, ...) {
  if(n == 0 || !is.numeric(n)) stop("n must be a non-zero integer")
  n = round(n)
  # if(abs(n) > nrow(x)) stop(paste("n must be between 1 and", nrow(x)))
  numRows = nrow(x)
  if(n < 0 && abs(n) >= numRows) return(data.frame())
  myView = ifelse(n > 0, min(n, numRows), numRows+n)
  if(myView > MAX_INSPECT_VIEW) stop(paste("Cannot view more than", MAX_INSPECT_VIEW, "rows"))
  
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT, key=x@key, offset=0, view=myView)
  temp = unlist(lapply(res$rows, function(y) { y$row = NULL; y }))
  if(is.null(temp)) return(temp)
  x.df = data.frame(matrix(temp, nrow = myView, byrow = TRUE))
  colnames(x.df) = unlist(lapply(res$cols, function(y) y$name))
  x.df
  
  # if(n > 0) as.data.frame(x[1:n,])
  # else as.data.frame(x[1:(nrow(x)+n),])
})

setMethod("tail", "H2OParsedData", function(x, n = 6L, ...) {
  if(n == 0 || !is.numeric(n)) stop("n must be a non-zero integer")
  n = round(n)
  # if(abs(n) > nrow(x)) stop(paste("n must be between 1 and", nrow(x)))
  numRows = nrow(x)
  if(n < 0 && abs(n) >= numRows) return(data.frame())
  myOff = ifelse(n > 0, max(0, numRows-n), abs(n))
  myView = ifelse(n > 0, min(n, numRows), numRows+n)
  if(myView > MAX_INSPECT_VIEW) stop(paste("Cannot view more than", MAX_INSPECT_VIEW, "rows"))
  
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT, key=x@key, offset=myOff, view=myView)
  temp = unlist(lapply(res$rows, function(y) { y$row = NULL; y }))
  if(is.null(temp)) return(temp)
  x.df = data.frame(matrix(temp, nrow = myView, byrow = TRUE))
  colnames(x.df) = unlist(lapply(res$cols, function(y) y$name))
  x.df
  
  # if(n > 0) opt = paste(h2o.__escape(x@key), nrow(x)-n, sep=",")
  # else opt = paste(h2o.__escape(x@key), abs(n), sep=",")
  # res = h2o.__exec(x@h2o, paste("slice(", opt, ")", sep=""))
  # as.data.frame(new("H2OParsedData", h2o=x@h2o, key=res))
})

setMethod("plot", "H2OPCAModel", function(x, y, ...) {
  barplot(x@model$sdev^2)
  title(main = paste("h2o.prcomp(", x@data@key, ")", sep=""), ylab = "Variances")
})

setGeneric("h2o.factor", function(data, col) { standardGeneric("h2o.factor") })
setMethod("h2o.factor", signature(data="H2OParsedData", col="numeric"),
   function(data, col) {
     if(col < 1 || col > ncol(data)) stop("col must be between 1 and ", ncol(data))
     col = col - 1
      newCol = paste("factor(", h2o.__escape(data@key), "[", col, "])", sep="")
      expr = paste("colSwap(", h2o.__escape(data@key), ",", col, ",", newCol, ")", sep="")
      res = h2o.__exec_dest_key(data@h2o, expr, destKey=data@key)
      data
})

setMethod("h2o.factor", signature(data="H2OParsedData", col="character"), 
   function(data, col) {
      ind = match(col, colnames(data))
      if(is.na(ind)) stop("Column ", col, " does not exist in ", data@key)
      h2o.factor(data, ind-1)
})

#--------------------------------- FluidVecs --------------------------------------#
setMethod("colnames", "H2OParsedData2", function(x) {
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT2, src_key=x@key)
  unlist(lapply(res$cols, function(y) y$name))
})

setMethod("names", "H2OParsedData2", function(x) { colnames(x) })

setMethod("nrow", "H2OParsedData2", function(x) { 
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT2, src_key=x@key); res$numRows })

setMethod("ncol", "H2OParsedData2", function(x) {
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT2, src_key=x@key); res$numCols })

setMethod("as.data.frame", "H2OParsedData2", function(x) {
  as.data.frame(new("H2OParsedData", h2o=x@h2o, key=x@key))
})

setMethod("head", "H2OParsedData2", function(x, n = 6L, ...) { 
  head(new("H2OParsedData", h2o=x@h2o, key=x@key), n, ...)
})

setMethod("tail", "H2OParsedData2", function(x, n = 6L, ...) {
  tail(new("H2OParsedData", h2o=x@h2o, key=x@key), n, ...)
})
