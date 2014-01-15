MAX_INSPECT_VIEW = 10000

# Class definitions
# WARNING: Do NOT touch the env slot! It is used to link garbage collection between R and H2O
# setClass("H2OClient", representation(ip="character", port="numeric"), prototype(ip="127.0.0.1", port=54321))
setClass("H2OClient", representation(ip="character", port="numeric"), prototype(ip="127.0.0.1", port=54321))
setClass("H2ORawData", representation(h2o="H2OClient", key="character", env="environment"))
# setClass("H2OParsedData", representation(h2o="H2OClient", key="character"))
setClass("H2OParsedData", representation(h2o="H2OClient", key="character", env="environment", logic="logical"), prototype(logic=FALSE))
setClass("H2OModel", representation(key="character", data="H2OParsedData", model="list", env="environment", "VIRTUAL"))
setClass("H2OGrid", representation(key="character", data="H2OParsedData", model="list", sumtable="list", "VIRTUAL"))

setClass("H2OGLMModel", contains="H2OModel", representation(xval="list"))
# setClass("H2OGLMGrid", contains="H2OGrid")
setClass("H2OKMeansModel", contains="H2OModel")
setClass("H2ONNModel", contains="H2OModel", representation(valid="H2OParsedData"))
setClass("H2ODRFModel", contains="H2OModel", representation(valid="H2OParsedData"))
setClass("H2OPCAModel", contains="H2OModel")
setClass("H2OGBMModel", contains="H2OModel", representation(valid="H2OParsedData"))

setClass("H2OGLMGrid", contains="H2OGrid")
setClass("H2OGBMGrid", contains="H2OGrid")
setClass("H2OKMeansGrid", contains="H2OGrid")
setClass("H2ODRFGrid", contains="H2OGrid")
setClass("H2ONNGrid", contains="H2OGrid")

setClass("H2ORawDataVA", representation(h2o="H2OClient", key="character", env="environment"))
setClass("H2OParsedDataVA", representation(h2o="H2OClient", key="character", env="environment"))
setClass("H2OModelVA", representation(key="character", data="H2OParsedDataVA", model="list", env="environment", "VIRTUAL"))
setClass("H2OGridVA", representation(key="character", data="H2OParsedDataVA", model="list", sumtable="list", "VIRTUAL"))
setClass("H2OGLMModelVA", contains="H2OModelVA", representation(xval="list"))
setClass("H2OGLMGridVA", contains="H2OGridVA")
setClass("H2ORFModelVA", contains="H2OModelVA")

# Register finalizers for H2O data and model objects
# setMethod("initialize", "H2ORawData", function(.Object, h2o = new("H2OClient"), key = "") {
#   .Object@h2o = h2o
#   .Object@key = key
#   .Object@env = new.env()
#
#   assign("h2o", .Object@h2o, envir = .Object@env)
#   assign("key", .Object@key, envir = .Object@env)
#
#   # Empty keys don't refer to any object in H2O
#   if(key != "") reg.finalizer(.Object@env, h2o.__finalizer)
#   return(.Object)
# })
#
# setMethod("initialize", "H2OParsedData", function(.Object, h2o = new("H2OClient"), key = "") {
#   .Object@h2o = h2o
#   .Object@key = key
#   .Object@env = new.env()
#
#   assign("h2o", .Object@h2o, envir = .Object@env)
#   assign("key", .Object@key, envir = .Object@env)
#
#   # Empty keys don't refer to any object in H2O
#   if(key != "") reg.finalizer(.Object@env, h2o.__finalizer)
#   return(.Object)
# })
#
# setMethod("initialize", "H2OModel", function(.Object, key = "", data = new("H2OParsedData"), model = list()) {
#   .Object@key = key
#   .Object@data = data
#   .Object@model = model
#   .Object@env = new.env()
#
#   assign("h2o", .Object@data@h2o, envir = .Object@env)
#   assign("key", .Object@key, envir = .Object@env)
#
#   # Empty keys don't refer to any object in H2O
#   if(key != "") reg.finalizer(.Object@env, h2o.__finalizer)
#   return(.Object)
# })

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

setMethod("show", "H2OGrid", function(object) {
  print(object@data)
  cat("Grid Search Model Key:", object@key, "\n")

  temp = data.frame(t(sapply(object@sumtable, c)))
  cat("\nSummary\n"); print(temp)
})

setMethod("show", "H2OGLMModel", function(object) {
  print(object@data)
  cat("GLM2 Model Key:", object@key, "\n\n")

  model = object@model
  cat("Coefficients:\n"); print(round(model$coefficients,5))
  cat("\nNormalized Coefficients:\n"); print(round(model$normalized_coefficients,5))
  cat("\nDegrees of Freedom:", model$df.null, "Total (i.e. Null); ", model$df.residual, "Residual\n")
  cat("Null Deviance:    ", round(model$null.deviance,1), "\n")
  cat("Residual Deviance:", round(model$deviance,1), " AIC:", round(model$aic,1), "\n")
  cat("Avg Training Error Rate:", round(model$train.err,5), "\n")

  # if(model$family == "binomial") {
  if(model$family$family == "binomial") {
    cat("AUC:", round(model$auc,5), " Best Threshold:", round(model$best_threshold,5), "\n")
    cat("\nConfusion Matrix:\n"); print(model$confusion,2)
  }

  if(length(object@xval) > 0) {
    cat("\nCross-Validation Models:\n")
    # if(model$family == "binomial") {
    if(model$family$family == "binomial") {
      modelXval = t(sapply(object@xval, function(x) { c(x@model$rank-1, x@model$auc, 1-x@model$deviance/x@model$null.deviance) }))
      colnames(modelXval) = c("Nonzeros", "AUC", "Deviance Explained")
    } else {
      modelXval = t(sapply(object@xval, function(x) { c(x@model$rank-1, x@model$aic, 1-x@model$deviance/x@model$null.deviance) }))
      colnames(modelXval) = c("Nonzeros", "AIC", "Deviance Explained")
    }
    rownames(modelXval) = paste("Model", 1:nrow(modelXval))
    print(modelXval)
  }
})

setMethod("show", "H2OKMeansModel", function(object) {
  print(object@data)
  cat("K-Means Model Key:", object@key)

  model = object@model
  cat("\n\nK-means clustering with", length(model$size), "clusters of sizes "); cat(model$size, sep=", ")
  cat("\n\nCluster means:\n"); print(model$centers)
  cat("\nClustering vector:\n"); print(summary(model$cluster))
  cat("\nWithin cluster sum of squares by cluster:\n"); print(model$withinss)
  cat("\nAvailable components:\n\n"); print(names(model))
})

setMethod("show", "H2ONNModel", function(object) {
  print(object@data)
  cat("Neural Net Model Key:", object@key)

  model = object@model
  cat("\n\nTraining classification error:", model$train_class_error)
  cat("\nTraining square error:", model$train_sqr_error)
  cat("\n\nValidation classification error:", model$valid_class_error)
  cat("\nValidation square error:", model$valid_sqr_error)
  cat("\n\nConfusion matrix:\n"); cat("Reported on", object@valid@key, "\n"); print(model$confusion)
})

setMethod("show", "H2ODRFModel", function(object) {
  print(object@data)
  cat("Distributed Random Forest Model Key:", object@key)

  model = object@model
  cat("\nNumber of trees:", model$ntree)
  cat("\nTree statistics:\n"); print(model$forest)
  cat("\nConfusion matrix:\n"); cat("Reported on", object@valid@key, "\n"); print(model$confusion)
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
  if( model$classification ){
    cat("\n\nConfusion matrix:\nReported on", object@valid@key, "\n");
    print(model$confusion)
  }
  cat("\nMean Squared error by tree:\n"); print(model$err)
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

setMethod("plot", "H2OPCAModel", function(x, y, ...) {
  barplot(x@model$sdev^2)
  title(main = paste("h2o.prcomp(", x@data@key, ")", sep=""), ylab = "Variances")
})

# i are the rows, j are the columns. These can be vectors of integers or character strings, or a single logical data object
setMethod("[", "H2OParsedData", function(x, i, j, ..., drop = TRUE) {
  numRows = nrow(x); numCols = ncol(x)
  if (!missing(j) && is.numeric(j) && any(abs(j) < 1 || abs(j) > numCols))
    stop("Array index out of bounds")

  if(missing(i) && missing(j)) return(x)
  if(missing(i) && !missing(j)) {
    if(is.character(j)) {
      # return(do.call("$", c(x, j)))
      myCol = colnames(x)
      if(any(!(j %in% myCol))) stop("Undefined columns selected")
      j = match(j, myCol)
    }
    # if(is.logical(j)) j = -which(!j)
    if(is.logical(j)) j = which(j)

    # if(class(j) == "H2OLogicalData")
    if(class(j) == "H2OParsedData" && j@logic)
      expr = paste(x@key, "[", j@key, ",]", sep="")
    else if(is.numeric(j) || is.integer(j))
      expr = paste(x@key, "[,c(", paste(j, collapse=","), ")]", sep="")
    else stop(paste("Column index of type", class(j), "unsupported!"))
  } else if(!missing(i) && missing(j)) {
    # if(is.logical(i)) i = -which(!i)
    if(is.logical(i)) i = which(i)
    # if(class(i) == "H2OLogicalData")
    if(class(i) == "H2OParsedData" && i@logic)
      expr = paste(x@key, "[", i@key, ",]", sep="")
    else if(is.numeric(i) || is.integer(i))
      expr = paste(x@key, "[c(", paste(i, collapse=","), "),]", sep="")
    else stop(paste("Row index of type", class(i), "unsupported!"))
  } else {
    # if(is.logical(i)) i = -which(!i)
    if(is.logical(i)) i = which(i)
    # if(class(i) == "H2OLogicalData") rind = i@key
    if(class(i) == "H2OParsedData" && i@logic) rind = i@key
    else if(is.numeric(i) || is.integer(i))
      rind = paste("c(", paste(i, collapse=","), ")", sep="")
    else stop(paste("Row index of type", class(i), "unsupported!"))

    if(is.character(j)) {
      # return(do.call("$", c(x, j)))
      myCol = colnames(x)
      if(any(!(j %in% myCol))) stop("Undefined columns selected")
      j = match(j, myCol)
    }
    # if(is.logical(j)) j = -which(!j)
    if(is.logical(j)) j = which(j)
    # if(class(j) == "H2OLogicalData") cind = j@key
    if(class(j) == "H2OParsedData" && j@logic) cind = j@key
    else if(is.numeric(j) || is.integer(j))
      cind = paste("c(", paste(j, collapse=","), ")", sep="")
    else stop(paste("Column index of type", class(j), "unsupported!"))
    expr = paste(x@key, "[", rind, ",", cind, "]", sep="")
  }
  res = h2o.__exec2(x@h2o, expr)
  if(res$num_rows == 0 && res$num_cols == 0)
    res$scalar
  else
    new("H2OParsedData", h2o=x@h2o, key=res$dest_key)
})

setMethod("$", "H2OParsedData", function(x, name) {
  myNames = colnames(x)
  if(!(name %in% myNames)) return(NULL)
  cind = match(name, myNames)
  expr = paste(x@key, "[,", cind, "]", sep="")
  res = h2o.__exec2(x@h2o, expr)
  if(res$num_rows == 0 && res$num_cols == 0)
    res$scalar
  else
    new("H2OParsedData", h2o=x@h2o, key=res$dest_key)
})

setMethod("[<-", "H2OParsedData", function(x, i, j, ..., value) {
  numRows = nrow(x); numCols = ncol(x)
  # if((!missing(i) && is.numeric(i) && any(abs(i) < 1 || abs(i) > numRows)) ||
  #     (!missing(j) && is.numeric(j) && any(abs(j) < 1 || abs(j) > numCols)))
  #  stop("Array index out of bounds!")
  if(!(missing(i) || is.numeric(i)) || !(missing(j) || is.numeric(j) || is.character(j)))
    stop("Row/column types not supported!")

  if(!missing(i) && is.numeric(i)) {
    if(any(i == 0)) stop("Array index out of bounds")
    if(any(i < 0 && abs(i) > numRows)) stop("Unimplemented: can't extend rows")
    if(min(i) > numRows+1) stop("new rows would leave holes after existing rows")
  }
  if(!missing(j) && is.numeric(j)) {
    if(any(j == 0)) stop("Array index out of bounds")
    if(any(j < 0 && abs(j) > numCols)) stop("Unimplemented: can't extend columns")
    if(min(j) > numCols+1) stop("new columns would leaves holes after existing columns")
  }

  if(missing(i) && missing(j))
    lhs = x@key
  else if(missing(i) && !missing(j)) {
    if(is.character(j)) {
      myNames = colnames(x)
      if(any(!(j %in% myNames))) stop("Unimplemented: undefined column names specified")
      cind = match(j, myNames)
    } else cind = j
    cind = paste("c(", paste(cind, collapse = ","), ")", sep = "")
    lhs = paste(x@key, "[,", cind, "]", sep = "")
  } else if(!missing(i) && missing(j)) {
    rind = paste("c(", paste(i, collapse = ","), ")", sep = "")
    lhs = paste(x@key, "[", rind, ",]", sep = "")
  } else {
    if(is.character(j)) {
      myNames = colnames(x)
      if(any(!(j %in% myNames))) stop("Unimplemented: undefined column names specified")
      cind = match(j, myNames)
    } else cind = j
    cind = paste("c(", paste(cind, collapse = ","), ")", sep = "")
    rind = paste("c(", paste(i, collapse = ","), ")", sep = "")
    lhs = paste(x@key, "[", rind, ",", cind, "]", sep = "")
  }

  rhs = ifelse(class(value) == "H2OParsedData", value@key, value)
  res = h2o.__exec2(x@h2o, paste(lhs, "=", rhs))
  return(x)
})

setMethod("$<-", "H2OParsedData", function(x, name, value) {
  myNames = colnames(x)
  idx = match(name, myNames)
  if(is.na(idx)) {
    stop("Unimplemented: undefined column name specified")
    lhs = paste(x@key, "[,", ncol(x)+1, "]", sep = "")
    # TODO: Set column name of ncol(x) + 1 to name
  } else
    lhs = paste(x@key, "[,", idx, "]", sep="")
  rhs = ifelse(class(value) == "H2OParsedData", value@key, paste("c(", paste(value, collapse = ","), ")", sep=""))
  res = h2o.__exec2(x@h2o, paste(lhs, "=", rhs))
  return(x)
})

setMethod("+", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__binop2("+", e1, e2) })
setMethod("-", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__binop2("-", e1, e2) })
setMethod("*", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__binop2("*", e1, e2) })
setMethod("/", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__binop2("/", e1, e2) })
setMethod("%%", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__binop2("%", e1, e2) })
setMethod("==", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__binop2("==", e1, e2) })
setMethod(">", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__binop2(">", e1, e2) })
setMethod("<", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__binop2("<", e1, e2) })
setMethod("!=", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__binop2("!=", e1, e2) })
setMethod(">=", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__binop2(">=", e1, e2) })
setMethod("<=", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__binop2("<=", e1, e2) })
setMethod("&", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__binop2("&", e1, e2) })
setMethod("|", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { h2o.__binop2("|", e1, e2) })

setMethod("+", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__binop2("+", e1, e2) })
setMethod("-", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__binop2("-", e1, e2) })
setMethod("*", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__binop2("*", e1, e2) })
setMethod("/", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__binop2("/", e1, e2) })
setMethod("%%", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__binop2("%", e1, e2) })
setMethod("==", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__binop2("==", e1, e2) })
setMethod(">", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__binop2(">", e1, e2) })
setMethod("<", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__binop2("<", e1, e2) })
setMethod("!=", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__binop2("!=", e1, e2) })
setMethod(">=", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__binop2(">=", e1, e2) })
setMethod("<=", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__binop2("<=", e1, e2) })
setMethod("&", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__binop2("&", e1, e2) })
setMethod("|", c("numeric", "H2OParsedData"), function(e1, e2) { h2o.__binop2("|", e1, e2) })

setMethod("+", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__binop2("+", e1, e2) })
setMethod("-", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__binop2("-", e1, e2) })
setMethod("*", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__binop2("*", e1, e2) })
setMethod("/", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__binop2("/", e1, e2) })
setMethod("%%", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__binop2("%", e1, e2) })
setMethod("==", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__binop2("==", e1, e2) })
setMethod(">", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__binop2(">", e1, e2) })
setMethod("<", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__binop2("<", e1, e2) })
setMethod("!=", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__binop2("!=", e1, e2) })
setMethod(">=", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__binop2(">=", e1, e2) })
setMethod("<=", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__binop2("<=", e1, e2) })
setMethod("&", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__binop2("&", e1, e2) })
setMethod("|", c("H2OParsedData", "numeric"), function(e1, e2) { h2o.__binop2("|", e1, e2) })

setMethod("!", "H2OParsedData", function(x) { h2o.__unop2("!", x) })
setMethod("abs", "H2OParsedData", function(x) { h2o.__unop2("abs", x) })
setMethod("sign", "H2OParsedData", function(x) { h2o.__unop2("sgn", x) })
setMethod("sqrt", "H2OParsedData", function(x) { h2o.__unop2("sqrt", x) })
setMethod("ceiling", "H2OParsedData", function(x) { h2o.__unop2("ceil", x) })
setMethod("floor", "H2OParsedData", function(x) { h2o.__unop2("floor", x) })
setMethod("log", "H2OParsedData", function(x) { h2o.__unop2("log", x) })
setMethod("exp", "H2OParsedData", function(x) { h2o.__unop2("exp", x) })
setMethod("is.na", "H2OParsedData", function(x) { h2o.__unop2("is.na", x) })

setGeneric("as.h2o", function(h2o, object) { standardGeneric("as.h2o") })
setGeneric("as.h2o.key", function(h2o, object, key) { standardGeneric("as.h2o.key") })

setMethod("as.h2o", signature(h2o="H2OClient", object="data.frame"),
 function(h2o, object) {
   tmpf <- tempfile(fileext=".csv")
   write.csv(object, file=tmpf, quote=F, row.names=F)
   destKey = paste(TEMP_KEY, ".", pkg.env$temp_count, sep="")
   pkg.env$temp_count = (pkg.env$temp_count + 1) %% RESULT_MAX
   h2f <- h2o.uploadFile(h2o, tmpf, key=destKey)
   unlink(tmpf)
   return(h2f)
 })

setMethod("as.h2o", signature(h2o="H2OClient", object="numeric"),
  function(h2o, object) {
    res <- h2o.__exec2(h2o, paste("c(", paste(object, sep=',', collapse=","), ")", collapse=""))
    return(new("H2OParsedData", h2o=h2o, key=res$dest_key))
  })

setMethod("as.h2o.key", signature(h2o="H2OClient", object="numeric", key="character"),
  function(h2o, object, key) {
    res <- h2o.__exec2_dest_key(h2o, paste("c(", paste(object, sep=',', collapse=","), ")", collapse=""), key)
    return(res$dest_key)
  })

setGeneric("h2o.cut", function(x, breaks) { standardGeneric("h2o.cut") })
setMethod("h2o.cut", signature(x="H2OParsedData", breaks="numeric"), function(x, breaks) {
  nums = ifelse(length(breaks) == 1, breaks, paste("c(", paste(breaks, collapse=","), ")", sep=""))
  expr = paste("cut(", x@key, ",", nums, ")", sep="")
  res = h2o.__exec2(x@h2o, expr)
  if(res$num_rows == 0 && res$num_cols == 0)   # TODO: If logical operator, need to indicate
    return(res$scalar)
  new("H2OParsedData", h2o=x@h2o, key=res$dest_key)
})

# TODO: H2O doesn't support any arguments beyond the single H2OParsedData object (with <= 2 cols)
table.internal <- table
table <- function(..., exclude = if (useNA == "no") c(NA, NaN), useNA = c("no", "ifany", "always"), dnn = list.names(...), deparse.level = 1) {
  idx = sapply(c(...), function(x) { class(x) == "H2OParsedData" })
  if(any(idx) && !all(idx))
    stop("Can't mix H2OParsedData objects with R objects in table")
  else if(any(idx)) {
    myData = c(...)
    if(length(myData) > 1 || ncol(myData[[1]]) > 2) stop("Unimplemented")
    h2o.__unop2("table", myData[[1]]) 
  } else {
    list.names <- function(...) {
      l <- as.list(substitute(list(...)))[-1L]
      nm <- names(l)
      fixup <- if (is.null(nm)) 
        seq_along(l)
      else nm == ""
      dep <- vapply(l[fixup], function(x) switch(deparse.level + 
                                                   1, "", if (is.symbol(x)) as.character(x) else "", 
                                                 deparse(x, nlines = 1)[1L]), "")
      if (is.null(nm)) 
        dep
      else {
        nm[fixup] <- dep
        nm
      }
    }
    if (!missing(exclude) && is.null(exclude)) 
      useNA <- "always"
    useNA <- match.arg(useNA)
    args <- list(...)
    if (!length(args)) 
      stop("nothing to tabulate")
    if (length(args) == 1L && is.list(args[[1L]])) {
      args <- args[[1L]]
      if (length(dnn) != length(args)) 
        dnn <- if (!is.null(argn <- names(args))) 
          argn
      else paste(dnn[1L], seq_along(args), sep = ".")
    }
    bin <- 0L
    lens <- NULL
    dims <- integer()
    pd <- 1L
    dn <- NULL
    for (a in args) {
      if (is.null(lens)) 
        lens <- length(a)
      else if (length(a) != lens) 
        stop("all arguments must have the same length")
      cat <- if (is.factor(a)) {
        if (any(is.na(levels(a)))) 
          a
        else {
          if (is.null(exclude) && useNA != "no") 
            addNA(a, ifany = (useNA == "ifany"))
          else {
            if (useNA != "no") 
              a <- addNA(a, ifany = (useNA == "ifany"))
            ll <- levels(a)
            a <- factor(a, levels = ll[!(ll %in% exclude)], 
                        exclude = if (useNA == "no") 
                          NA)
          }
        }
      }
      else {
        a <- factor(a, exclude = exclude)
        if (useNA != "no") 
          addNA(a, ifany = (useNA == "ifany"))
        else a
      }
      nl <- length(ll <- levels(cat))
      dims <- c(dims, nl)
      if (prod(dims) > .Machine$integer.max) 
        stop("attempt to make a table with >= 2^31 elements")
      dn <- c(dn, list(ll))
      bin <- bin + pd * (as.integer(cat) - 1L)
      pd <- pd * nl
    }
    names(dn) <- dnn
    bin <- bin[!is.na(bin)]
    if (length(bin)) 
      bin <- bin + 1L
    y <- array(tabulate(bin, pd), dims, dimnames = dn)
    class(y) <- "table"
    y
  }   
}

h2o.runif <- function(x) { h2o.__unop2("runif", x) }

setMethod("colnames", "H2OParsedData", function(x) {
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT2, src_key=x@key)
  unlist(lapply(res$cols, function(y) y$name))
})
setMethod("colnames<-", "H2OParsedData", function(x, value) { stop("Unimplemented") })

setMethod("names", "H2OParsedData", function(x) { colnames(x) })
setMethod("names<-", "H2OParsedData", function(x, value) { names(x) <- value })
# setMethod("nrow", "H2OParsedData", function(x) { h2o.__unop2("nrow", x) })
# setMethod("ncol", "H2OParsedData", function(x) { h2o.__unop2("ncol", x) })

setMethod("nrow", "H2OParsedData", function(x) {
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT2, src_key=x@key); as.numeric(res$numRows) })

setMethod("ncol", "H2OParsedData", function(x) {
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT2, src_key=x@key); as.numeric(res$numCols) })

# setMethod("min", "H2OParsedData", function(x, ..., na.rm = FALSE) {
#   if(na.rm) stop("Unimplemented")
#   # res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT2, src_key=x@key)
#   # min(..., sapply(res$cols, function(x) { x$min }), na.rm)
#   min(..., h2o.__unop2("min", x), na.rm)
# })
#
# setMethod("max", "H2OParsedData", function(x, ..., na.rm = FALSE) {
#   if(na.rm) stop("Unimplemented")
#   # res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT2, src_key=x@key)
#   # max(..., sapply(res$cols, function(x) { x$max }), na.rm)
#   max(..., h2o.__unop2("max", x), na.rm)
# })

min.internal <- min
min <- function(..., na.rm = FALSE) {
  idx = sapply(c(...), function(y) { class(y) == "H2OParsedData" })

  if(any(idx)) {
    hex.op = ifelse(na.rm, "min.na.rm", "min")
    myVals = c(...); myData = myVals[idx]
    myKeys = sapply(myData, function(y) { y@key })
    expr = paste(hex.op, "(", paste(myKeys, collapse=","), ")", sep = "")
    res = h2o.__exec2(myData[[1]]@h2o, expr)
    .Primitive("min")(unlist(myVals[!idx]), res$scalar, na.rm = na.rm)
  } else
    .Primitive("min")(..., na.rm = na.rm)
}

max.internal <- max
max <- function(..., na.rm = FALSE) {
  idx = sapply(c(...), function(y) { class(y) == "H2OParsedData" })

  if(any(idx)) {
    hex.op = ifelse(na.rm, "max.na.rm", "max")
    myVals = c(...); myData = myVals[idx]
    myKeys = sapply(myData, function(y) { y@key })
    expr = paste(hex.op, "(", paste(myKeys, collapse=","), ")", sep = "")
    res = h2o.__exec2(myData[[1]]@h2o, expr)
    .Primitive("max")(unlist(myVals[!idx]), res$scalar, na.rm = na.rm)
  } else
    .Primitive("max")(..., na.rm = na.rm)
}

sum.internal <- sum
sum <- function(..., na.rm = FALSE) {
  idx = sapply(c(...), function(y) { class(y) == "H2OParsedData" })
  
  if(any(idx)) {
    hex.op = ifelse(na.rm, "sum.na.rm", "sum")
    myVals = c(...); myData = myVals[idx]
    myKeys = sapply(myData, function(y) { y@key })
    expr = paste(hex.op, "(", paste(myKeys, collapse=","), ")", sep = "")
    res = h2o.__exec2(myData[[1]]@h2o, expr)
    .Primitive("sum")(unlist(myVals[!idx]), res$scalar, na.rm = na.rm)
  } else
    .Primitive("sum")(..., na.rm = na.rm)
}

setMethod("range", "H2OParsedData", function(x) {
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT2, src_key=x@key)
  temp = sapply(res$cols, function(x) { c(x$min, x$max) })
  c(min(temp[1,]), max(temp[2,]))
})

setMethod("colMeans", "H2OParsedData", function(x) {
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT2, src_key=x@key)
  temp = sapply(res$cols, function(x) { x$mean })
  names(temp) = sapply(res$cols, function(x) { x$name })
  temp
})

mean.H2OParsedData <- function(x, trim = 0, na.rm = FALSE, ...) {
  if(length(x) != 1 || trim != 0) stop("Unimplemented")
  if(any.factor(x) || dim(x)[2] != 1) {
    warning("argument is not numeric or logical: returning NA")
    return(NA_real_)
  }
  if(!na.rm && h2o.__unop2("any.na", x)) return(NA)
  h2o.__unop2("mean", x)
}

setMethod("sd", "H2OParsedData", function(x, na.rm = FALSE) {
  if(length(x) != 1) stop("Unimplemented")
  if(dim(x)[2] != 1 || any.factor(x)) stop("Could not coerce argument to double. H2O sd requires a single numeric column.")
  if(!na.rm && h2o.__unop2("any.na", x)) return(NA)
  h2o.__unop2("sd", x)
})

setMethod("dim", "H2OParsedData", function(x) {
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT2, src_key=x@key)
  as.numeric(c(res$numRows, res$numCols))
})
setMethod("dim<-", "H2OParsedData", function(x, value) { stop("Unimplemented") })

setMethod("as.data.frame", "H2OParsedData", function(x) {
  url <- paste('http://', x@h2o@ip, ':', x@h2o@port, '/2/DownloadDataset?src_key=', x@key, sep='')
  ttt <- getURL(url)
  n = nchar(ttt)

  # Delete last 1 or 2 characters if it's a newline.
  # Handle \r\n (for windows) or just \n (for not windows).
  chars_to_trim = 0
  if (n >= 2) {
      c = substr(ttt, n, n)
      if (c == "\n") {
          chars_to_trim = chars_to_trim + 1
      }
      if (chars_to_trim > 0) {
          c = substr(ttt, n-1, n-1)
          if (c == "\r") {
              chars_to_trim = chars_to_trim + 1
          }
      }    
  }

  if (chars_to_trim > 0) {
      ttt2 = substr(ttt, 1, n-chars_to_trim)
      # Is this going to use an extra copy?  Or should we assign directly to ttt?
      ttt = ttt2
  }
  
  # Substitute NAs for blank cells rather than skipping.
  df = read.csv(textConnection(ttt), blank.lines.skip = FALSE)
  return(df)
})

setMethod("head", "H2OParsedData", function(x, n = 6L, ...) {
  numRows = nrow(x)
  stopifnot(length(n) == 1L)
  n <- ifelse(n < 0L, max(numRows + n, 0L), min(n, numRows))
  if(n == 0) return(data.frame())
  
  x.slice = as.data.frame(x[seq_len(n),])
  res = h2o.__remoteSend(x@h2o, h2o.__HACK_LEVELS, source = x@key)
  for(i in 1:ncol(x)) {
    if(!is.null(res$levels[[i]]))
      x.slice[,i] <- factor(x.slice[,i], levels = res$levels[[i]])
  }
  return(x.slice)
})

setMethod("tail", "H2OParsedData", function(x, n = 6L, ...) {
  stopifnot(length(n) == 1L)
  nrx <- nrow(x)
  n <- ifelse(n < 0L, max(nrx + n, 0L), min(n, nrx))
  if(n == 0) return(data.frame())
  
  idx = seq.int(to = nrx, length.out = n)
  x.slice = as.data.frame(x[idx,])
  rownames(x.slice) = idx
  res = h2o.__remoteSend(x@h2o, h2o.__HACK_LEVELS, source = x@key)
  for(i in 1:ncol(x)) {
    if(!is.null(res$levels[[i]]))
      x.slice[,i] <- factor(x.slice[,i], levels = res$levels[[i]])
  }
  return(x.slice)
})

setMethod("as.factor", "H2OParsedData", function(x) { h2o.__unop2("factor", x) })
setMethod("is.factor", "H2OParsedData", function(x) { as.logical(h2o.__unop2("is.factor", x)) })

any.factor <- function(x) {
  if(class(x) != "H2OParsedData")
    stop("x must be an H2OParsedData object")
  as.logical(h2o.__unop2("any.factor", x))
}

setMethod("quantile", "H2OParsedData", function(x, probs = c(0.01, 0.05, 0.1, 0.25, 0.33, 0.5, 0.66, 0.75, 0.9, 0.95, 0.99), na.rm = FALSE, names = TRUE) {
  if(any.factor(x)) stop("factors are not allowed")
  if(na.rm) stop("Unimplemented")
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_SUMMARY2, source=x@key)
  temp = sapply(res$summaries, function(x) { x$stats$pctile })
  # filt = !sapply(temp, is.null)
  # temp = temp[filt]
  if(length(temp) == 0) return(NULL)

  # myFeat = res$names[filt[1:length(res$names)]]
  # myQuantiles = c(1, 5, 10, 25, 33, 50, 66, 75, 90, 95, 99)
  myFeat = sapply(res$summaries, function(x) { x$colname })
  myQuantiles = res$summaries[[1]]$stats$pct
  if(any(!probs %in% myQuantiles)) stop("Only the following quantiles are supported: ", paste(myQuantiles, collapse=", "))
  temp2 = matrix(unlist(temp), ncol = length(myFeat), dimnames = list(paste(100*myQuantiles, "%", sep=""), myFeat))
  temp2[match(probs, myQuantiles),]
})

setGeneric("histograms", function(object) { standardGeneric("histograms") })
setMethod("histograms", "H2OParsedData", function(object) {
  res = h2o.__remoteSend(object@h2o, h2o.__PAGE_SUMMARY2, source=object@key)
  list.of.bins <- lapply(res$summaries, function(x) {
    if (x$stats$type == 'Enum') {
      bins <- NULL
    } else {
      counts <- x$hcnt
      breaks <- seq(x$hstart, by=x$hstep, length.out=length(x$hcnt) + 1)
      bins <- list(counts,breaks)
      names(bins) <- cbind('counts', 'breaks')
    }
    bins
  })
  return(list.of.bins)
})

setMethod("summary", "H2OParsedData", function(object) {
  digits = 12L
  res = h2o.__remoteSend(object@h2o, h2o.__PAGE_SUMMARY2, source=object@key)
  cols <- sapply(res$summaries, function(col) {
    if(col$stats$type != 'Enum') { # numeric column
      if(is.null(col$stats$mins) || length(col$stats$mins) == 0) col$stats$mins = NaN
      if(is.null(col$stats$maxs) || length(col$stats$maxs) == 0) col$stats$maxs = NaN
      if(is.null(col$stats$pctile))
        params = format(rep(signif(as.numeric(col$stats$mean), digits), 6), digits = 4)
      else
        params = format(signif(as.numeric(c(
          col$stats$mins[1],
          col$stats$pctile[4],
          col$stats$pctile[6],
          col$stats$mean,
          col$stats$pctile[8],
          col$stats$maxs[1])), digits), digits = 4)
      result = c(paste("Min.   :", params[1], "  ", sep=""), paste("1st Qu.:", params[2], "  ", sep=""),
                 paste("Median :", params[3], "  ", sep=""), paste("Mean   :", params[4], "  ", sep=""),
                 paste("3rd Qu.:", params[5], "  ", sep=""), paste("Max.   :", params[6], "  ", sep=""))
    }
    else {
      top.ix <- sort.int(col$hcnt, decreasing=T, index.return=T)$ix[1:6]
      # domains <- col$hbrk[top.ix]
      if(is.null(col$hbrk)) domains <- top.ix[1:6] else domains <- col$hbrk[top.ix]
      counts <- col$hcnt[top.ix]
      # width <- max(cbind(nchar(domains), nchar(counts)))
      width <- max(nchar(domains) + nchar(counts))
      result <- paste(domains,
                      mapply(function(x, y) { paste(rep(' ',width - nchar(x) - nchar(y)), collapse='') }, domains, counts),
                      ":",
                      counts,
                      " ",
                      sep='')
      result[is.na(top.ix)] <- NA
      result
    }
  })

  result = as.table(cols)
  rownames(result) <- rep("", 6)
  colnames(result) <- sapply(res$summaries, function(col) col$colname)
  result
})

setMethod("ifelse", "H2OParsedData", function(test, yes, no) {
  if(!(is.numeric(yes) || class(yes) == "H2OParsedData") || !(is.numeric(no) || class(no) == "H2OParsedData"))
    stop("Unimplemented")
  if(!test@logic) stop(test@key, " is not a H2O logical data type")
  yes = ifelse(class(yes) == "H2OParsedData", yes@key, yes)
  no = ifelse(class(no) == "H2OParsedData", no@key, no)
  expr = paste("ifelse(", test@key, ",", yes, ",", no, ")", sep="")
  res = h2o.__exec2(test@h2o, expr)
  if(res$num_rows == 0 && res$num_cols == 0)   # TODO: If logical operator, need to indicate
    res$scalar
  else
    new("H2OParsedData", h2o=test@h2o, key=res$dest_key, logic=FALSE)
})

setMethod("levels", "H2OParsedData", function(x) {
  if(ncol(x) != 1) return(NULL)
  res = h2o.__remoteSend(x@h2o, h2o.__HACK_LEVELS, source = x@key)
  res$levels[[1]]
})

#----------------------------- Work in Progress -------------------------------#
# TODO: Substitute in key names for H2OParsedData variables
setMethod("apply", "H2OParsedData", function(X, MARGIN, FUN, ...) {
  myfun = deparse(substitute(FUN))
  len = length(myfun)

  if(len > 2 && myfun[len] == "}")
    myfun = paste(myfun[1], paste(myfun[2:(len-1)], collapse = ";"), myfun[len])
  else
    myfun = paste(myfun, collapse = "")
  params = c(X@key, MARGIN, myfun)
  expr = paste("apply(", paste(params, collapse = ","), ")", sep="")
  res = h2o.__exec2(X@h2o, expr)
  new("H2OParsedData", h2o=X@h2o, key=res$dest_key)
})

str.H2OParsedData <- function(object, ...) {
  if (length(l <- list(...)) && any("give.length" == names(l))) 
    invisible(NextMethod("str", ...))
  else invisible(NextMethod("str", give.length = FALSE, ...))
  
  if(class(object) != "H2OParsedData")
    stop("object must be of class H2OParsedData")
  res = h2o.__remoteSend(object@h2o, h2o.__PAGE_INSPECT, key = object@key)
  cat("\nH2O dataset '", object@key, "':\t", res$num_rows, " obs. of  ", (p <- res$num_cols), 
      " variable", if(p != 1) "s", if(p > 0) ":", "\n", sep = "")
  
  cc <- unlist(lapply(res$cols, function(y) y$name))
  width <- max(nchar(cc))
  rows <- res$rows[1:min(res$num_rows, 10)]    # TODO: Might need to check rows > 0
  res2 = h2o.__remoteSend(object@h2o, h2o.__HACK_LEVELS, source = object@key)
  for(i in 1:p) {
    cat("$ ", cc[i], rep(' ', width - nchar(cc[i])), ": ", sep = "")
    rhead <- sapply(rows, function(x) { x[i+1] })
    if(is.null(res2$levels[[i]]))
      cat("num  ", paste(rhead, collapse = " "), if(res$num_rows > 10) " ...", "\n", sep = "")
    else {
      rlevels = res2$levels[[i]]
      cat("Factor w/ ", (count <- length(rlevels)), " level", if(count != 1) "s", ' "', paste(rlevels[1:min(count, 2)], collapse = '","'), '"', if(count > 2) ",..", ": ", sep = "")
      cat(paste(match(rhead, rlevels), collapse = " "), if(res$num_rows > 10) " ...", "\n", sep = "")
    }
  }
}

str.H2OParsedDataVA <- function(object, ...) {
  str(new("H2OParsedData", h2o=object@h2o, key=object@key), ...)
}

#--------------------------------- ValueArray ----------------------------------#
setMethod("show", "H2ORawDataVA", function(object) {
  print(object@h2o)
  cat("Raw Data Key:", object@key, "\n")
})

setMethod("show", "H2OParsedDataVA", function(object) {
  print(object@h2o)
  cat("Parsed Data Key:", object@key, "\n")
})

setMethod("show", "H2OGLMModelVA", function(object) {
  print(object@data)
  cat("GLM Model Key:", object@key, "\n\n")
  
  model = object@model
  cat("Coefficients:\n"); print(round(model$coefficients,5))
  cat("\nNormalized Coefficients:\n"); print(round(model$normalized_coefficients,5))
  cat("\nDegrees of Freedom:", model$df.null, "Total (i.e. Null); ", model$df.residual, "Residual\n")
  cat("Null Deviance:    ", round(model$null.deviance,1), "\n")
  cat("Residual Deviance:", round(model$deviance,1), " AIC:", round(model$aic,1), "\n")
  cat("Avg Training Error Rate:", round(model$train.err,5), "\n")

  # if(model$family == "binomial") {
  if(model$family$family == "binomial") {
    cat("AUC:", round(model$auc,5), " Best Threshold:", round(model$threshold,5), "\n")
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

setMethod("show", "H2OGLMGridVA", function(object) {
  print(object@data)
  cat("GLMGrid Model Key:", object@key, "\n")

  temp = data.frame(t(sapply(object@sumtable, c)))
  cat("\nSummary\n"); print(temp)
})

setMethod("show", "H2ORFModelVA", function(object) {
  print(object@data)
  cat("Random Forest Model Key:", object@key)

  model = object@model
  cat("\n\nClassification Error:", model$classification_error)
  cat("\nConfusion Matrix:\n"); print(model$confusion)
  cat("\nTree Stats:\n"); print(model$tree_sum)
})

setMethod("colnames", "H2OParsedDataVA", function(x) {
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT, key=x@key)
  unlist(lapply(res$cols, function(y) y$name))
})

setMethod("colnames<-", signature(x="H2OParsedDataVA", value="H2OParsedDataVA"), 
  function(x, value) { h2o.__remoteSend(x@h2o, h2o.__PAGE_COLNAMES, target=x@key, source=value@key); return(x) })

setMethod("colnames<-", signature(x="H2OParsedDataVA", value="character"),
  function(x, value) {
    if(length(value) != ncol(x)) stop("Mismatched column dimensions!")
    stop("Unimplemented"); return(x)
  })

setMethod("names", "H2OParsedDataVA", function(x) { colnames(x) })

setMethod("nrow", "H2OParsedDataVA", function(x) {
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT, key=x@key); as.numeric(res$num_rows) })

setMethod("ncol", "H2OParsedDataVA", function(x) {
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT, key=x@key); as.numeric(res$num_cols) })

setMethod("dim", "H2OParsedDataVA", function(x) {
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT, key=x@key)
  as.numeric(c(res$num_rows, res$num_cols))
})

setMethod("length", "H2OParsedData", function(x) { ncol(x) })

setMethod("head", "H2OParsedDataVA", function(x, n = 6L, ...) {
  numRows = nrow(x)
  stopifnot(length(n) == 1L)
  n <- ifelse(n < 0L, max(numRows + n, 0L), min(n, numRows))
  if(n == 0) return(data.frame())
  if(n > MAX_INSPECT_VIEW) stop(paste("Cannot view more than", MAX_INSPECT_VIEW, "rows"))
  
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT, key=x@key, offset=0, view=n)
  temp = lapply(res$rows, function(y) { y$row = NULL; as.data.frame(y) })
  if(is.null(temp)) return(temp)
  x.slice = do.call(rbind, temp)

  res2 = h2o.__remoteSend(x@h2o, h2o.__HACK_LEVELS, source = x@key)
  for(i in 1:ncol(x)) {
    if(!is.null(res2$levels[[i]]))
      x.slice[,i] <- factor(x.slice[,i], levels = res2$levels[[i]])
  }
  return(x.slice)
})

setMethod("tail", "H2OParsedDataVA", function(x, n = 6L, ...) {
  stopifnot(length(n) == 1L)
  nrx <- nrow(x)
  n <- ifelse(n < 0L, max(nrx + n, 0L), min(n, nrx))
  if(n == 0) return(data.frame())
  if(n > MAX_INSPECT_VIEW) stop(paste("Cannot view more than", MAX_INSPECT_VIEW, "rows"))
  
  idx = seq.int(to = nrx, length.out = n)
  res = h2o.__remoteSend(x@h2o, h2o.__PAGE_INSPECT, key=x@key, offset=idx[1], view=length(idx))
  temp = lapply(res$rows, function(y) { y$row = NULL; as.data.frame(y) })
  if(is.null(temp)) return(temp)
  x.slice = do.call(rbind, temp)
  rownames(x.slice) = idx
  
  res2 = h2o.__remoteSend(x@h2o, h2o.__HACK_LEVELS, source = x@key)
  for(i in 1:ncol(x)) {
    if(!is.null(res2$levels[[i]]))
      x.slice[,i] <- factor(x.slice[,i], levels = res2$levels[[i]])
  }
  return(x.slice)
})

setMethod("summary", "H2OParsedDataVA", function(object) {
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
        params = format(rep(round(as.numeric(res[[i]]$mean), 3), 6))
      else
        params = format(round(as.numeric(c(res[[i]]$min[1], res[[i]]$percentiles$values[4], res[[i]]$percentiles$values[6], res[[i]]$mean, res[[i]]$percentiles$values[8], res[[i]]$max[1])), 3))
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
