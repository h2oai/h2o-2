#--------------------------------- Class Definitions ----------------------------------#
# WARNING: Do NOT touch the env slot! It is used to link garbage collection between R and H2O
setClass("H2OClient", representation(ip="character", port="numeric"), prototype(ip="127.0.0.1", port=54321))
setClass("H2ORawData", representation(h2o="H2OClient", key="character"))
# setClass("H2ORawData", representation(h2o="H2OClient", key="character", env="environment"))
setClass("H2OParsedData", representation(h2o="H2OClient", key="character", logic="logical", col_names="vector", nrows="numeric", ncols="numeric", any_enum="logical"),
          prototype(logic=FALSE, col_names="", ncols=-1, nrows=-1, any_enum = FALSE))
# setClass("H2OParsedData", representation(h2o="H2OClient", key="character", env="environment", logic="logical"), prototype(logic=FALSE))
setClass("H2OModel", representation(key="character", data="H2OParsedData", model="list", "VIRTUAL"))
# setClass("H2OModel", representation(key="character", data="H2OParsedData", model="list", env="environment", "VIRTUAL"))
setClass("H2OGrid", representation(key="character", data="H2OParsedData", model="list", sumtable="list", "VIRTUAL"))
setClassUnion("data.frameORnull", c("data.frame", "NULL"))
setClass("H2OPerfModel", representation(cutoffs="numeric", measure="numeric", perf="character", model="list", roc="data.frame", gains="data.frameORnull"))

setClass("H2OGapStatModel", contains="H2OModel")
setClass("H2OCoxPHModel", contains="H2OModel", representation(summary="list", survfit="list"))
setClass("H2OGLMModel", contains="H2OModel", representation(xval="list"))
setClass("H2OKMeansModel", contains="H2OModel")
setClass("H2ODeepLearningModel", contains="H2OModel", representation(valid="H2OParsedData", xval="list"))
setClass("H2ODRFModel", contains="H2OModel", representation(valid="H2OParsedData", xval="list"))
setClass("H2ONBModel", contains="H2OModel")
setClass("H2OPCAModel", contains="H2OModel")
setClass("H2OGBMModel", contains="H2OModel", representation(valid="H2OParsedData", xval="list"))
setClass("H2OSpeeDRFModel", contains="H2OModel", representation(valid="H2OParsedData", xval="list"))

setClass("H2OGLMGrid", contains="H2OGrid")
setClass("H2OGBMGrid", contains="H2OGrid")
setClass("H2OKMeansGrid", contains="H2OGrid")
setClass("H2ODRFGrid", contains="H2OGrid")
setClass("H2ODeepLearningGrid", contains="H2OGrid")
setClass("H2OSpeeDRFGrid", contains="H2OGrid")
setClass("H2OCoxPHModelSummary", representation(summary="list"))
setClass("H2OGLMModelList", representation(models="list", best_model="numeric", lambdas="numeric"))

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
#   if(key != "") reg.finalizer(.Object@env, .h2o.__finalizer)
#   return(.Object)
# })
#
# setMethod("initialize", "H2OParsedData", function(.Object, h2o = new("H2OClient"), key = "") {
#   .Object@h2o = h2o
#   .Object@key = key
#   .Object@env = new.env()ASTSS
#
#   assign("h2o", .Object@h2o, envir = .Object@env)
#   assign("key", .Object@key, envir = .Object@env)
#
#   # Empty keys don't refer to any object in H2O
#   if(key != "") reg.finalizer(.Object@env, .h2o.__finalizer)
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
#   if(key != "") reg.finalizer(.Object@env, .h2o.__finalizer)
#   return(.Object)
# })

#--------------------------------- Class Display Functions ----------------------------------#
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
  cat("Parsed Data Key:", object@key, "\n\n")
  print(head(object))
})

setMethod("show", "H2OGrid", function(object) {
  print(object@data@h2o)
  cat("Parsed Data Key:", object@data@key, "\n\n")
  cat("Grid Search Model Key:", object@key, "\n")

  temp = data.frame(t(sapply(object@sumtable, c)))
  cat("\nSummary\n"); print(temp)
})


setMethod("show", "H2OGapStatModel", function(object) {
  cat("\n")
  cat("Number of KMeans Run: ", object@model$params$K*(object@model$params$B + 1), "\n")
  cat("Optimal Number of Clusters: ", object@model$k_opt, "\n")
  cat("\nFor more, try `summary` and `plot` methods.\n\n")
  cat("\n")
})

summary.H2OGapStatModel <-
function(object, ...) {
  x    <- 1:length(object@model$log_within_ss)
  lwk  <- object@model$log_within_ss
  elwk <- object@model$boot_within_ss
  sdev <- object@model$se_boot_within_ss
  gaps <- object@model$gap_stats

  row.names <- c("LWCSS", "E[LWCSS]", "sdevs", "gaps")

  fr <- matrix(ncol=length(x), nrow=4)
  fr[1,] <- lwk
  fr[2,] <- elwk
  fr[3,] <- sdev
  fr[4,] <- gaps
  fr <- as.data.frame(fr)
  rownames(fr) <- row.names
  colnames(fr) <- x

#  cat("\n")
#  cat("(LWCSS = Log of Within Cluster Sum of Squares)", "\n\n")
#  cat("LWCSS for each k (log(W_k)):\n", lwk, "\n\n")
#  cat("Expected LWCSS for each k (log(W*_k)):\n", elwk, "\n\n")
#  cat("Standard Errors Expected LWCSS for each k:\n", sdev, "\n\n")
#  cat("Gap Statistics:\n", gaps, "\n\n")
#  cat("\n")
  print(fr)

  cat("\nTry plotting the Gap Statistic Model:\n")
  cat("\nExample: plot(my.model)\n\n")

  invisible(return(fr))
}

plot.H2OGapStatModel<-
function(x, ...) {
  object <- x
  x    <- 1:length(object@model$log_within_ss)
  lwk  <- object@model$log_within_ss
  elwk <- object@model$boot_within_ss
  sdev <- object@model$se_boot_within_ss
  gaps <- object@model$gap_stats

  par(mfrow=c(3,1))
  plot(x, lwk, xlab="number of clusters k", ylab = "log(W_k)", type="o", pch=19)
  plot(x, lwk, xlab="number of clusters k", ylab = "obs and exp log(W_k)", pch = "O", type="o", ylim=c(0,max(lwk, elwk)))
  lines(x,elwk, pch="E", type="o")
  plot(x, gaps, ylim=c(min(gaps-sdev), max(gaps+sdev)), type="o", pch=19, xlab="number of clusters k", ylab="Gap")
  suppressWarnings(arrows(x, gaps-sdev, x, gaps+sdev, length=0.05, angle=90, code=3))  # suppress warning on case where sdev ~ 0
}

setMethod("show", "H2OCoxPHModel", function(object)
  get("print.coxph", getNamespace("survival"))(object@model))

setMethod("show", "H2OCoxPHModelSummary", function(object)
  get("print.summary.coxph", getNamespace("survival"))(object@summary))

print.survfit.H2OCoxPHModel <- function(x, ...)
  suppressWarnings(NextMethod("print"))

setMethod("summary","H2OCoxPHModel",
function(object, conf.int = 0.95, scale = 1, ...) {
  res <- new("H2OCoxPHModelSummary", summary = object@summary)
  if (conf.int == 0)
    res@summary$conf.int <- NULL
  else {
    z <- qnorm((1 + conf.int)/2, 0, 1)
    coef <- scale * res@summary$coefficients[,    "coef",  drop = TRUE]
    se   <- scale * res@summary$coefficients[, "se(coef)", drop = TRUE]
    shift <- z * se
    res@summary$conf.int <-
      structure(cbind(exp(coef), exp(- coef), exp(coef - shift), exp(coef + shift)),
                dimnames =
                list(rownames(res@summary$coefficients),
                     c("exp(coef)", "exp(-coef)",
                       sprintf("lower .%.0f", 100 * conf.int),
                       sprintf("upper .%.0f", 100 * conf.int))))
  }
  res
})

coef.H2OCoxPHModel        <- function(object, ...) object@model$coefficients
coef.H2OCoxPHModelSummary <- function(object, ...) object@summary$coefficients

extractAIC.H2OCoxPHModel <- function(fit, scale, k = 2, ...)
{
  fun <- get("extractAIC.coxph", getNamespace("stats"))
  if (missing(scale))
    fun(fit@model, k = k)
  else
    fun(fit@model, scale = scale, k = k)
}

logLik.H2OCoxPHModel <- function(object, ...)
  get("logLik.coxph", getNamespace("survival"))(object@model, ...)

survfit.H2OCoxPHModel <-
function(formula, newdata, conf.int = 0.95,
         conf.type = c("log", "log-log", "plain", "none"), ...) {
  if (missing(newdata))
    newdata <- as.data.frame(c(as.list(formula@model$means),
                               as.list(formula@model$means.offset)))
  if (is.data.frame(newdata))
    capture.output(newdata <- as.h2o(formula@data@h2o, newdata, header = TRUE))
  conf.type <- match.arg(conf.type)

  # Code below has calculation performed in R
  pred <- as.data.frame(h2o.predict(formula, newdata))[[1L]]
  res <- formula@survfit
  if (length(pred) == 1L)
    res$cumhaz <- pred * res$cumhaz
  else
    res$cumhaz <- outer(res$cumhaz, pred, FUN = "*")
  res$std.err <- NULL
  res$surv <- exp(- res$cumhaz)
  class(res) <- c("survfit.H2OCoxPHModel", "survfit.cox", "survfit")
  return(res)

  # Code below assumes calculation in H2O
  pred <- as.matrix(h2o.predict(formula, newdata)[,-1L])
  nms <- colnames(pred)
  dimnames(pred) <- NULL
  ch <- grep("^cumhaz_", nms)
  drop <- (nrow(pred) == 1L)

  res <- formula@survfit
  if (drop) {
    res$cumhaz  <- pred[,  ch, drop = TRUE]
    res$std.err <- pred[, -ch, drop = TRUE]
  } else {
    res$cumhaz  <- t(pred[,  ch, drop = FALSE])
    res$std.err <- t(pred[, -ch, drop = FALSE])
  }
  res$surv <- exp(- res$cumhaz)

  if (conf.type != "none")
    z <- qnorm(1 - (1 - conf.int)/2, 0, 1)
  switch (conf.type,
          "plain" = {
            shift <- z * res$std.err * res$surv
            upper <- res$surv + shift
            lower <- res$surv - shift
          },
          "log" = {
            center <- log(res$surv)
            shift  <- z * res$std.err
            upper  <- exp(center + shift)
            lower  <- exp(center - shift)
          },
          "log-log" = {
            center <- log(- log(res$surv))
            shift  <- z * res$std.err/log(res$surv)
            shift[is.nan(shift)] <- 0
            upper  <- exp(- exp(center + shift))
            lower  <- exp(- exp(center - shift))
          }
  )
  if (conf.type != "none") {
    res$upper <- pmin(pmax(upper, 0), 1)
    res$lower <- pmin(pmax(lower, 0), 1)
    res$conf.type <- conf.type
    res$conf.int  <- conf.int
  }

  class(res) <- c("survfit.H2OCoxPHModel", "survfit.cox", "survfit")
  res
}

vcov.H2OCoxPHModel <- function(object, ...)
  get("vcov.coxph", getNamespace("survival"))(object@model, ...)

setMethod("show", "H2OKMeansModel", function(object) {
    print(object@data@h2o)
    cat("Parsed Data Key:", object@data@key, "\n\n")
    cat("K-Means Model Key:", object@key)
    model = object@model
    cat("\n\nK-means clustering with", length(model$size), "clusters of sizes "); cat(model$size, sep=", ")
    cat("\n\nCluster means:\n"); print(model$centers)
    if (!is.null(model$cluster)) { cat("\nClustering vector:\n"); print(summary(model$cluster)) }
    cat("\nWithin cluster sum of squares by cluster:\n"); print(model$withinss)
#    cat("(between_SS / total_SS = ", round(100*sum(model$betweenss)/model$totss, 1), "%)\n")
    cat("\nAvailable components:\n\n"); print(names(model))
})

setMethod("show", "H2OGLMModel", function(object) {
    print(object@data@h2o)
    cat("Parsed Data Key:", object@data@key, "\n\n")
    cat("GLM2 Model Key:", object@key)
    
    model <- object@model
    cat("\n\nCoefficients:\n"); print(round(model$coefficients,5))
    if(!is.null(model$normalized_coefficients)) {
        cat("\nNormalized Coefficients:\n"); print(round(model$normalized_coefficients,5))
    }
    if( !(identical(model$df.null, numeric(0))) ) 
      cat("\nDegrees of Freedom:", model$df.null, "Total (i.e. Null); ", model$df.residual, "Residual")
    if(is.numeric(model$null.deviance)) cat("\nNull Deviance:    ", round(model$null.deviance,1))
    #Return AIC NaN while calculations for tweedie/gamma not implemented; keep R from throwing error
    if (class(model$aic) != "numeric") {
      if(is.numeric(model$deviance)) cat("\nResidual Deviance:", round(model$deviance,1), " AIC: NaN")
    } else {
      if(is.numeric(model$deviance)) cat("\nResidual Deviance:", round(model$deviance,1), " AIC:", round(model$aic,1))
    }
    if(is.numeric(model$null.deviance)) cat("\nDeviance Explained:", round(1-model$deviance/model$null.deviance,5), "\n")
    # cat("\nAvg Training Error Rate:", round(model$train.err,5), "\n")

    family <- model$params$family$family
    if(family == "binomial") {
        if(is.numeric(model$best_threshold)) cat(" Best Threshold:", round(model$best_threshold,5))
        if(is.matrix(model$confusion)) cat("\n\nConfusion Matrix:\n") ; if(is.matrix(model$confusion)) print(model$confusion)
        if (!is.null(model$auc)) {
            if(.hasSlot(object, "valid")) {
              trainOrValidation <- ifelse(is.na(object@valid@key), "train)", "validation)")
            } else {trainOrValidation <- "train)"
            if(is.numeric(model$auc)) cat("\nAUC = ", model$auc, "(on", trainOrValidation ,"\n")
            }
          }
    }
    
    if(length(object@xval) > 0) {
        cat("\nCross-Validation Models:\n")
        if(family == "binomial") {
            modelXval <- t(sapply(object@xval, function(x) { c(x@model$rank-1, x@model$auc, 1-x@model$deviance/x@model$null.deviance) }))
            colnames(modelXval) = c("Nonzeros", "AUC", "Deviance Explained")
        } else {
            modelXval <- t(sapply(object@xval, function(x) { c(x@model$rank-1, x@model$aic, 1-x@model$deviance/x@model$null.deviance) }))
            colnames(modelXval) = c("Nonzeros", "AIC", "Deviance Explained")
        }
        rownames(modelXval) <- paste("Model", 1:nrow(modelXval))
        print(modelXval)
    }
})

setMethod("summary","H2OGLMModelList", function(object) {
    summary <- NULL
    if(object@models[[1]]@model$params$family$family == 'binomial'){
        for(m in object@models) {
            model = m@model
            if(is.null(summary)) {
                summary = t(as.matrix(c(model$lambda, model$df.null-model$df.residual,round((1-model$deviance/model$null.deviance),2),round(model$auc,2))))
            } else {
                summary = rbind(summary,c(model$lambda,model$df.null-model$df.residual,round((1-model$deviance/model$null.deviance),2),round(model$auc,2)))
            }
        }
        summary = cbind(1:nrow(summary),summary)
        colnames(summary) <- c("id","lambda","predictors","dev.ratio"," AUC ")
    } else {
        for(m in object@models) {
            model = m@model
            if(is.null(summary)) {
                summary = t(as.matrix(c(model$lambda, model$df.null-model$df.residual,round((1-model$deviance/model$null.deviance),2))))
            } else {
                summary = rbind(summary,c(model$lambda,model$df.null-model$df.residual,round((1-model$deviance/model$null.deviance),2)))
            }
        }
        summary = cbind(1:nrow(summary),summary)
        colnames(summary) <- c("id","lambda","predictors","explained dev")
    }    
    summary
})

setMethod("show", "H2OGLMModelList", function(object) {
    print(summary(object))
    cat("best model:",object@best_model, "\n")
})

setMethod("show", "H2ODeepLearningModel", function(object) {
  print(object@data@h2o)
  cat("Parsed Data Key:", object@data@key, "\n\n")
  cat("Deep Learning Model Key:", object@key)

  model = object@model
  if (model$params$classification == 1) {
    cat("\n\nTraining classification error:", model$train_class_error)
    if (!is.null(model$valid_class_error)) cat("\n\nValidation classification error:", model$valid_class_error)
  } else {
    cat("\nTraining mean square error:", model$train_sqr_error)
    if (!is.null(model$valid_sqr_error)) cat("\nValidation mean square error:", model$valid_sqr_error)
  }
  
  if(!is.null(model$confusion)) {
    cat("\n\nConfusion matrix:\n")
    if(is.na(object@valid@key)) {
      if(model$params$nfolds == 0)
        cat("Reported on", object@data@key, "\n")
      else
        if (!is.null(object@model$params$nfolds) && object@model$params$nfolds >= 2)
          cat("Reported on", paste(model$params$nfolds, "-fold cross-validated data", sep = ""), "\n")
    } else
      cat("Reported on", object@valid@key, "\n")
    print(model$confusion)
  }
  
  if(!is.null(model$hit_ratios)) {
    cat("\nHit Ratios for Multi-class Classification:\n")
    print(model$hit_ratios)
  }
  if(!is.null(model$varimp)) {
    cat("\nRelative Variable Importance:\n"); print(model$varimp)
  }
  
  if(!is.null(object@xval) && length(object@xval) > 0) {
    cat("\nCross-Validation Models:\n")
    temp = lapply(object@xval, function(x) { cat(" ", x@key, "\n") })
  }

  if (!is.null(model$train_auc) && is.null(model$auc)) {
    trainOrValidation <- "train)"
    cat("\nAUC = ", model$train_auc, "(on", trainOrValidation, "\n")
  }

  if (!is.null(model$auc)) {
      trainOrValidation <- ifelse(is.na(object@valid@key), "train)", "validation)")
      cat("\nAUC = ", model$auc, "(on", trainOrValidation ,"\n")
  }
})

setMethod("show", "H2ODRFModel", function(object) {
  print(object@data@h2o)
  cat("Parsed Data Key:", object@data@key, "\n\n")
  cat("Distributed Random Forest Model Key:", object@key)

  model = object@model
  cat("\n\nClassification:", model$params$classification)
  cat("\nNumber of trees:", model$params$ntree)
  cat("\nTree statistics:\n"); print(model$forest)
  
  if(model$params$classification) {
    cat("\nConfusion matrix:\n")
    if(is.na(object@valid@key))
      if (!is.null(object@model$params$nfolds) && object@model$params$nfolds >= 2)
        cat("Reported on", paste(object@model$params$nfolds, "-fold cross-validated data", sep = ""), "\n")
      else cat("Reported on training data.\n")
    else
      cat("Reported on", object@valid@key, "\n")
    print(model$confusion)
    
    if(!is.null(model$gini))
      cat("\nGini:", model$gini, "\n")
  }
  if (!is.null(model$auc)) {
    trainOrValidation <- ifelse(is.na(object@valid@key), "train) (OOBEE)", "validation)")
    cat("\nAUC = ", model$auc, "(on", trainOrValidation ,"\n")
  }
  if(!is.null(model$varimp)) {
    cat("\nVariable importance:\n"); print(model$varimp)
  }
  cat("\nOverall Mean-squared Error: ", model$err[[length(model$err)]], "\n")
  if(length(object@xval) > 0) {
    cat("\nCross-Validation Models:\n")
    print(sapply(object@xval, function(x) x@key))
  }
})

setMethod("show", "H2OSpeeDRFModel", function(object) {
  print(object@data@h2o)
  cat("Parsed Data Key:", object@data@key, "\n\n")
  cat("Random Forest Model Key:", object@key)
  cat("\n\nSeed Used: ", format(object@model$params$seed, digits = 20))

  model = object@model
  cat("\n\nClassification:", model$params$classification)
  cat("\nNumber of trees:", model$params$ntree)

  cat("\nConfusion matrix:\n");
  if(is.na(object@valid@key)) {
    if (!is.null(object@model$params$nfolds) && object@model$params$nfolds >= 2)
      cat("Reported on", paste(object@model$params$nfolds, "-fold cross-validated data", sep = ""), "\n")
    else cat("Reported on training data.")
  } else cat("Reported on", object@valid@key, "\n")
  print(model$confusion)
 
  if(!is.null(model$varimp)) {
    cat("\nVariable importance:\n"); print(model$varimp)
  }

  #mse <-model$mse[length(model$mse)] # (model$mse[is.na(model$mse) | model$mse <= 0] <- "")

  if (model$mse != -1) {
    cat("\nMean-squared Error from the",model$params$ntree, "trees: "); cat(model$mse, "\n")
  }

  if(length(object@xval) > 0) {
    cat("\nCross-Validation Models:\n")
    print(sapply(object@xval, function(x) x@key))
  }

  if (!is.null(model$auc)) {
    trainOrValidation <- ifelse(is.na(object@valid@key), "train)", "validation)")
    oobee_or_not <- ifelse( (is.na(object@valid@key) && object@model$params$oobee), "(OOBEE)", "")
    cat("\nAUC = ", model$auc, "(on", trainOrValidation , oobee_or_not,"\n")
  }

})

setMethod("show", "H2OPCAModel", function(object) {
  print(object@data@h2o)
  cat("Parsed Data Key:", object@data@key, "\n\n")
  cat("PCA Model Key:", object@key)

  model = object@model
  cat("\n\nStandard deviations:\n", model$sdev)
  cat("\n\nRotation:\n"); print(model$rotation)
})

setMethod("show", "H2ONBModel", function(object) {
  print(object@data@h2o)
  cat("Parsed Data Key:", object@data@key, "\n\n")
  cat("Naive Bayes Model Key:", object@key)
  
  model = object@model
  cat("\n\nA-priori probabilities:\n"); print(model$apriori_prob)
  cat("\n\nConditional probabilities:\n"); print(model$tables)
})

setMethod("show", "H2OGBMModel", function(object) {
  print(object@data@h2o)
  cat("Parsed Data Key:", object@data@key, "\n\n")
  cat("GBM Model Key:", object@key, "\n")

  model = object@model
  if(model$params$distribution %in% c("multinomial", "bernoulli")) {
    cat("\nConfusion matrix:\n")
    if(is.na(object@valid@key))
      cat("Reported on", paste(object@model$params$nfolds, "-fold cross-validated data", sep = ""), "\n")
    else
      cat("Reported on", object@valid@key, "\n")
    print(model$confusion)
    
    if(!is.null(model$gini))
      cat("\nGini:", model$gini, "\n")

    if (!is.null(model$auc)) {
      trainOrValidation <- ifelse(is.na(object@valid@key), "train)", "validation)")
      cat("\nAUC = ", model$auc, "(on", trainOrValidation ,"\n")
    }
  }
  
  if(!is.null(model$varimp)) {
    cat("\nVariable importance:\n"); print(model$varimp)
  }
  cat("\nOverall Mean-squared Error: ", model$err[[length(model$err)]], "\n")
  if(length(object@xval) > 0) {
    cat("\nCross-Validation Models:\n")
    print(sapply(object@xval, function(x) x@key))
  }
})

setMethod("show", "H2OPerfModel", function(object) {
  model = object@model
  tmp = t(data.frame(model[-length(model)]))
  
  if(object@perf == "mcc")
    criterion = "MCC"
  else
    criterion = paste(toupper(substring(object@perf, 1, 1)), substring(object@perf, 2), sep = "")
  rownames(tmp) = c("AUC", "Gini", paste("Best Cutoff for", criterion), "F1", "F2", "Accuracy", "Error", "Precision", "Recall", "Specificity", "MCC", "Max per Class Error")
  colnames(tmp) = "Value"; print(tmp)
  cat("\n\nConfusion matrix:\n"); print(model$confusion)
})

#--------------------------------- Unique H2O Methods ----------------------------------#
# TODO: s4 year, month impls as well?
h2o.year <- function(x) {
  if(missing(x)) stop('must specify x')
  if(class(x) != 'H2OParsedData' ) stop('x must be an H2OParsedData object')
  res1 <- .h2o.__unop2('year', x)
  .h2o.__binop2("-", res1, 1900)
}

h2o.month <- function(x) {
  if(missing(x)) stop('must specify x')
  if(class(x) != 'H2OParsedData') stop('x must be an H2OParsedData object')
  .h2o.__unop2('month', x)
}

year <- function(x) UseMethod('year', x)
year.H2OParsedData <- h2o.year
month <- function(x) UseMethod('month', x)
month.H2OParsedData <- h2o.month

as.Date.H2OParsedData <- function(x, format, ...) {
  if(!is.character(format)) stop("format must be a string")

  expr = paste("as.Date(", paste(x@key, deparse(eval(format, envir = parent.frame())), sep = ","), ")", sep = "")
  res = .h2o.__exec2(x@h2o, expr)
  res <- .h2o.exec2(res$dest_key, h2o = x@h2o, res$dest_key)
  res@logic <- FALSE
  return(res)
}

h2o.setTimezone <- function(client, tz) {
  if(class(client) != "H2OClient") stop("client must be a H2OClient object")
  if (!is.character(tz)) stop('tz must be a string')

  res = .h2o.__remoteSend(client, .h2o.__PAGE_SETTIMEZONE, tz = tz)
  res$tz
}

h2o.getTimezone <- function(client) {
  if(class(client) != "H2OClient") stop("client must be a H2OClient object")

  res = .h2o.__remoteSend(client, .h2o.__PAGE_GETTIMEZONE)
  res$tz
}

h2o.listTimezones <- function(client) {
  if(class(client) != "H2OClient") stop("client must be a H2OClient object")

  res = .h2o.__remoteSend(client, .h2o.__PAGE_LISTTIMEZONES)
  cat(res$tzlist)
}

diff.H2OParsedData <- function(x, lag = 1, differences = 1, ...) {
  if(!is.numeric(lag)) stop("lag must be numeric")
  if(!is.numeric(differences)) stop("differences must be numeric")
  
  expr = paste("diff(", paste(x@key, lag, differences, sep = ","), ")", sep = "")
  res = .h2o.__exec2(x@h2o, expr)
  res <- .h2o.exec2(res$dest_key, h2o = x@h2o, res$dest_key)
  res@logic <- FALSE
}

as.h2o <- function(client, object, key = "", header, sep = "") {
  if(missing(client) || class(client) != "H2OClient") stop("client must be a H2OClient object")
  if(missing(object) || !is.numeric(object) && !is.data.frame(object)) stop("object must be numeric or a data frame")
  if(!is.character(key)) stop("key must be of class character")
  if(missing(key) || nchar(key) == 0) {
    key = paste(.TEMP_KEY, ".", .pkg.env$temp_count, sep="")
    .pkg.env$temp_count = (.pkg.env$temp_count + 1) %% .pkg.env$RESULT_MAX
  }
  
  # TODO: Be careful, there might be a limit on how long a vector you can define in console
  if(is.numeric(object) && is.vector(object)) {
    res <- .h2o.__exec2_dest_key(client, paste("c(", paste(object, sep=',', collapse=","), ")", collapse=""), key)
    return(.h2o.exec2(res$dest_key, h2o = client, res$dest_key))
  } else {
    tmpf <- tempfile(fileext=".csv")
    toFactor <- names(which(unlist(lapply(object, is.factor))))
    write.csv(object, file=tmpf, quote = TRUE, row.names = FALSE)
    h2f <- h2o.uploadFile(client, tmpf, key=key, header=header, sep=sep)
    invisible(lapply(toFactor, function(a) { h2f[,a] <- as.factor(h2f[,a]) }))
    unlink(tmpf)
    return(h2f)
  }
}

h2o.exec <- function(expr_to_execute, h2o = NULL, dest_key = "") {
  if (!is.null(h2o) && !.anyH2O(substitute(expr_to_execute), envir = parent.frame())) {
    return(.h2o.exec2(h2o, deparse(substitute(expr_to_execute)), dest_key))
  }
  expr <- .replace_with_keys(substitute( expr_to_execute ), envir = parent.frame())
  res <- NULL
  if (dest_key != "") .pkg.env$DESTKEY <- dest_key
  if (.pkg.env$DESTKEY == "") {
    res <- .h2o.__exec2(.pkg.env$SERVER, deparse(expr))
  } else {
    res <- .h2o.__exec2_dest_key(.pkg.env$SERVER, deparse(expr), .pkg.env$DESTKEY)
  }
  if (.pkg.env$NEWCOL != "") {
    .h2o.__remoteSend(.pkg.env$SERVER, .h2o.__HACK_SETCOLNAMES2, source=.pkg.env$FRAMEKEY,
                       cols=.pkg.env$NUMCOLS, comma_separated_list=.pkg.env$NEWCOL)
  }

  if(res$num_rows == 0 && res$num_cols == 0)
    return(res$scalar)

  key <- res$dest_key
  if (.pkg.env$FRAMEKEY != "") {
    key <- as.character(.pkg.env$FRAMEKEY)
    newFrame <- .h2o.exec2(key, h2o = .pkg.env$SERVER, key)
    topCall <- sys.calls()[[1]]
    idxs <- which( "H2OParsedData" == unlist(lapply(as.list(topCall), .eval_class, envir=parent.frame())))
    obj_name <- as.character(.pkg.env$CURS4)
    if (length(idxs) != 0) obj_name <- as.character(topCall[[idxs]])[1]

    env <- .lookUp(obj_name)
    if (is.null(env)) {
      env <- parent.frame()
    }
    assign(obj_name, newFrame, env)
    return(newFrame)
  }
  .h2o.exec2(key, h2o = .pkg.env$SERVER, key)
}

h2o.cut <- function(x, breaks) {
  if(missing(x)) stop("Must specify data set")
  if(!inherits(x, "H2OParsedData")) stop(cat("\nData must be an H2O data set. Got ", class(x), "\n"))
  if(missing(breaks) || !is.numeric(breaks)) stop("breaks must be a numeric vector")

  nums = ifelse(length(breaks) == 1, breaks, paste("c(", paste(breaks, collapse=","), ")", sep=""))
  expr = paste("cut(", x@key, ",", nums, ")", sep="")
  res = .h2o.__exec2(x@h2o, expr)
  if(res$num_rows == 0 && res$num_cols == 0)   # TODO: If logical operator, need to indicate
    return(res$scalar)
  .h2o.exec2(res$dest_key, h2o = x@h2o, res$dest_key)
}

# TODO: H2O doesn't support any arguments beyond the single H2OParsedData object (with <= 2 cols)
h2o.table <- function(x, return.in.R = FALSE) {
  if(missing(x)) stop("Must specify data set")
  if(!inherits(x, "H2OParsedData")) stop(cat("\nData must be an H2O data set. Got ", class(x), "\n"))
  if(ncol(x) > 2) stop("Unimplemented")
  tb <- .h2o.__unop2("table", x)
  
  if(return.in.R) {
    df <- as.data.frame(tb)
    if(!is.null(df$Count))
      return(xtabs(Count ~ ., data = df))
    rownames(df) <- df$'row.names'
    df$'row.names' <- NULL
    tb <- as.table(as.matrix(df))
    # TODO: Dimension names should be the names of the columns containing the cross-classifying factors
    dimnames(tb) <- list("row.levels" = rownames(tb), "col.levels" = colnames(tb))
  }
  return(tb)
}

revalue <- function(x, replace = NULL, warn_missing = TRUE) {
  if (inherits(x, "H2OParsedData")) UseMethod("revalue")
  else plyr::revalue(x,replace, warn_missing)
}

revalue.H2OParsedData <- function(x, replace = NULL, warn_missing = TRUE) {
  if (!is.null(replace)) {
    s <- paste(names(replace), replace, sep = ":", collapse = ";")
    expr <- paste("revalue(", paste(x@key, deparse(s), as.numeric(warn_missing), sep = ","), ")", sep = "")
    invisible(.h2o.__exec2(x@h2o, expr))
  }
}

#ddply <- function (.data, .variables, .fun = NULL, ..., .progress = "none",
#             .inform = FALSE, .drop = TRUE, .parallel = FALSE, .paropts = NULL) {
#             if (inherits(.data, "H2OParsedData")) UseMethod("ddply")
#             else { if (require(plyr)) {plyr::ddply(.data, .variables, .fun, ..., .progress, .inform, .drop, .parallel, .paropts)} else { stop("invalid input data for H2O. Trying to default to plyr ddply, but plyr not found. Install plyr, or use H2OParsedData objects.") } } }

h2o.ddply <- function (.data, .variables, .fun = NULL, ..., .progress = "none") {

  # .inform, .drop, .parallel, .paropts are all ignored inputs.

  if(missing(.data)) stop('must specify .data')
  if(class(.data) != "H2OParsedData") stop('.data must be an H2OParsedData object')
  if( missing(.variables) ) stop('must specify .variables')
  if( missing(.fun) ) stop('must specify .fun')
  
  mm <- match.call()
  
  # we accept eg .(col1, col2), c('col1', 'col2'), 1:2, c(1,2)
  # as column names.  This is a bit complicated
  if( class(.variables) == 'character'){
    vars <- .variables
    idx <- match(vars, colnames(.data))
  } else if( class(.variables) == 'H2Oquoted' ){
    vars <- as.character(.variables)
    idx <- match(vars, colnames(.data))
  } else if( class(.variables) == 'quoted' ){ # plyr overwrote our . fn
    vars <- names(.variables)
    idx <- match(vars, colnames(.data))
  } else if( class(.variables) == 'integer' ){
    vars <- .variables
    idx <- .variables
  } else if( class(.variables) == 'numeric' ){   # this will happen eg c(1,2,3)
    vars <- .variables
    idx <- as.integer(.variables)
  }
  
  bad <- is.na(idx) | idx < 1 | idx > ncol(.data)
  if( any(bad) ) stop( sprintf('can\'t recognize .variables %s', paste(vars[bad], sep=',')) )
  
  fun_name <- mm[[ '.fun' ]]

  if(identical(as.list(substitute(.fun))[[1]], quote(`function`))) {
    h2o.addFunction(.data@h2o, .fun, "anonymous")
    fun_name <- "anonymous"
  }

  exec_cmd <- sprintf('ddply(%s,c(%s),%s)', .data@key, paste(idx, collapse=','), as.character(fun_name))
  res <- .h2o.__exec2(.data@h2o, exec_cmd)
  .h2o.exec2(res$dest_key, h2o = .data@h2o, res$dest_key)
}

# TODO: how to avoid masking plyr?
`h2o..` <- function(...) {
  mm <- match.call()
  mm <- mm[-1]
  structure( as.list(mm), class='H2Oquoted')
}

`.` <- `h2o..`

#'
#' Impute Missing Values
#'
#' Impute the missing values in the data `column` belonging to the dataset `data`.
#'
#' Possible values for `method`:  "mean", "median", "mode"
#'
#' If `groupBy` is NULL, then for `mean`/`median`/`mode`, missing values are imputed using the column mean/median.
#'
#' If `groupBy` is not NULL, then for `mean` and `median` and `mode`, the missing values are imputed using the mean/median/mode of
#' `column` within the groups formed by the groupBy columns.
h2o.impute <- function(data, column, method = "mean", groupBy = NULL) {
  stopifnot(!missing(data))
  stopifnot(!missing(column))
  stopifnot(method %in% c("mean", "median", "mode"))
  stopifnot(inherits(data, "H2OParsedData"))

  .data <- data
  .variables <- groupBy
  idx <- NULL
  if (!is.null(.variables)) {
  # we accept eg .(col1, col2), c('col1', 'col2'), 1:2, c(1,2)
    # as column names.  This is a bit complicated
    if( class(.variables) == 'character'){
      vars <- .variables
      idx <- match(vars, colnames(.data))
    } else if( class(.variables) == 'H2Oquoted' ){
      vars <- as.character(.variables)
      idx <- match(vars, colnames(.data))
    } else if( class(.variables) == 'quoted' ){ # plyr overwrote our . fn
      vars <- names(.variables)
      idx <- match(vars, colnames(.data))
    } else if( class(.variables) == 'integer' ){
      vars <- .variables
      idx <- .variables
    } else if( class(.variables) == 'numeric' ){   # this will happen eg c(1,2,3)
      vars <- .variables
      idx <- as.integer(.variables)
    }
    bad <- is.na(idx) | idx < 1 | idx > ncol(.data)
    if( any(bad) ) stop( sprintf('can\'t recognize .variables %s', paste(vars[bad], sep=',')) )
    idx <- idx - 1
  }

  col_idx <- NULL
  if( class(column) == 'character'){
    vars <- column
    col_idx <- match(vars, colnames(.data))
  } else if( class(column) == 'H2Oquoted' ){
    vars <- as.character(column)
    col_idx <- match(vars, colnames(.data))
  } else if( class(column) == 'quoted' ){ # plyr overwrote our . fn
    vars <- names(column)
    col_idx <- match(vars, colnames(.data))
  } else if( class(column) == 'integer' ){
    vars <- column
    col_idx <- column
  } else if( class(column) == 'numeric' ){   # this will happen eg c(1,2,3)
    vars <- column
    col_idx <- as.integer(column)
  }
  bad <- is.na(col_idx) | col_idx < 1 | col_idx > ncol(.data)
  if( any(bad) ) stop( sprintf('can\'t recognize column %s', paste(vars[bad], sep=',')) )
  if (length(col_idx) > 1) stop("Only allows imputation of a single column at a time!")
  invisible(.h2o.__remoteSend(data@h2o, .h2o.__PAGE_IMPUTE, source=data@key, column=col_idx-1, method=method, group_by=idx))
}

h2o.addFunction <- function(object, fun, name){
  if( missing(object) || class(object) != 'H2OClient' ) stop('must specify h2o connection in object')
  if( missing(fun) ) stop('must specify fun')
  if( !missing(name) ){
    if( class(name) != 'character' ) stop('name must be a name')
    fun_name <- name
  } else {
    fun_name <- match.call()[['fun']]
  }
  src <- paste(deparse(fun), collapse='\n')
  exec_cmd <- sprintf('%s <- %s', as.character(fun_name), src)
  res <- .h2o.__exec2(object, exec_cmd)
}

h2o.unique <- function(x, incomparables = FALSE, ...){
  # NB: we do nothing with incomparables right now
  # NB: we only support MARGIN = 2 (which is the default)

  if(class(x) != "H2OParsedData")
    stop('h2o.unique: x must be an H2OParsedData object')
  if( nrow(x) == 0 | ncol(x) == 0) return(NULL) 
  if( nrow(x) == 1) return(x)

  args <- list(...)
  if( 'MARGIN' %in% names(args) && args[['MARGIN']] != 2 ) stop('h2o.unique: only MARGIN 2 supported')
  .h2o.__unop2("unique", x)
  
#   uniq <- function(df){1}
#   h2o.addFunction(l, uniq)
#   res <- h2o.ddply(x, 1:ncol(x), uniq)
# 
#   res[,1:(ncol(res)-1)]
}
unique.H2OParsedData <- h2o.unique

h2o.runif <- function(x, min = 0, max = 1, seed = -1) {
  if(missing(x)) stop("Must specify data set")
  if(class(x) != "H2OParsedData") stop(cat("\nData must be an H2O data set. Got ", class(x), "\n"))
  if(!is.numeric(min)) stop("min must be a single number")
  if(!is.numeric(max)) stop("max must be a single number")
  if(length(min) > 1 || length(max) > 1) stop("Unimplemented")
  if(min > max) stop("min must be a number less than or equal to max")
  if(!is.numeric(seed)) stop("seed must be an integer >= 0")
  
  expr = paste("runif(", x@key, ",", seed, ")*(", max - min, ")+", min, sep = "")
  res = .h2o.__exec2(x@h2o, expr)
  if(res$num_rows == 0 && res$num_cols == 0)
    return(res$scalar)
  else {
    res <- .h2o.exec2(res$dest_key, h2o = x@h2o, res$dest_key)
    res@logic <- FALSE
    return(res)
  }
}

h2o.anyFactor <- function(x) {
  if(class(x) != "H2OParsedData") stop("x must be an H2OParsedData object")
  x@any_enum
#  as.logical(.h2o.__unop2("any.factor", x))
}

setMethod("colnames", "H2OParsedData", function(x, do.NULL = TRUE, prefix = "col") {
  x@col_names
})

#--------------------------------- Overloaded R Methods ----------------------------------#
#--------------------------------- Slicing ----------------------------------#
# i are the rows, j are the columns. These can be vectors of integers or character strings, or a single logical data object
setMethod("[", "H2OParsedData", function(x, i, j, ..., drop = TRUE) {
  numRows <- nrow(x); numCols <- ncol(x)
  if (!missing(j) && is.numeric(j) && any(abs(j) < 1 || abs(j) > numCols))
    stop("Array index out of bounds")

  if(missing(i) && missing(j)) return(x)
  if(missing(i) && !missing(j)) {
    if(is.character(j)) {
      # return(do.call("$", c(x, j)))
      myCol <- colnames(x)
      if(any(!(j %in% myCol))) stop("Undefined columns selected")
      j <- match(j, myCol)
    }
    # if(is.logical(j)) j = -which(!j)
    if(is.logical(j)) j <- which(j)

    # if(class(j) == "H2OLogicalData")
    if(class(j) == "H2OParsedData" && j@logic)
      expr <- paste(x@key, "[", j@key, ",]", sep="")
    else if(is.numeric(j) || is.integer(j))
      expr <- paste(x@key, "[,c(", paste(j, collapse=","), ")]", sep="")
    else stop(paste("Column index of type", class(j), "unsupported!"))
  } else if(!missing(i) && missing(j)) {
    # treat `i` as a column selector in this case...
    if (is.character(i)) {
      myCol <- colnames(x)
      if (any(!(i %in% myCol))) stop ("Undefined columns selected")
      i <- match(i, myCol)
      if(is.logical(i)) i <- which(i)
      if(class(i) == "H2OParsedData" && i@logic)
        expr <- paste(x@key, "[", i@key, ",]", sep="")
      else if(is.numeric(i) || is.integer(i))
        expr <- paste(x@key, "[,c(", paste(i, collapse=","), ")]", sep="")
      else stop(paste("Column index of type", class(i), "unsupported!"))
    } else {
    # if(is.logical(i)) i = -which(!i)
    if(is.logical(i)) i = which(i)
    # if(class(i) == "H2OLogicalData")
    if(class(i) == "H2OParsedData" && i@logic)
      expr <- paste(x@key, "[", i@key, ",]", sep="")
    else if(is.numeric(i) || is.integer(i))
      expr <- paste(x@key, "[c(", paste(i, collapse=","), "),]", sep="")
    else stop(paste("Row index of type", class(i), "unsupported!"))
   }
  } else {
    # if(is.logical(i)) i = -which(!i)
    if(is.logical(i)) i <- which(i)
    # if(class(i) == "H2OLogicalData") rind = i@key
    if(class(i) == "H2OParsedData" && i@logic) rind = i@key
    else if(is.numeric(i) || is.integer(i))
      rind <- paste("c(", paste(i, collapse=","), ")", sep="")
    else stop(paste("Row index of type", class(i), "unsupported!"))

    if(is.character(j)) {
      # return(do.call("$", c(x, j)))
      myCol <- colnames(x)
      if(any(!(j %in% myCol))) stop("Undefined columns selected")
      j <- match(j, myCol)
    }
    # if(is.logical(j)) j = -which(!j)
    if(is.logical(j)) j <- which(j)
    # if(class(j) == "H2OLogicalData") cind = j@key
    if(class(j) == "H2OParsedData" && j@logic) cind <- j@key
    else if(is.numeric(j) || is.integer(j))
      cind <- paste("c(", paste(j, collapse=","), ")", sep="")
    else stop(paste("Column index of type", class(j), "unsupported!"))
    expr <- paste(x@key, "[", rind, ",", cind, "]", sep="")
  }
  res <- .h2o.__exec2(x@h2o, expr)
  if(res$num_rows == 0 && res$num_cols == 0)
    res$scalar
  else
    .h2o.exec2(res$dest_key, h2o = x@h2o, res$dest_key)
})

setMethod("$", "H2OParsedData", function(x, name) {
  myNames <- colnames(x)
  # if(!(name %in% myNames)) return(NULL)
  if(!(name %in% myNames)) stop(paste("Column", name, "does not exist!"))
  cind <- match(name, myNames)
  expr <- paste(x@key, "[,", cind, "]", sep="")
  res <- .h2o.__exec2(x@h2o, expr)
  if(res$num_rows == 0 && res$num_cols == 0)
    res$scalar
  else
    .h2o.exec2(res$dest_key, h2o = x@h2o, res$dest_key)
})

setMethod("[<-", "H2OParsedData", function(x, i, j, ..., value) {
  numRows = nrow(x); numCols = ncol(x)
  # if((!missing(i) && is.numeric(i) && any(abs(i) < 1 || abs(i) > numRows)) ||
  #     (!missing(j) && is.numeric(j) && any(abs(j) < 1 || abs(j) > numCols)))
  #  stop("Array index out of bounds!")
  if(!(missing(i) || is.numeric(i) || is.character(i)) || !(missing(j) || is.numeric(j) || is.character(j)))
    stop("Row/column types not supported!")
  if(class(value) != "H2OParsedData" && !is.numeric(value))
    stop("value can only be numeric or an H2OParsedData object")
  if(is.numeric(value) && length(value) != 1 && length(value) != numRows)
    stop("value must be either a single number or a vector of length ", numRows)

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
    lhs <- x@key
  else if(missing(i) && !missing(j)) {
    if(is.character(j)) {
      myNames <- colnames(x)
      if(any(!(j %in% myNames))) {
        if(length(j) == 1)
          return(do.call("$<-", list(x, j, value)))
        else stop("Unimplemented: undefined column names specified")
      }
      cind <- match(j, myNames)
    } else cind <- j
    cind <- paste("c(", paste(cind, collapse = ","), ")", sep = "")
    lhs <- paste(x@key, "[,", cind, "]", sep = "")
  } else if(!missing(i) && missing(j)) {
      # treat `i` as a column selector in this case...
      if (is.character(i)) {
        myNames <- colnames(x)
        if (any(!(i %in% myNames))) {
          if (length(i) == 1) return(do.call("$<-", list(x, i, value)))
          else stop("Unimplemented: undefined column names specified")
          }
        cind <- match(i, myNames)
        cind <- paste("c(", paste(cind, collapse = ","), ")", sep = "")
        lhs <- paste(x@key, "[,", cind, "]", sep = "")
        } else {
        rind <- paste("c(", paste(i, collapse = ","), ")", sep = "")
        lhs <- paste(x@key, "[", rind, ",]", sep = "")
      }
  } else {
    if(is.character(j)) {
      myNames <- colnames(x)
      if(any(!(j %in% myNames))) stop("Unimplemented: undefined column names specified")
      cind <- match(j, myNames)
      # cind = match(j[j %in% myNames], myNames)
    } else cind <- j
    cind <- paste("c(", paste(cind, collapse = ","), ")", sep = "")
    rind <- paste("c(", paste(i, collapse = ","), ")", sep = "")
    lhs <- paste(x@key, "[", rind, ",", cind, "]", sep = "")
  }

  # rhs = ifelse(class(value) == "H2OParsedData", value@key, paste("c(", paste(value, collapse = ","), ")", sep=""))
  if(class(value) == "H2OParsedData")
    rhs <- value@key
  else
    rhs <- ifelse(length(value) == 1, value, paste("c(", paste(value, collapse = ","), ")", sep=""))
  res <- .h2o.__exec2(x@h2o, paste(lhs, "=", rhs))
  .h2o.exec2(x@key, h2o = x@h2o, x@key)
})

setMethod("$<-", "H2OParsedData", function(x, name, value) {
  if(missing(name) || !is.character(name) || nchar(name) == 0)
    stop("name must be a non-empty string")
  if(class(value) != "H2OParsedData" && !is.numeric(value))
    stop("value can only be numeric or an H2OParsedData object")
  numCols <- ncol(x); numRows <- nrow(x)
  if(is.numeric(value) && length(value) != 1 && length(value) != numRows)
    stop("value must be either a single number or a vector of length ", numRows)
  myNames <- colnames(x); idx <- match(name, myNames)
 
  lhs <- paste(x@key, "[,", ifelse(is.na(idx), numCols+1, idx), "]", sep = "")
  # rhs = ifelse(class(value) == "H2OParsedData", value@key, paste("c(", paste(value, collapse = ","), ")", sep=""))
  if(class(value) == "H2OParsedData")
    rhs <- value@key
  else
    rhs <- ifelse(length(value) == 1, value, paste("c(", paste(value, collapse = ","), ")", sep=""))
  .h2o.__exec2(x@h2o, paste(lhs, "=", rhs))
  
  if(is.na(idx))
    .h2o.__remoteSend(x@h2o, .h2o.__HACK_SETCOLNAMES2, source=x@key, cols=numCols, comma_separated_list=name)
  .h2o.exec2(x@key, h2o = x@h2o, x@key)
})

setMethod("[[", "H2OParsedData", function(x, i, exact = TRUE) {
  if(missing(i)) return(x)
  if(length(i) > 1) stop("[[]] may only select one column")
  if(!i %in% colnames(x) ) { warning(paste("Column", i, "does not exist!")); return(NULL) }
  x[, i]
})

setMethod("[[<-", "H2OParsedData", function(x, i, value) {
  if(class(value) != "H2OParsedData") stop('Can only append H2O data to H2O data')
  if( ncol(value) > 1 ) stop('May only set a single column')
  if( nrow(value) != nrow(x) ) stop(sprintf('Replacement has %d row, data has %d', nrow(value), nrow(x)))

  mm <- match.call()
  col_name <- as.list(i)[[1]]

  cc <- colnames(x)
  if( col_name %in% cc ){
    x[, match( col_name, cc ) ] <- value
  } else {
    x <- cbind(x, value)
    cc <- c( cc, col_name )
    colnames(x) <- cc
  }
  x
})

# Note: right now, all things must be H2OParsedData
cbind.H2OParsedData <- function(..., deparse.level = 1) {
  if(deparse.level != 1) stop("Unimplemented")
  
  l <- unlist(list(...))
  # l_dep <- sapply(substitute(placeholderFunction(...))[-1], deparse)
  if(length(l) == 0) stop('cbind requires an H2O parsed dataset')
  
  klass <- 'H2OParsedData'
  h2o <- l[[1]]@h2o
  nrows <- nrow(l[[1]])
  m <- Map(function(elem){ inherits(elem, klass) & elem@h2o@ip == h2o@ip & elem@h2o@port == h2o@port & nrows == nrow(elem) }, l)
  compatible <- Reduce(function(l,r) l & r, x=m, init=T)
  if(!compatible){ stop(paste('cbind: all elements must be of type', klass, 'and in the same H2O instance'))}
  
  # If cbind(x,x), dupe colnames will automatically be renamed by H2O
  if(is.null(names(l)))
    tmp <- Map(function(x) x@key, l)
  else
    tmp <- mapply(function(x,n) { if(is.null(n) || is.na(n) || nchar(n) == 0) x@key else paste(n, x@key, sep = "=") }, l, names(l))
  
  exec_cmd <- sprintf("cbind(%s)", paste(as.vector(tmp), collapse = ","))
  res <- .h2o.__exec2(h2o, exec_cmd)
  .h2o.exec2(res$dest_key, h2o = h2o, res$dest_key)
}


# Note: right now, all things must be H2OParsedData
rbind.H2OParsedData <- function(..., deparse.level = 1) {
  if(deparse.level != 1) stop("Unimplemented")

  l <- unlist(list(...))
  # l_dep <- sapply(substitute(placeholderFunction(...))[-1], deparse)
  if(length(l) == 0) stop('rbind requires an H2O parsed dataset')

#  klass <- 'H2OParsedData'
  h2o <- l[[1]]@h2o
#  m <- Map(function(elem){ inherits(elem, klass) & elem@h2o@ip == h2o@ip & elem@h2o@port == h2o@port & nrows == nrow(elem) }, l)
#  compatible <- Reduce(function(l,r) l & r, x=m, init=T)
#  if(!compatible){ stop(paste('rbind: all elements must be of type', klass, 'and in the same H2O instance'))}

  # If cbind(x,x), dupe colnames will automatically be renamed by H2O
  if(is.null(names(l)))
    tmp <- Map(function(x) x@key, l)
  else
    tmp <- mapply(function(x,n) { if(is.null(n) || is.na(n) || nchar(n) == 0) x@key else paste(n, x@key, sep = "=") }, l, names(l))

  exec_cmd <- sprintf("rbind(%s)", paste(as.vector(tmp), collapse = ","))
  res <- .h2o.__exec2(h2o, exec_cmd)
  .h2o.exec2(res$dest_key, h2o = h2o, res$dest_key)
}

#--------------------------------- Arithmetic ----------------------------------#
setMethod("+", c("H2OParsedData", "missing"), function(e1, e2) { .h2o.__binop2("+", 0, e1) })
setMethod("-", c("H2OParsedData", "missing"), function(e1, e2) { .h2o.__binop2("-", 0, e1) })

setMethod("+", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("+", e1, e2) })
setMethod("-", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("-", e1, e2) })
setMethod("*", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("*", e1, e2) })
setMethod("/", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("/", e1, e2) })
setMethod("%%", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("%", e1, e2) })
setMethod("==", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("==", e1, e2) })
setMethod(">", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { .h2o.__binop2(">", e1, e2) })
setMethod("<", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("<", e1, e2) })
setMethod("!=", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("!=", e1, e2) })
setMethod(">=", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { .h2o.__binop2(">=", e1, e2) })
setMethod("<=", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("<=", e1, e2) })
setMethod("&", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("&", e1, e2) })
setMethod("|", c("H2OParsedData", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("|", e1, e2) })
setMethod("%*%", c("H2OParsedData", "H2OParsedData"), function(x, y) { .h2o.__binop2("%*%", x, y) })

setMethod("+", c("numeric", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("+", e1, e2) })
setMethod("-", c("numeric", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("-", e1, e2) })
setMethod("*", c("numeric", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("*", e1, e2) })
setMethod("/", c("numeric", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("/", e1, e2) })
setMethod("%%", c("numeric", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("%", e1, e2) })
setMethod("==", c("numeric", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("==", e1, e2) })
setMethod(">", c("numeric", "H2OParsedData"), function(e1, e2) { .h2o.__binop2(">", e1, e2) })
setMethod("<", c("numeric", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("<", e1, e2) })
setMethod("!=", c("numeric", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("!=", e1, e2) })
setMethod(">=", c("numeric", "H2OParsedData"), function(e1, e2) { .h2o.__binop2(">=", e1, e2) })
setMethod("<=", c("numeric", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("<=", e1, e2) })
setMethod("&", c("numeric", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("&", e1, e2) })
setMethod("|", c("numeric", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("|", e1, e2) })

setMethod("+", c("H2OParsedData", "numeric"), function(e1, e2) { .h2o.__binop2("+", e1, e2) })
setMethod("-", c("H2OParsedData", "numeric"), function(e1, e2) { .h2o.__binop2("-", e1, e2) })
setMethod("*", c("H2OParsedData", "numeric"), function(e1, e2) { .h2o.__binop2("*", e1, e2) })
setMethod("/", c("H2OParsedData", "numeric"), function(e1, e2) { .h2o.__binop2("/", e1, e2) })
setMethod("%%", c("H2OParsedData", "numeric"), function(e1, e2) { .h2o.__binop2("%", e1, e2) })
setMethod("==", c("H2OParsedData", "numeric"), function(e1, e2) { .h2o.__binop2("==", e1, e2) })
setMethod(">", c("H2OParsedData", "numeric"), function(e1, e2) { .h2o.__binop2(">", e1, e2) })
setMethod("<", c("H2OParsedData", "numeric"), function(e1, e2) { .h2o.__binop2("<", e1, e2) })
setMethod("!=", c("H2OParsedData", "numeric"), function(e1, e2) { .h2o.__binop2("!=", e1, e2) })
setMethod(">=", c("H2OParsedData", "numeric"), function(e1, e2) { .h2o.__binop2(">=", e1, e2) })
setMethod("<=", c("H2OParsedData", "numeric"), function(e1, e2) { .h2o.__binop2("<=", e1, e2) })
setMethod("&", c("H2OParsedData", "numeric"), function(e1, e2) { .h2o.__binop2("&", e1, e2) })
setMethod("|", c("H2OParsedData", "numeric"), function(e1, e2) { .h2o.__binop2("|", e1, e2) })

setMethod("&", c("logical", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("&", as.numeric(e1), e2) })
setMethod("|", c("logical", "H2OParsedData"), function(e1, e2) { .h2o.__binop2("|", as.numeric(e1), e2) })
setMethod("&", c("H2OParsedData", "logical"), function(e1, e2) { .h2o.__binop2("&", e1, as.numeric(e2)) })
setMethod("|", c("H2OParsedData", "logical"), function(e1, e2) { .h2o.__binop2("|", e1, as.numeric(e2)) })
setMethod("%/%", c("numeric", "H2OParsedData"), function(e1, e2) {.h2o.__binop2("%/%", as.numeric(e1), e2) })
setMethod("%/%", c("H2OParsedData", "numeric"), function(e1, e2){ .h2o.__binop2("%/%", e1, as.numeric(e2)) })
setMethod("^", c("numeric", "H2OParsedData"), function(e1, e2) {.h2o.__binop2("^", as.numeric(e1), e2) })
setMethod("^", c("H2OParsedData", "numeric"), function(e1, e2){ .h2o.__binop2("^", e1, as.numeric(e2)) })


#'
#' Get the domain mapping of an int and a String
#'
.getDomainMapping <- function(vec, s="") {
  if(class(vec) != "H2OParsedData") stop("Object must be a H2OParsedData object. Input was: ", vec)
  .h2o.__remoteSend(vec@h2o, .h2o.__DOMAIN_MAPPING, src_key = vec@key, str = s)
}

setMethod("==", c("H2OParsedData", "character"), function(e1, e2) {
  m <- .getDomainMapping(e1,e2)$map
  .h2o.__binop2("==", e1, m)
})

setMethod("==", c("character", "H2OParsedData"), function(e1, e2) {
  m <- .getDomainMapping(e2,e1)$map
  .h2o.__binop2("==", m, e2)
})

setMethod("!=", c("H2OParsedData", "character"), function(e1, e2) {
  m <- .getDomainMapping(e1,e2)$map
  .h2o.__binop2("!=", e1, m)
})

setMethod("!=", c("character", "H2OParsedData"), function(e1, e2) {
  m <- .getDomainMapping(e2,e1)$map
  .h2o.__binop2("!=", m, e2)
})

setMethod("!",       "H2OParsedData", function(x) { .h2o.__unop2("!",     x) })
setMethod("abs",     "H2OParsedData", function(x) { .h2o.__unop2("abs",   x) })
setMethod("sign",    "H2OParsedData", function(x) { .h2o.__unop2("sgn",   x) })
setMethod("sqrt",    "H2OParsedData", function(x) { .h2o.__unop2("sqrt",  x) })
setMethod("ceiling", "H2OParsedData", function(x) { .h2o.__unop2("ceil",  x) })
setMethod("floor",   "H2OParsedData", function(x) { .h2o.__unop2("floor", x) })
setMethod("trunc",   "H2OParsedData", function(x) { .h2o.__unop2("trunc", x) })
setMethod("log",     "H2OParsedData", function(x) { .h2o.__unop2("log",   x) })
setMethod("exp",     "H2OParsedData", function(x) { .h2o.__unop2("exp",   x) })
setMethod("is.na",   "H2OParsedData", function(x) {
  res <- .h2o.__unop2("is.na", x)
#  res <- as.numeric(res)
})
setMethod("t",       "H2OParsedData", function(x) { .h2o.__unop2("t",     x) })
#setMethod("as.numeric", "H2OParsedData", function(x) { .h2o.__unop2("as.numeric", x) })

round.H2OParsedData <- function(x, digits = 0) {
  if(length(digits) > 1 || !is.numeric(digits)) stop("digits must be a single number")
  
  expr <- paste("round(", paste(x@key, digits, sep = ","), ")", sep = "")
  res <- .h2o.__exec2(x@h2o, expr)
  if(res$num_rows == 0 && res$num_cols == 0)
    return(res$scalar)
  .h2o.exec2(expr = res$dest_key, h2o = x@h2o, dest_key = res$dest_key)
}

signif.H2OParsedData <- function(x, digits = 6) {
  if(length(digits) > 1 || !is.numeric(digits)) stop("digits must be a single number")
  
  expr <- paste("signif(", paste(x@key, digits, sep = ","), ")", sep = "")
  res <- .h2o.__exec2(x@h2o, expr)
  if(res$num_rows == 0 && res$num_cols == 0)
    return(res$scalar)
  .h2o.exec2(expr = res$dest_key, h2o = x@h2o, dest_key = res$dest_key)
}

setMethod("colnames<-", signature(x="H2OParsedData", value="H2OParsedData"),
  function(x, value) {
    if(ncol(value) != ncol(x)) stop("Mismatched number of columns")
    res <- .h2o.__remoteSend(x@h2o, .h2o.__HACK_SETCOLNAMES2, source=x@key, copy_from=value@key)
    x@col_names <- value@col_names
    return(x)
})

setMethod("colnames<-", signature(x="H2OParsedData", value="character"),
  function(x, value) {
    if(any(nchar(value) == 0)) stop("Column names must be of non-zero length")
    else if(any(duplicated(value))) stop("Column names must be unique")
    else if(length(value) != (num = ncol(x))) stop(paste("Must specify a vector of exactly", num, "column names"))
    res <- .h2o.__remoteSend(x@h2o, .h2o.__HACK_SETCOLNAMES2, source=x@key, comma_separated_list=value)
    x@col_names <- value
    return(x)
})

setMethod("names", "H2OParsedData", function(x) { colnames(x) })
setMethod("names<-", "H2OParsedData", function(x, value) { colnames(x) <- value; return(x) })
# setMethod("nrow", "H2OParsedData", function(x) { .h2o.__unop2("nrow", x) })
# setMethod("ncol", "H2OParsedData", function(x) { .h2o.__unop2("ncol", x) })

setMethod("nrow", "H2OParsedData", function(x) {
  x@nrows
})
#  res = .h2o.__remoteSend(x@h2o, .h2o.__PAGE_INSPECT2, src_key=x@key); as.numeric(res$numRows) })

setMethod("ncol", "H2OParsedData", function(x) {
  x@ncols
#  res = .h2o.__remoteSend(x@h2o, .h2o.__PAGE_INSPECT2, src_key=x@key); as.numeric(res$numCols)
})

setMethod("length", "H2OParsedData", function(x) {
  numCols <- ncol(x)
  if (numCols == 1) {
    numRows <- nrow(x)
    return (numRows)      
  }
  return (numCols)
})

setMethod("dim", "H2OParsedData", function(x) {
  c(x@nrows, x@ncols)
#  res = .h2o.__remoteSend(x@h2o, .h2o.__PAGE_INSPECT2, src_key=x@key)
#  as.numeric(c(res$numRows, res$numCols))
})

setMethod("dim<-", "H2OParsedData", function(x, value) { stop("Unimplemented") })

# setMethod("min", "H2OParsedData", function(x, ..., na.rm = FALSE) {
#   if(na.rm) stop("Unimplemented")
#   # res = .h2o.__remoteSend(x@h2o, .h2o.__PAGE_INSPECT2, src_key=x@key)
#   # min(..., sapply(res$cols, function(x) { x$min }), na.rm)
#   min(..., .h2o.__unop2("min", x), na.rm)
# })
#
# setMethod("max", "H2OParsedData", function(x, ..., na.rm = FALSE) {
#   if(na.rm) stop("Unimplemented")
#   # res = .h2o.__remoteSend(x@h2o, .h2o.__PAGE_INSPECT2, src_key=x@key)
#   # max(..., sapply(res$cols, function(x) { x$max }), na.rm)
#   max(..., .h2o.__unop2("max", x), na.rm)
# })

.min_internal <- min
min <- function(..., na.rm = FALSE) {
  idx = sapply(c(...), function(y) { class(y) == "H2OParsedData" })
  
  if(any(idx)) {
    hex.op = ifelse(na.rm, "min.na.rm", "min")
    myVals = c(...); myData = myVals[idx]
    myKeys = sapply(myData, function(y) { y@key })
    expr = paste(hex.op, "(", paste(myKeys, collapse=","), ")", sep = "")
    res = .h2o.__exec2(myData[[1]]@h2o, expr)
    .Primitive("min")(unlist(myVals[!idx]), res$scalar, na.rm = na.rm)
  } else
    .Primitive("min")(..., na.rm = na.rm)
}

.max_internal <- max
max <- function(..., na.rm = FALSE) {
  idx = sapply(c(...), function(y) { class(y) == "H2OParsedData" })
   
  if(any(idx)) {
    hex.op = ifelse(na.rm, "max.na.rm", "max")
    myVals = c(...); myData = myVals[idx]
    myKeys = sapply(myData, function(y) { y@key })
    expr = paste(hex.op, "(", paste(myKeys, collapse=","), ")", sep = "")
    res = .h2o.__exec2(myData[[1]]@h2o, expr)
    .Primitive("max")(unlist(myVals[!idx]), res$scalar, na.rm = na.rm)
  } else
    .Primitive("max")(..., na.rm = na.rm)
}

.sum_internal <- sum
sum <- function(..., na.rm = FALSE) {
  idx = sapply(c(...), function(y) { class(y) == "H2OParsedData" })
  
  if(any(idx)) {
    hex.op = ifelse(na.rm, "sum.na.rm", "sum")
    myVals = c(...); myData = myVals[idx]
    myKeys = sapply(myData, function(y) { y@key })
    expr = paste(hex.op, "(", paste(myKeys, collapse=","), ")", sep = "")
    res = .h2o.__exec2(myData[[1]]@h2o, expr)
    .Primitive("sum")(unlist(myVals[!idx]), res$scalar, na.rm = na.rm)
  } else
    .Primitive("sum")(..., na.rm = na.rm)
}

setMethod("range", "H2OParsedData", function(x) {
  res = .h2o.__remoteSend(x@h2o, .h2o.__PAGE_INSPECT2, src_key=x@key)
  temp = sapply(res$cols, function(x) { c(x$min, x$max) })
  c(min(temp[1,]), max(temp[2,]))
})

mean.H2OParsedData <- function(x, trim = 0, na.rm = FALSE, ...) {
  if(ncol(x) != 1 || trim != 0) stop("Unimplemented")
  if(h2o.anyFactor(x) || dim(x)[2] != 1) {
    warning("argument is not numeric or logical: returning NA")
    return(NA_real_)
  }
  if(!na.rm && .h2o.__unop2("any.na", x)) return(NA)
  .h2o.__unop2("mean", x)
}

setMethod("sd", "H2OParsedData", function(x, na.rm = FALSE) {
  if(ncol(x) != 1) stop("Unimplemented")
  if(dim(x)[2] != 1 || h2o.anyFactor(x)) stop("Could not coerce argument to double. H2O sd requires a single numeric column.")
  if(!na.rm && .h2o.__unop2("any.na", x)) return(NA)
  .h2o.__unop2("sd", x)
})

setMethod("var", "H2OParsedData", function(x, y = NULL, na.rm = FALSE, use) {
  if(!is.null(y) || !missing(use)) stop("Unimplemented")
  if(h2o.anyFactor(x)) stop("x cannot contain any categorical columns")
  if(!na.rm && .h2o.__unop2("any.na", x)) return(NA)
  .h2o.__unop2("var", x)
})

as.data.frame.H2OParsedData <- function(x, ...) {
  if(class(x) != "H2OParsedData") stop("x must be of class H2OParsedData")
  # Versions of R prior to 3.1 should not use hex string.
  # Versions of R including 3.1 and later should use hex string.
  use_hex_string = FALSE
  if (as.numeric(R.Version()$major) >= 3) {
    if (as.numeric(R.Version()$minor) >= 1) {
      use_hex_string = TRUE
    }
  }

  url <- paste('http://', x@h2o@ip, ':', x@h2o@port,
               '/2/DownloadDataset',
               '?src_key=', URLencode(x@key),
               '&hex_string=', as.numeric(use_hex_string),
               sep='')
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
  
  # if((df.ncol = ncol(df)) != (x.ncol = ncol(x)))
  #  stop("Stopping conversion: Expected ", x.ncol, " columns, but data frame imported with ", df.ncol)
  # if(x.ncol > .MAX_INSPECT_COL_VIEW)
  #  warning(x@key, " has greater than ", .MAX_INSPECT_COL_VIEW, " columns. This may take awhile...")
  
  # Obtain the correct factor levels for each column
  # res = .h2o.__remoteSend(x@h2o, .h2o.__HACK_LEVELS2, source=x@key, max_ncols=.Machine$integer.max)
  # colClasses = sapply(res$levels, function(x) { ifelse(is.null(x), "numeric", "factor") })

  # Substitute NAs for blank cells rather than skipping
  df <- read.csv((tcon <- textConnection(ttt)), blank.lines.skip = FALSE, ...)
  # df = read.csv(textConnection(ttt), blank.lines.skip = FALSE, colClasses = colClasses, ...)
  close(tcon)
  return(df)
}

as.matrix.H2OParsedData <- function(x, ...) { as.matrix(as.data.frame(x, ...)) }
as.table.H2OParsedData <- function(x, ...) { as.table(as.matrix(x, ...))}

head.H2OParsedData <- function(x, n = 6L, ...) {
  numRows = nrow(x)
  stopifnot(length(n) == 1L)
  n <- ifelse(n < 0L, max(numRows + n, 0L), min(n, numRows))
  if(n == 0) return(data.frame())

  tmp_head <- x[seq_len(n),]
  x.slice = as.data.frame(tmp_head)
  h2o.rm(tmp_head@h2o, tmp_head@key)
  return(x.slice)
}

tail.H2OParsedData <- function(x, n = 6L, ...) {
  stopifnot(length(n) == 1L)
  nrx <- nrow(x)
  n <- ifelse(n < 0L, max(nrx + n, 0L), min(n, nrx))
  if(n == 0) return(data.frame())
  
  idx <- seq.int(to = nrx, length.out = n)
  tmp_tail <- x[idx,]
  x.slice <- as.data.frame(tmp_tail)
  h2o.rm(tmp_tail@h2o, tmp_tail@key)
  rownames(x.slice) <- idx
  return(x.slice)
}

setMethod("as.factor", "H2OParsedData", function(x) { .h2o.__unop2("factor", x) })
setMethod("is.factor", "H2OParsedData", function(x) { as.logical(.h2o.__unop2("is.factor", x)) })
#setMethod("as.numeric", "H2OParsedData", function(x, ...) {
#  if(class(x) != "H2OParsedData") stop("x must be of class H2OParsedData")
#  .h2o.__unop2("as.numeric", x)
#})

as.numeric.H2OParsedData <- function(x, ...) {
  if(class(x) != "H2OParsedData") stop("x must be of class H2OParsedData")
  .h2o.__unop2("as.numeric", x)
}
#setMethod("as.numeric", "H2OParsedData", function(x) { .h2o.__unop2("as.numeric", x) })

#'
#' The H2O Gains Method
#'
#' Construct the gains table and lift charts for binary outcome algorithms. Lift charts and gains tables
#' are commonly applied to marketing.
#'
#' Ties are broken by building quantiles over the data (tie-breaking needed in deciding which group an observation
#' belongs). Please examine the GainsTask within the GainsLiftTable.java class for more details on ranking.
#'
#' The values returned are in percent form if `percents` is TRUE.
h2o.gains <- function(actual, predicted, groups=10, percents = FALSE) {
  if(class(actual) != "H2OParsedData") stop("`actual` must be an H2O parsed dataset")
  if(class(predicted) != "H2OParsedData") stop("`predicted` must be an H2O parsed dataset")
  if(ncol(actual) != 1) stop("Must specify exactly one column for `actual`")
  if(ncol(predicted) != 1) stop("Must specify exactly one column for `predicted`")
  if(groups < 1) stop("`groups` must be  >= 1. Got: " %p0% groups)

  h2o <- actual@h2o
  res <- .h2o.__remoteSend(h2o, .h2o.__GAINS, actual = actual@key, vactual = 0, predict = predicted@key, vpredict = 0, groups = groups)
  resp_rates <- res$response_rates
  avg <- res$avg_response_rate
  groups <- res$groups
  percents <- 99*percents + 1  # multiply by 100 or by 1
  lifts <- resp_rates / avg

  # need to build a data frame with 4 columns: Quantile, Response Rate, Lift, Cum. Lift
  col_names <- c("Quantile", "Mean.Response", "Lift", "Cume.Pct.Total.Lift")

  gains_table <- data.frame(
    Quantile        = qtiles <- seq(0,1,1/groups)[-1] * percents,
    Response.Rate   = resp_rates, # * percents,
    Lift            = (resp_rates / avg),
    Cumulative.Lift = cumsum(lifts/groups) * percents
  )
  colnames(gains_table) <- col_names
  gains_table
}

setMethod("which", "H2OParsedData", function(x, arr.ind = FALSE, useNames = TRUE) {
  .h2o.__unop2("which", x)
})

strsplit <- function(x, split, fixed = FALSE, perl = FALSE, useBytes = FALSE) {
  if (inherits(x, "H2OParsedData")) { UseMethod("strsplit")
  } else base::strsplit(x, split, fixed, perl, useBytes)
}

tolower <- function(x) if (inherits(x, "H2OParsedData")) UseMethod("tolower") else base::tolower(x)
toupper <- function(x) if (inherits(x, "H2OParsedData")) UseMethod("toupper") else base::toupper(x)

tolower.H2OParsedData <- function(x) {
  expr <- paste("tolower(", x@key, ")", sep = "")
  res <- .h2o.__exec2(x@h2o, expr)
  res <- .h2o.exec2(res$dest_key, h2o = x@h2o, res$dest_key)
  res@logic <- FALSE
  return(res)
}

toupper.H2OParsedData <- function(x) {
  expr <- paste("toupper(", x@key, ")", sep = "")
  res <- .h2o.__exec2(x@h2o, expr)
  res <- .h2o.exec2(res$dest_key, h2o = x@h2o, res$dest_key)
  res@logic <- FALSE
  return(res)
}

strsplit.H2OParsedData<-
function(x, split, fixed = FALSE, perl = FALSE, useBytes = FALSE) {
  if (missing(split)) split <- ' '
  if (split == "") stop("Empty split argument is unsupported")
  expr <- paste("strsplit(", paste(x@key, deparse(eval(split, envir = parent.frame())), sep = ","), ")", sep = "")
  res <- .h2o.__exec2(x@h2o, expr)
  res <- .h2o.exec2(res$dest_key, h2o = x@h2o, res$dest_key)
  res@logic <- FALSE
  return(res)
}

h2o.gsub <- function(pattern, replacement, x, ignore.case = FALSE) {
  expr <- paste("gsub(", paste(deparse(eval(pattern, envir = parent.frame())), deparse(eval(replacement, envir = parent.frame())), x@key, as.numeric(ignore.case), sep = ","), ")", sep = "")
  res <- .h2o.__exec2(x@h2o, expr)
  res <- .h2o.exec2(res$dest_key, h2o = x@h2o, res$dest_key)
  res@logic <- FALSE
  return(res)
}

h2o.sub <- function(pattern, replacement, x, ignore.case = FALSE) {
  expr <- paste("sub(", paste(deparse(eval(pattern, envir = parent.frame())), deparse(eval(replacement, envir = parent.frame())), x@key, as.numeric(ignore.case), sep = ","), ")", sep = "")
  res <- .h2o.__exec2(x@h2o, expr)
  res <- .h2o.exec2(res$dest_key, h2o = x@h2o, res$dest_key)
  res@logic <- FALSE
  return(res)
}

h2o.setLevel <- function(x, level) {
  expr <- paste("setLevel(", paste(x@key, deparse(level), sep = ","), ")", sep = "")
  res <- .h2o.__exec2(x@h2o, expr)
  res <- .h2o.exec2(res$dest_key, h2o = x@h2o, res$dest_key)
  res@logic <- FALSE
  res
}

trim <- function(x) {
  if (!inherits(x, "H2OParsedData")) stop("x must be an H2OParsedData object")
  .h2o.__unop2("trim", x)
}

h2o.sample <- function(data, nobs, seed = -1) {
    expr <- paste("sample(", paste(data@key, nobs, seed, sep = ","), ")", sep = "")
    print(expr)
    res <- .h2o.__exec2(data@h2o, expr)
    res <- .h2o.exec2(res$dest_key, h2o = data@h2o, res$dest_key)
    res
}

# setMethod("hist", "H2OParsedData", function(object))
hist.H2OParsedData <- function(x, freq = TRUE, ...){
  if(ncol(x) > 1) stop("object needs to be a single column H2OParsedData object")
  if(is.factor(x)) stop("object needs to numeric")
  if(!is.logical(freq)) stop("freq needs to be a boolean")
  res <- .h2o.__remoteSend(x@h2o, .h2o.__PAGE_SUMMARY2, source=x@key)
  summary <- res$summaries[[1]]
  rows = nrow(x)
  hstart = summary$hstart
  hstep = summary$hstep
  breaks = as.numeric(summary$hbrk)
  counts = summary$hcnt
  
  object = list()
  object$breaks = append(breaks,values = hstart - hstep, after = 0)
  object$counts = counts
  object$density = (counts/(rows*hstep))
  object$mids = breaks[1:length(breaks)-1]+(hstep/2)
  object$xname = names(x)
  object$equidist = freq
  class(object) = "histogram"
  plot(object)
  invisible(object)
}

quantile.H2OParsedData <- function(x, probs = seq(0, 1, 0.25), na.rm = FALSE, names = TRUE, type = 7, ...) {
  if((numCols = ncol(x)) != 1) stop("quantile only operates on a single column")
  if(is.factor(x)) stop("factors are not allowed")
  if(!na.rm && .h2o.__unop2("any.na", x)) stop("missing values and NaN's not allowed if 'na.rm' is FALSE")
  if(!is.numeric(probs)) stop("probs must be a numeric vector")
  if(any(probs < 0 | probs > 1)) stop("probs must fall in the range of [0,1]")
  if(type != 2 && type != 7) stop("type must be either 2 (mean interpolation) or 7 (linear interpolation)")
  if(type != 7) stop("Unimplemented: Only type 7 (linear interpolation) is supported from the console")
  
  myProbs <- paste("c(", paste(probs, collapse = ","), ")", sep = "")
  expr <- paste("quantile(", x@key, ",", myProbs, ")", sep = "")
  res <- .h2o.__exec2(x@h2o, expr)
  # res = .h2o.__remoteSend(x@h2o, .h2o.__PAGE_QUANTILES, source_key = x@key, column = 0, quantile = paste(probs, collapse = ","), interpolation_type = type, ...)
  # col <- as.numeric(strsplit(res$result, "\n")[[1]][-1])
  # if(numCols > .MAX_INSPECT_COL_VIEW)
  #   warning(x@key, " has greater than ", .MAX_INSPECT_COL_VIEW, " columns. This may take awhile...")
  # res2 = .h2o.__remoteSend(x@h2o, .h2o.__PAGE_INSPECT, key=res$dest_key, view=res$num_rows, max_column_display=.Machine$integer.max)
  # col <- sapply(res2$rows, function(x) { x[[2]] })
  col <- as.data.frame(new("H2OParsedData", h2o=x@h2o, key=res$dest_key))[[1]]
  if(names) names(col) <- paste(100*probs, "%", sep="")
  return(col)
}

# setMethod("summary", "H2OParsedData", function(object) {
summary.H2OParsedData <- function(object, ...) {
  digits <- 12L
  if(ncol(object) > .MAX_INSPECT_COL_VIEW)
    warning(object@key, " has greater than ", .MAX_INSPECT_COL_VIEW, " columns. This may take awhile...")
  res <- .h2o.__remoteSend(object@h2o, .h2o.__PAGE_SUMMARY2, source=object@key, max_ncols=.Machine$integer.max)
  cols <- sapply(res$summaries, function(col) {
    if(col$stats$type != 'Enum') { # numeric column
      if(is.null(col$stats$mins) || length(col$stats$mins) == 0) col$stats$mins = NaN
      if(is.null(col$stats$maxs) || length(col$stats$maxs) == 0) col$stats$maxs = NaN
      if(is.null(col$stats$pctile))
        params <- format(rep(signif(as.numeric(col$stats$mean), digits), 6), digits = 4)
      else
        params <- format(signif(as.numeric(c(
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
      top.ix <- sort.int(col$hcnt, decreasing=TRUE, index.return=TRUE)$ix[1:6]
      if(is.null(col$hbrk)) domains <- top.ix[1:6] else domains <- col$hbrk[top.ix]
      counts <- col$hcnt[top.ix]
      
      # TODO: Make sure "NA's" isn't a legal domain level
      if(!is.null(col$nacnt) && col$nacnt > 0) {
        idx <- ifelse(any(is.na(top.ix)), which(is.na(top.ix))[1], 6)
        domains[idx] <- "NA's"
        counts[idx] <- col$nacnt
      }
      
      # width <- max(cbind(nchar(domains), nchar(counts)))
      width <- c(max(nchar(domains)), max(nchar(counts)))
      result <- paste(domains,
                      sapply(domains, function(x) { ifelse(width[1] == nchar(x), "", paste(rep(' ', width[1] - nchar(x)), collapse='')) }),
                      ":", 
                      sapply(counts, function(y) { ifelse(width[2] == nchar(y), "", paste(rep(' ', width[2] - nchar(y)), collapse='')) }),
                      counts,
                      " ",
                      sep='')
      # result[is.na(top.ix)] <- NA
      result[is.na(domains)] <- NA
      result
    }
  })
  # Filter out rows with nothing in them
  cidx <- apply(cols, 1, function(x) { any(!is.na(x)) })
  if(ncol(cols) == 1) { cols <- as.matrix(cols[cidx,]) } else { cols <- cols[cidx,] }
  # cols <- as.matrix(cols[cidx,])

  result <- as.table(cols)
  rownames(result) <- rep("", nrow(result))
  colnames(result) <- sapply(res$summaries, function(col) col$colname)
  result
}

summary.H2OPCAModel <- function(object, ...) {
  # TODO: Save propVar and cumVar from the Java output instead of computing here
  myVar = object@model$sdev^2
  myProp = myVar/sum(myVar)
  result = rbind(object@model$sdev, myProp, cumsum(myProp))   # Need to limit decimal places to 4
  colnames(result) = paste("PC", seq(1, length(myVar)), sep="")
  rownames(result) = c("Standard deviation", "Proportion of Variance", "Cumulative Proportion")
  
  cat("Importance of components:\n")
  print(result)
}

screeplot.H2OPCAModel <- function(x, npcs = min(10, length(x@model$sdev)), type = "barplot", main = paste("h2o.prcomp(", x@data@key, ")", sep=""), ...) {
  if(type == "barplot")
    barplot(x@model$sdev[1:npcs]^2, main = main, ylab = "Variances", ...)
  else if(type == "lines")
    lines(x@model$sdev[1:npcs]^2, main = main, ylab = "Variances", ...)
  else
    stop("type must be either 'barplot' or 'lines'")
}

.canBeCoercedToLogical <- function(vec) {
  if(class(vec) != "H2OParsedData") stop("Object must be a H2OParsedData object. Input was: ", vec)
  # expects fr to be a vec.
  as.logical(.h2o.__unop2("canBeCoercedToLogical", vec))
}

.check.ifelse.conditions <-
function(test, yes, no, type) {
 if (type == "test") {
   return(class(test) == "H2OParsedData"
            && (is.numeric(yes) || class(yes) == "H2OParsedData" || is.logical(yes))
            && (is.numeric(no) || class(no) == "H2OParsedData" || is.logical(no))
            && (test@logic || .canBeCoercedToLogical(test)))
 }
}

setMethod("ifelse", signature(test="H2OParsedData", yes="ANY", no="ANY"), function(test, yes, no) {
  .h2o.ifelse(test,yes,no)
})

setMethod("ifelse", signature(test="ANY",yes="H2OParsedData", no="H2OParsedData"), function(test,yes,no) {
  .h2o.ifelse(test,yes,no)
})

.h2o.ifelse <- function(test,yes,no) {
  if (.check.ifelse.conditions(test, yes, no, "test")) {
    if (is.logical(yes)) yes <- as.numeric(yes)
    if (is.logical(no)) no <- as.numeric(no)
    return(.h2o.__multop2("ifelse", test, yes, no))

  } else if ( class(yes) == "H2OParsedData" && class(test) == "logical") {
    if (is.logical(yes)) yes <- as.numeric(yes)
    if (is.logical(no)) no <- as.numeric(no)
    return(.h2o.__multop2("ifelse", as.numeric(test), yes, no))

  } else if (class(no) == "H2OParsedData" && class(test) == "logical") {
    if (is.logical(yes)) yes <- as.numeric(yes)
    if (is.logical(no)) no <- as.numeric(no)
    return(.h2o.__multop2("ifelse", as.numeric(test), yes, no))
  }

  if( is(test, "H2OParsedData") ) {
    if( is.character(yes) ) yes <- deparse(yes)
    if( is.character(no)  ) no <- deparse(no)
    return(.h2o.__multop2("ifelse",test,yes,no))
  }

  if (is.atomic(test))  storage.mode(test) <- "logical"
  else if( isS4(test) ) test <- as(test, "logical")
  else                  test <- as.logical("test")
  ans <- test
  ok <- !(nas <- is.na(test))
  if (any(test[ok]))
      ans[test & ok] <- rep(yes, length.out = length(ans))[test &
          ok]
  if (any(!test[ok]))
      ans[!test & ok] <- rep(no, length.out = length(ans))[!test &
          ok]
  ans[nas] <- NA
  ans
}


#.getDomainMapping2 <- function(l, s = "") {
# if (is.list(l)) {
#   return( .getDomainMapping2( l[[length(l)]], s))
# }
# return(.getDomainMapping(eval(l), s)$map)
#}
#
#ifelse <- function(test,yes, no) if (inherits(test, "H2OParsedData") ||
#                                     inherits(no, "H2OParsedData")    ||
#                                     inherits(yes, "H2oParsedData")) UseMethod("ifelse") else base::ifelse(test, yes, no)
#
#ifelse.H2OParsedData <- function(test, yes, no) {
#  if (is.character(yes)) yes <- .getDomainMapping2(as.list(substitute(test)), yes)
#  if (is.character(no))  no  <- .getDomainMapping2(as.list(substitute(test)), no)
#  h2o.exec(ifelse(test, yes, no))
#}

#setMethod("ifelse", signature(test="H2OParsedData", yes="ANY", no="ANY"), function(test, yes, no) {
#  if(!(is.numeric(yes) || class(yes) == "H2OParsedData") || !(is.numeric(no) || class(no) == "H2OParsedData"))
#    stop("Unimplemented")
#  if(!test@logic && !.canBeCoercedToLogical(test)) stop(test@key, " is not a H2O logical data type")
#  h2o.exec(ifelse(test, yes, no))
##  .h2o.__multop2("ifelse", eval(test), yes, no)
#})
##
#setMethod("ifelse", signature(test="logical", yes="H2OParsedData", no="ANY"), function(test, yes, no) {
#  if(length(test) > 1) stop("test must be a single logical value")
#  h2o.exec(ifelse(test, yes, no))
##  .h2o.__multop2("ifelse", as.numeric(test), eval(yes), no)
#})
#
#setMethod("ifelse", signature(test="logical", yes="ANY", no="H2OParsedData"), function(test, yes, no) {
#  if(length(test) > 1) stop("test must be a single logical value")
#  h2o.exec(ifelse(test, yes, no))
##  .h2o.__multop2("ifelse", as.numeric(test), yes, eval(no))
#})
#
#setMethod("ifelse", signature(test="logical", yes="H2OParsedData", no="H2OParsedData"), function(test, yes, no) {
#  if(length(test) > 1) stop("test must be a single logical value")
#  h2o.exec(ifelse(test, yes, no))
##  .h2o.__multop2("ifelse", as.numeric(test), eval(yes), eval(no))
#})
#
setMethod("levels", "H2OParsedData", function(x) {
  # if(ncol(x) != 1) return(NULL)
  if(ncol(x) != 1) stop("Can only retrieve levels of one column.")
  res = .h2o.__remoteSend(x@h2o, .h2o.__HACK_LEVELS2, source = x@key, max_ncols = .Machine$integer.max)
  res$levels[[1]]
})

#----------------------------- Work in Progress -------------------------------#
# TODO: Need to change ... to environment variables and pass to substitute method,
#       Can't figure out how to access outside environment from within lapply
setMethod("apply", "H2OParsedData", function(X, MARGIN, FUN, ...) {
  if(missing(X) || class(X) != "H2OParsedData")
   stop("X must be a H2OParsedData object")
  if(missing(MARGIN) || !(length(MARGIN) <= 2 && all(MARGIN %in% c(1,2))))
    stop("MARGIN must be either 1 (rows), 2 (cols), or a vector containing both")
  if(missing(FUN) || !is.function(FUN))
    stop("FUN must be an R function")
  
  myList <- list(...)
  if(length(myList) > 0) {
    stop("Unimplemented")
    tmp = sapply(myList, function(x) { !class(x) %in% c("H2OParsedData", "numeric") } )
    if(any(tmp)) stop("H2O only recognizes H2OParsedData and numeric objects")
    
    idx = which(sapply(myList, function(x) { class(x) == "H2OParsedData" }))
    # myList <- lapply(myList, function(x) { if(class(x) == "H2OParsedData") x@key else x })
    myList[idx] <- lapply(myList[idx], function(x) { x@key })
    
    # TODO: Substitute in key name for H2OParsedData objects and push over wire to console
    if(any(names(myList) == ""))
      stop("Must specify corresponding variable names of ", myList[names(myList) == ""])
  }
  
  # Substitute in function name: FUN <- match.fun(FUN)
  myfun = deparse(substitute(FUN))
  len = length(myfun)
  if(len > 3 && substr(myfun[1], nchar(myfun[1]), nchar(myfun[1])) == "{" && myfun[len] == "}")
    myfun = paste(myfun[1], paste(myfun[2:(len-1)], collapse = ";"), "}")
  else
    myfun = paste(myfun, collapse = "")
  if(length(MARGIN) > 1)
    params = c(X@key, paste("c(", paste(MARGIN, collapse = ","), ")", sep = ""), myfun)
  else
    params = c(X@key, MARGIN, myfun)
  expr = paste("apply(", paste(params, collapse = ","), ")", sep="")
  res = .h2o.__exec2(X@h2o, expr)
  .h2o.exec2(res$dest_key, h2o = X@h2o, res$dest_key)
})

str.H2OParsedData <- function(object, ...) {
  if (length(l <- list(...)) && any("give.length" == names(l)))
    invisible(NextMethod("str", ...))
  else invisible(NextMethod("str", give.length = FALSE, ...))
  
  if(ncol(object) > .MAX_INSPECT_COL_VIEW)
    warning(object@key, " has greater than ", .MAX_INSPECT_COL_VIEW, " columns. This may take awhile...")
  res = .h2o.__remoteSend(object@h2o, .h2o.__PAGE_INSPECT2, src_key=object@key)
  cat("\nH2O dataset '", object@key, "':\t", res$numRows, " obs. of  ", (p <- res$numCols),
      " variable", if(p != 1) "s", if(p > 0) ":", "\n", sep = "")
  
  cc <- unlist(lapply(res$cols, function(y) y$name))
  width <- max(nchar(cc))
  rows <- res$rows[1:min(res$numRows, 10)]    # TODO: Might need to check rows > 0
  
  res2 = .h2o.__remoteSend(object@h2o, .h2o.__HACK_LEVELS2, source=object@key, max_ncols=.Machine$integer.max)
  for(i in 1:p) {
    cat("$ ", cc[i], rep(' ', width - nchar(cc[i])), ": ", sep = "")
    rhead <- sapply(rows, function(x) { x[i+1] })
    if(is.null(res2$levels[[i]]))
      cat("num  ", paste(rhead, collapse = " "), if(res$numRows > 10) " ...", "\n", sep = "")
    else {
      rlevels = res2$levels[[i]]
      cat("Factor w/ ", (count <- length(rlevels)), " level", if(count != 1) "s", ' "', paste(rlevels[1:min(count, 2)], collapse = '","'), '"', if(count > 2) ",..", ": ", sep = "")
      cat(paste(match(rhead, rlevels), collapse = " "), if(res$numRows > 10) " ...", "\n", sep = "")
    }
  }
}

setMethod("findInterval", "H2OParsedData", function(x, vec, rightmost.closed = FALSE, all.inside = FALSE) {
  if(any(is.na(vec)))
    stop("'vec' contains NAs")
  if(is.unsorted(vec))
    stop("'vec' must be sorted non-decreasingly")
  if(all.inside) stop("Unimplemented")
  
  myVec = paste("c(", .seq_to_string(vec), ")", sep = "")
  expr = paste("findInterval(", x@key, ",", myVec, ",", as.numeric(rightmost.closed), ")", sep = "")
  res = .h2o.__exec2(x@h2o, expr)

  new('H2OParsedData', h2o=x@h2o, key=res$dest_key)
})

# setGeneric("histograms", function(object) { standardGeneric("histograms") })
# setMethod("histograms", "H2OParsedData", function(object) {
#   if(ncol(object) > .MAX_INSPECT_COL_VIEW)
#     warning(object@key, " has greater than ", .MAX_INSPECT_COL_VIEW, " columns. This may take awhile...")
#   res = .h2o.__remoteSend(object@h2o, .h2o.__PAGE_SUMMARY2, source=object@key, max_ncols=.Machine$integer.max)
#   list.of.bins <- lapply(res$summaries, function(x) {
#     if (x$stats$type == 'Enum') {
#       bins <- NULL
#     } else {
#       counts <- x$hcnt
#       breaks <- seq(x$hstart, by=x$hstep, length.out=length(x$hcnt) + 1)
#       bins <- list(counts,breaks)
#       names(bins) <- cbind('counts', 'breaks')
#     }
#     bins
#   })
#   return(list.of.bins)
# })
