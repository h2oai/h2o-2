# Model-building operations and algorithms
setGeneric("h2o.glm", function(x, y, data, family, nfolds = 10, alpha = 0.5, lambda = 1.0e-5, tweedie.p=ifelse(family=='tweedie', 1.5, NA)) { standardGeneric("h2o.glm") })
setGeneric("h2o.glmgrid", function(x, y, data, family, nfolds = 10, alpha = c(0.25,0.5), lambda = 1.0e-5) { standardGeneric("h2o.glmgrid") })
setGeneric("h2o.kmeans", function(data, centers, cols = "", iter.max = 10) { standardGeneric("h2o.kmeans") })
setGeneric("h2o.prcomp", function(data, tol = 0, standardize = TRUE) { standardGeneric("h2o.prcomp") })
setGeneric("h2o.randomForest", function(y, x_ignore = "", data, ntree, depth, classwt = as.numeric(NA)) { standardGeneric("h2o.randomForest") })
# setGeneric("h2o.randomForest", function(y, data, ntree, depth, classwt = as.numeric(NA)) { standardGeneric("h2o.randomForest") })
setGeneric("h2o.getTree", function(forest, k, plot = FALSE) { standardGeneric("h2o.getTree") })
setGeneric("h2o.gbm", function(data, destination, y, x_ignore = "", ntrees = 10, max_depth=8, learn_rate=.2, min_rows=10) { standardGeneric("h2o.gbm") })
setGeneric("h2o.gbmgrid", function(data, destination, y, x_ignore = as.numeric(NA), ntrees = c(10,100), max_depth=c(1,5,10), learn_rate=c(0.01,0.1,0.2), min_rows=10) { standardGeneric("h2o.gbmgrid") })
setGeneric("h2o.predict", function(object, newdata) { standardGeneric("h2o.predict") })

setMethod("h2o.gbm", signature(data="H2OParsedData", destination="character", y="character", x_ignore="numeric", ntrees="numeric", max_depth="numeric", learn_rate="numeric", min_rows="numeric"),
          function(data, destination, y, x_ignore, ntrees, max_depth, learn_rate, min_rows) {
      # ignoredFeat = ifelse(length(x_ignore) == 1 && is.na(x_ignore), "", paste(x_ignore - 1, sep="", collapse=","))
      if(length(x_ignore) == 1 && any(is.na(x_ignore) == TRUE))
        ignoredFeat = ""
      else if(min(x_ignore) < 1 || max(x_ignore) > ncol(data))
        stop("Column index out of bounds!")
      else
        ignoredFeat = paste(x_ignore - 1, sep="", collapse=",")
      
      res=h2o.__remoteSend(data@h2o, h2o.__PAGE_GBM, destination_key=destination, source=data@key, vresponse=y, ignored_cols=ignoredFeat, ntrees=ntrees, max_depth=max_depth, learn_rate=learn_rate, min_rows=min_rows)
      while(h2o.__poll(data@h2o, res$job_key) != -1) { Sys.sleep(1) }
	    res2=h2o.__remoteSend(data@h2o, h2o.__PAGE_GBMModelView,'_modelKey'=destination)
      
	    result=list()
	    categories=length(res2$gbm_model$cm)
	    cf_matrix = t(matrix(unlist(res2$gbm_model$cm),nrow=categories ))
	    colnames(cf_matrix)=c(1:categories)
	    rownames(cf_matrix)=c(1:categories)
	    result$confusion= cf_matrix
      
	    # mse_matrix=matrix(unlist(res2$gbm_model$errs),ncol=ntrees)
	    # colnames(mse_matrix)=c(1:ntrees)
	    # rownames(mse_matrix)="MSE"
	    # result$err=mse_matrix
      result$err = res2$gbm_model$errs
	    resGBM=new("H2OGBMModel", key=destination, data=data, model=result)
	    resGBM
	})

setMethod("h2o.gbm", signature(data="H2OParsedData", destination="character", y="character", x_ignore="character", ntrees="numeric", max_depth="numeric", learn_rate="numeric", min_rows="numeric"),
          function(data, destination, y, x_ignore, ntrees, max_depth, learn_rate, min_rows) {
            if(length(x_ignore) == 1 && x_ignore == "")
              h2o.gbm(data, destination, y, as.numeric(NA), ntrees, max_depth, learn_rate, min_rows)
            else if(any(c(y, x_ignore) %in% colnames(data) == FALSE))
              stop("Column name does not exist!")
            else {
              myCol = colnames(data); myCol = myCol[-which(myCol == y)]
              myIgnore = which(myCol %in% x_ignore)
              h2o.gbm(data, destination, y, myIgnore, ntrees, max_depth, learn_rate, min_rows)
            }
          })

setMethod("h2o.gbm", signature(data="H2OParsedData", destination="character",y="character",x_ignore="ANY",ntrees="ANY", max_depth="ANY", learn_rate="ANY", min_rows="ANY"),
          function(data, destination, y, x_ignore, ntrees, max_depth, learn_rate, min_rows) {
            if(!(missing(x_ignore) || class(x_ignore) == "numeric" || class(x_ignore) == "character"))
              stop(paste("ignore cannot be of class", class(x_ignore)))
            else if(!(missing(ntrees) || class(ntrees) == "numeric"))
              stop(paste("ntrees cannot be of class", class(ntrees)))
            else if(!(missing(max_depth) || class(max_depth) == "numeric"))
              stop(paste("max_depth cannot be of class", class(max_depth)))
            else if(!(missing(learn_rate) || class(learn_rate) == "numeric"))
              stop(paste("learn_rate cannot be of class", class(learn_rate)))
	          else if(!(missing(min_rows) || class(min_rows) == "numeric"))
              stop(paste("min_rows cannot be of class", class(min_rows)))
            h2o.gbm(data, destination, y, x_ignore, ntrees, max_depth, learn_rate, min_rows) 
          })

setMethod("h2o.gbmgrid", signature(data="H2OParsedData", destination="character", y="character", x_ignore="numeric", ntrees="numeric", max_depth="numeric", learn_rate="numeric", min_rows="numeric"),
          function(data, destination, y, x_ignore, ntrees, max_depth, learn_rate, min_rows) {
            ignoredFeat = ifelse(length(x_ignore) == 1 && is.na(x_ignore), "", paste(x_ignore, sep="", collapse=","))
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GBMGrid, destination_key=destination, source=data@key, vresponse=y, ignored_cols=ignoredFeat, ntrees=ntrees, max_depth=max_depth, learn_rate=learn_rate, min_rows=min_rows, nbins=1024)
            while(h2o.__poll(data@h2o, res$job_key) != -1) { Sys.sleep(1) }
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_INSPECT, key=destination)
            
            # Create list of individual GBM models
            myModels = list()
            resGrid = res$rows
            for(i in 1:length(resGrid)) {
              destKey = resGrid[[i]]$'model key'
              resModel = h2o.__remoteSend(data@h2o, h2o.__PAGE_GBMModelView, '_modelKey'=destKey)
              
              result = list()
              categories = length(resModel$gbm_model$cm)
              cf_matrix = t(matrix(unlist(resModel$gbm_model$cm), nrow = categories))
              colnames(cf_matrix) = c(1:categories)
              rownames(cf_matrix) = c(1:categories)
              result$confusion = cf_matrix
              
              # ntreesModel = resGrid[[i]]$ntrees
              # mse_matrix = matrix(unlist(resModel$gbm_model$errs), ncol = ntreesModel)
              # colnames(mse_matrix) = c(1:ntreesModel)
              # rownames(mse_matrix) = "MSE"
              # result$err = mse_matrix
              result$err = resModel$gbm_model$errs
              myModels[[i]] = new("H2OGBMModel", key=destKey, data=data, model=result)
            }
            resGBMGrid = new("H2OGBMGrid", key=destination, data=data, models=myModels, sumtable=resGrid)
            resGBMGrid
          })

setMethod("h2o.gbmgrid", signature(data="H2OParsedData", destination="character",y="character", x_ignore="ANY", ntrees="ANY", max_depth="ANY", learn_rate="ANY", min_rows="ANY"),
          function(data, destination, y, x_ignore, ntrees, max_depth, learn_rate, min_rows) {
            if(!(missing(x_ignore) || class(x_ignore) == "numeric"))
              stop(paste("ignore cannot be of class", class(x_ignore)))
            else if(!(missing(ntrees) || class(ntrees) == "numeric"))
              stop(paste("ntrees cannot be of class", class(ntrees)))
            else if(!(missing(max_depth) || class(max_depth) == "numeric"))
              stop(paste("max_depth cannot be of class", class(max_depth)))
            else if(!(missing(learn_rate) || class(learn_rate) == "numeric"))
              stop(paste("learn_rate cannot be of class", class(learn_rate)))
            else if(!(missing(min_rows) || class(min_rows) == "numeric"))
              stop(paste("min_rows cannot be of class", class(min_rows)))
            h2o.gbmgrid(data, destination, y, x_ignore, ntrees, max_depth, learn_rate, min_rows) 
          })

# internally called glm to allow games with method dispatch
h2o.glm.internal <- function(x, y, data, family, nfolds, alpha, lambda, tweedie.p) {
            if( family == 'tweedie' ){
                if ( tweedie.p < 1 || tweedie.p > 2 )
                    stop('tweedie.p must be in (1,2)')
            } else {
                if( !(missing(tweedie.p) || is.na(tweedie.p) ) )
                    stop('tweedie.p may only be set for family tweedie')
            }

            if( family != 'tweedie' )
                res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLM, key = data@key, y = y, x = paste(x, sep="", collapse=","), family = family, n_folds = nfolds, alpha = alpha, lambda = lambda, case_mode="=", case=1.0)
            else
                res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLM, key = data@key, y = y, x = paste(x, sep="", collapse=","), family = family, n_folds = nfolds, alpha = alpha, lambda = lambda, case_mode="=", case=1.0, tweedie_power=tweedie.p)
            while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
            destKey = res$destination_key
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_INSPECT, key=destKey)
            resModel = res$GLMModel
            modelOrig = h2o.__getGLMResults(resModel, y, family, tweedie.p)
            
            # Get results from cross-validation
            if(nfolds < 2)
              return(new("H2OGLMModel", key=destKey, data=data, model=modelOrig, xval=list()))
            
            res_xval = list()
            for(i in 1:nfolds) {
              xvalKey = resModel$validations[[1]]$xval_models[i]
              res = h2o.__remoteSend(data@h2o, h2o.__PAGE_INSPECT, key=xvalKey)
              modelXval = h2o.__getGLMResults(res$GLMModel, y, family, tweedie.p)
              res_xval[[i]] = new("H2OGLMModel", key=xvalKey, data=data, model=modelXval, xval=list())
            }
            resGLMModel = new("H2OGLMModel", key=destKey, data=data, model=modelOrig, xval=res_xval)
            resGLMModel
}

# Pretty formatting of H2O GLM results
h2o.__getGLMResults <- function(res, y, family, tweedie.p) {
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
  result$train.err = res$validations[[1]]$err
  result$y = y
  result$x = res$column_names
  result$tweedie.p = ifelse(missing(tweedie.p), 'NA', tweedie.p)
  
  if(family == "binomial") {
    result$auc = res$validations[[1]]$auc
    result$threshold = res$validations[[1]]$threshold
    result$class.err = res$validations[[1]]$classErr
    
    # Construct confusion matrix
    temp = t(data.frame(sapply(res$validations[[1]]$cm, c)))
    dn = list(Actual = temp[-1,1], Predicted = temp[1,-1])
    temp = temp[-1,]; temp = temp[,-1]
    dimnames(temp) = dn
    result$cm = temp
  }
  return(result)
}

setMethod("h2o.glm", signature(x="character", y="character", data="H2OParsedData", family="character", nfolds="ANY", alpha="ANY", lambda="ANY", tweedie.p="ANY"),
  function(x, y, data, family, nfolds = 10, alpha = 0.5, lambda = 1.0e-5, tweedie.p=ifelse(family=='tweedie', 1.5, NA)) {
            if(!(missing(nfolds) || class(nfolds) == "numeric"))
              stop(paste("nfolds cannot be of class", class(nfolds)))
            else if(!(missing(alpha) || class(alpha) == "numeric"))
              stop(paste("alpha cannot be of class", class(alpha)))
            else if(!(missing(lambda) || class(lambda) == "numeric"))
              stop(paste("lambda cannot be of class", class(lambda)))

            if ( !(missing( tweedie.p ) || class( tweedie.p ) == 'numeric' ) )
              stop(paste('tweedie.p cannot be of class', class(tweedie.p)))

            if( missing(tweedie.p) && family != 'tweedie' )
                h2o.glm.internal(x, y, data, family, nfolds, alpha, lambda) 
            else 
                h2o.glm.internal(x, y, data, family, nfolds, alpha, lambda, tweedie.p) 
          })

setMethod("h2o.glmgrid", signature(x="character", y="character", data="H2OParsedData", family="character", nfolds="numeric", alpha="numeric", lambda="numeric"),
          function(x, y, data, family, nfolds, alpha, lambda) {
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLMGrid, key = data@key, y = y, x = paste(x, sep="", collapse=","), family = family, n_folds = nfolds, alpha = alpha, lambda = lambda, case_mode="=", case=1.0, parallel= 1 )
            while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
            destKey = res$destination_key
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLMGridProgress, destination_key=res$destination_key)
            allModels = res$models
            
            result = list()
            tweedie.p = "NA"
            # result$Summary = t(sapply(res$models,c))
            for(i in 1:length(allModels)) {
              resH = h2o.__remoteSend(data@h2o, h2o.__PAGE_INSPECT, key=allModels[[i]]$key)
              modelOrig = h2o.__getGLMResults(resH$GLMModel, y, family, tweedie.p)
              
              res_xval = list()
              for(j in 1:nfolds) {
                xvalKey = resH$GLMModel$validations[[1]]$xval_models[j]
                resX = h2o.__remoteSend(data@h2o, h2o.__PAGE_INSPECT, key=xvalKey)
                modelXval = h2o.__getGLMResults(resX$GLMModel, y, family, tweedie.p)
                res_xval[[j]] = new("H2OGLMModel", key=xvalKey, data=data, model=modelXval, xval=list())
              }
              result[[i]] = new("H2OGLMModel", key=allModels[[i]]$key, data=data, model=modelOrig, xval=res_xval)
            }
            
            # temp = data.frame(t(sapply(allModels, c)))
            resGLMGrid = new("H2OGLMGrid", key=destKey, data=data, models=result, sumtable=allModels)
            resGLMGrid
          })

setMethod("h2o.glmgrid", signature(x="character", y="character", data="H2OParsedData", family="character", nfolds="ANY", alpha="ANY", lambda="ANY"),
          function(x, y, data, family, nfolds, alpha, lambda) {
            if(!(missing(nfolds) || class(nfolds) == "numeric"))
              stop(paste("nfolds cannot be of class", class(nfolds)))
            else if(!(missing(alpha) || class(alpha) == "numeric"))
              stop(paste("alpha cannot be of class", class(alpha)))
            else if(!(missing(lambda) || class(lambda) == "numeric"))
              stop(paste("lambda cannot be of class", class(lambda)))
            h2o.glmgrid(x, y, data, family, nfolds, alpha, lambda) 
          })

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

setMethod("h2o.prcomp", signature(data="H2OParsedData", tol="numeric", standardize="logical"), 
          function(data, tol, standardize) {
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_PCA, key=data@key, tolerance=tol, standardize=as.numeric(standardize))
            while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
            destKey = res$destination_key
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_INSPECT, key=destKey)
            res = res$PCAModel
            
            result = list()
            result$standardized = standardize
            result$sdev = as.numeric(unlist(res$stdDev))
            # result$rotation = do.call(rbind, res$eigenvectors)
            # temp = t(do.call(rbind, res$eigenvectors))
            nfeat = length(res$eigenvectors[[1]])
            temp = matrix(unlist(res$eigenvectors), nrow = nfeat)
            rownames(temp) = names(res$eigenvectors[[1]])
            colnames(temp) = paste("PC", seq(1, ncol(temp)), sep="")
            result$rotation = temp
            
            new("H2OPCAModel", key=destKey, data=data, model=result)
          })

setMethod("h2o.prcomp", signature(data="H2OParsedData", tol="ANY", standardize="ANY"), 
          function(data, tol, standardize) {
            if(!(missing(tol) || class(tol) == "numeric"))
              stop("tol cannot be of class", class(tol))
            if(!(missing(standardize) || class(standardize) == "logical"))
              stop("standardize cannot be of class", class(standardize))
            h2o.prcomp(data, tol, standardize)
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
            cf_matrix = cbind(t(matrix(unlist(cf$scores), nrow=length(cf$header))), unlist(cf$classes_errors))
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

setMethod("h2o.predict", signature(object="H2OModel", newdata="H2OParsedData"),
          function(object, newdata) {
            if(class(object) == "H2OGLMModel" || class(object) == "H2ORForestModel") {
              res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_PREDICT, model_key=object@key, data_key=newdata@key)
              res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_INSPECT, key=res$response$redirect_request_args$key)
              new("H2OParsedData", h2o=object@data@h2o, key=res$key)
            } else if(class(object) == "H2OKMeansModel") {
              res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_KMAPPLY, model_key=object@key, data_key=newdata@key)
              while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
              new("H2OParsedData", h2o=object@data@h2o, key=res$key)
            # } else if(class(object) == "H2OPCAModel") {
            #  numMatch = colnames(newdata) %in% colnames(object@data)
            #  numPC = length(numMatch[numMatch == TRUE])
            #  res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_PCASCORE, model_key=object@key, key=newdata@key, num_pc=numPC)
            #  while(h2o.__poll(object@data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
            #  res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_INSPECT2, key=res$response$redirect_request_args$key)
            #  new("H2OParsedData2", h2o=object@data@h2o, key=res$key)
            } else
              stop(paste("Prediction has not yet been implemented for", class(object)))
          })

setMethod("h2o.predict", signature(object="H2OModel", newdata="missing"), 
          function(object) { h2o.predict(object, object@data) })