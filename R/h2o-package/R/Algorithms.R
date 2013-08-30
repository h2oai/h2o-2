# Model-building operations and algorithms
setGeneric("h2o.glm", function(x, y, data, family, nfolds = 10, alpha = 0.5, lambda = 1.0e-5) { standardGeneric("h2o.glm") })
setGeneric("h2o.kmeans", function(data, centers, cols = "", iter.max = 10) { standardGeneric("h2o.kmeans") })
setGeneric("h2o.randomForest", function(y, x_ignore = "", data, ntree, depth, classwt = as.numeric(NA)) { standardGeneric("h2o.randomForest") })
# setGeneric("h2o.randomForest", function(y, data, ntree, depth, classwt = as.numeric(NA)) { standardGeneric("h2o.randomForest") })
setGeneric("h2o.getTree", function(forest, k, plot = FALSE) { standardGeneric("h2o.getTree") })
setGeneric("h2o.glmgrid", function(x, y, data, family, nfolds = 10, alpha = c(0.25,0.5), lambda = 1.0e-5) { standardGeneric("h2o.glmgrid") })

setMethod("h2o.glm", signature(x="character", y="character", data="H2OParsedData", family="character", nfolds="numeric", alpha="numeric", lambda="numeric"),
          function(x, y, data, family, nfolds, alpha, lambda) {
            # res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLM, key = data@key, y = y, x = paste(x, sep="", collapse=","), family = family, n_folds = nfolds, alpha = alpha, lambda = lambda)
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLM, key = data@key, y = y, x = paste(x, sep="", collapse=","), family = family, n_folds = nfolds, alpha = alpha, lambda = lambda, case_mode="=", case=1.0)
            # while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
            while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
            destKey = res$destination_key
            res = h2o.__remoteSend(data@h2o, h2o.__PAGE_INSPECT, key=res$destination_key)
            res = res$GLMModel
            
            # Put model results in a pretty format
            parseGLMResults = function(res) {
              # Parameters matching those in R
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
            
              # Additional parameters from H2O
              result$auc = res$validations[[1]]$auc
              result$training.err = res$validations[[1]]$err
              result$threshold = res$validations[[1]]$threshold
            
              # Build confusion matrix
              if(family == "binomial") {
                temp = res$validations[[1]]$cm
                temp[[1]] = NULL; temp = lapply(temp, function(x) { x[[1]] = NULL; x })
                result$confusion = t(matrix(unlist(temp), nrow = length(temp)))
                myNames = c("False", "True", "Error")
                dimnames(result$confusion) = list(Actual = myNames, Predicted = myNames)
              }
              result
            }
            
            # Save cross-validation models as separate objects in a list
            xtemp = res$validations[[1]]$xval_models
            if(is.null(xtemp)) cross = list()    # Should this be empty list or null when nfolds = 0?
            else {
              cross = lapply(1:length(xtemp), function(i) {
                xres = h2o.__remoteSend(data@h2o, h2o.__PAGE_INSPECT, key=xtemp[i])
                new("H2OGLMModel", key=xtemp[i], data=data, model=parseGLMResults(xres$GLMModel), xval=list())
              })
            }
            
            resGLMModel = new("H2OGLMModel", key=destKey, data=data, model=parseGLMResults(res), xval=cross)
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

setMethod("predict", signature(object="H2OGLMModel"), 
        function(object) {
          res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_PREDICT, model_key=object@key, data_key=object@data@key)
          res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_INSPECT, key=res$response$redirect_request_args$key)
          result = new("H2OParsedData", h2o=object@data@h2o, key=res$key)
          result
        })


setMethod("h2o.glmgrid", signature(x="character", y="character", data="H2OParsedData", family="character", nfolds="numeric", alpha="numeric", lambda="numeric"),
          function(x, y, data, family, nfolds, alpha, lambda) {
            
	    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLMGrid, key = data@key, y = y, x = paste(x, sep="", collapse=","), family = family, 		    n_folds = nfolds, alpha = alpha, lambda = lambda, case_mode="=",case=1.0,parallel= 1 )
            while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { 
		Sys.sleep(1) 
	    }
	    destKey = res$destination_key
	    res=h2o.__remoteSend(data@h2o, h2o.__PAGE_GLMGridProgress, destination_key=res$destination_key)
	    	result = list()
		result$Summary = t(sapply(res$models,c))
#            result=rep( list(list()),length(res$models)+1  ) 
#            result[[length(result)]]$Summary = t(sapply(res$models,c))
		
#		for(i in 1:length(res$models)){
#			resH=h2o.__remoteSend(data@h2o, "Inspect.json",key=res$models[[i]]$key)
#			resH = resH$GLMModel
#			    result[[i]]$LSMParams=unlist(resH$LSMParams)
#	            result[[i]]$coefficients = unlist(resH$coefficients)
#	            result[[i]]$normalized_coefficients = unlist(resH$normalized_coefficients)
#            		result[[i]]$dof = resH$dof            
#			result[[i]]$null.deviance = resH$validations[[1]]$nullDev
#			result[[i]]$deviance = resH$validations[[1]]$resDev
#			result[[i]]$aic = resH$validations[[1]]$aic
#			result[[i]]$auc = resH$validations[[1]]$auc
#			result[[i]]$iter = resH$iterations
#		        result[[i]]$threshold = resH$validations[[1]]$threshold
#	                result[[i]]$error_table = t(sapply(resH$validations[[1]]$cm,c))
#
#            }
	    resGLMGridModel = new("H2OGLMGridModel", key=destKey, data=data, model=result)
            resGLMGridModel
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
