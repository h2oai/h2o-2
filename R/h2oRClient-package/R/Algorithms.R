# Model-building operations and algorithms
setGeneric("h2o.glm", function(x, y, data, family, nfolds = 10, alpha = 0.5, lambda = 1.0e-5, epsilon = 1e-5, standardize = TRUE, tweedie.p=ifelse(family=='tweedie', 1.5, NA)) { standardGeneric("h2o.glm") })
# setGeneric("h2o.glmgrid", function(x, y, data, family, nfolds = 10, alpha = c(0.25,0.5), lambda = 1.0e-5) { standardGeneric("h2o.glmgrid") })
setGeneric("h2o.glm.FV", function(x, y, data, family, nfolds = 10, alpha = 0.5, lambda = 1.0e-5, tweedie.p=ifelse(family=='tweedie', 0, NA)) { standardGeneric("h2o.glm.FV") })
setGeneric("h2o.kmeans", function(data, centers, cols = "", iter.max = 10) { standardGeneric("h2o.kmeans") })
setGeneric("h2o.prcomp", function(data, tol = 0, standardize = TRUE, retx = FALSE) { standardGeneric("h2o.prcomp") })
setGeneric("h2o.pcr", function(x, y, data, ncomp, family, nfolds = 10, alpha = 0.5, lambda = 1.0e-5, tweedie.p = ifelse(family=="tweedie", 0, NA)) { standardGeneric("h2o.pcr") })
setGeneric("h2o.randomForest", function(x, y, data, ntree = 50, depth = 2147483647, classwt = as.numeric(NA)) { standardGeneric("h2o.randomForest") })
setGeneric("h2o.getTree", function(forest, k, plot = FALSE) { standardGeneric("h2o.getTree") })
setGeneric("h2o.gbm", function(x, y, distribution = "multinomial", data, n.trees = 10, interaction.depth = 8, n.minobsinnode = 10, shrinkage = 0.2) { standardGeneric("h2o.gbm") })
# setGeneric("h2o.gbmgrid", function(x, y, data, n.trees = c(10,100), interaction.depth = c(1,5,10), n.minobsinnode = 10, shrinkage = c(0.01,0.1,0.2)) { standardGeneric("h2o.gbmgrid") })
setGeneric("h2o.predict", function(object, newdata) { standardGeneric("h2o.predict") })

#----------------------- Generalized Boosting Machines (GBM) -----------------------#
setMethod("h2o.gbm", signature(x="numeric", y="numeric", distribution="character", data="H2OParsedData", n.trees="numeric", interaction.depth="numeric", n.minobsinnode="numeric", shrinkage="numeric"),
   function(x, y, distribution, data, n.trees, interaction.depth, n.minobsinnode, shrinkage) {
      if (length(x) < 1) stop("GBM requires at least one explanatory variable")
      if(any( x < 1 | x > ncol(data))) stop(paste('Out of range explanatory variable', paste(x[which(x < 1 || x > ncol(data))], collapse=',')))
      if( y < 1 || y > ncol(data) ) stop(paste('Response variable index', y, 'is out of range'))
      if( y %in% x ) stop(paste(colnames(data)[y], 'is both an explanatory and dependent variable'))
      x <- x - 1
      cols=paste(x,collapse=',')

      # if( missing(distribution) )
      #  distribution <- 'multinomial'
      if( !(distribution %in% c('multinomial', 'gaussian')) )
        stop(paste(distribution, "is not a valid distribution; only [multinomial, gaussian] are supported"))
      classification <- ifelse(distribution == 'multinomial', 1, ifelse(distribution=='gaussian', 0, -1))
    
      destKey = paste("__GBMModel_", UUIDgenerate(), sep="")
      res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GBM, destination_key=destKey, source=data@key, response=colnames(data)[y], cols=cols, ntrees=n.trees, max_depth=interaction.depth, learn_rate=shrinkage, min_rows=n.minobsinnode, classification=classification)
      while(h2o.__poll(data@h2o, res$job_key) != -1) { Sys.sleep(1) }
      res2 = h2o.__remoteSend(data@h2o, h2o.__PAGE_GBMModelView, '_modelKey'=destKey)
    
      result=list()
      categories=length(res2$gbm_model$cm)
      cf_matrix = t(matrix(unlist(res2$gbm_model$cm),nrow=categories ))
      cf_names <- res2$gbm_model[['_domains']]
      cf_names <- cf_names[[ length(cf_names) ]]

      # colnames(cf_matrix) <- cf_names
      # rownames(cf_matrix) <- cf_names
      dimnames(cf_matrix) = list(Actual = cf_names, Predicted = cf_names)
      result$cm = cf_matrix
      
      # mse_matrix=matrix(unlist(res2$gbm_model$errs),ncol=n.trees)
      # colnames(mse_matrix)=c(1:n.trees)
      # rownames(mse_matrix)="MSE"
      # result$err=mse_matrix
      result$err = res2$gbm_model$errs
      new("H2OGBMModel", key=destKey, data=data, model=result)
	})

setMethod("h2o.gbm", signature(x="numeric", y="character", distribution='ANY', data="H2OParsedData", n.trees="numeric", interaction.depth="numeric", n.minobsinnode="numeric", shrinkage="numeric"),
    function(x, y, distribution, data, n.trees, interaction.depth, n.minobsinnode, shrinkage) {
      cc <- colnames( data )
      if( !(y %in% cc) ) stop(paste(y, 'is not a valid column name'))
      y_i <- which(y==cc)
      h2o.gbm(x, y_i, distribution, data, n.trees, interaction.depth, n.minobsinnode, shrinkage)
  })

setMethod("h2o.gbm", signature(x="character", y="character", distribution='ANY', data="H2OParsedData", n.trees="numeric", interaction.depth="numeric", n.minobsinnode="numeric", shrinkage="numeric"),
    function(x, y, distribution, data, n.trees, interaction.depth, n.minobsinnode, shrinkage) {
      cc <- colnames( data )
      if( y %in% x ) stop(paste(y, 'is both an explanatory and dependent variable'))
      if(any(!(x %in% cc))) stop(paste(paste(x[which(!(x %in% cc))], collapse=','), 'is not a valid column name'))
      x_i = match(x, cc)
      h2o.gbm(x_i, y, distribution, data, n.trees, interaction.depth, n.minobsinnode, shrinkage)
  })

setMethod("h2o.gbm", signature(x="ANY", y="character", distribution='ANY', data="H2OParsedData", n.trees="ANY", interaction.depth="ANY", n.minobsinnode="ANY", shrinkage="ANY"),
   function(x, y, distribution, data, n.trees, interaction.depth, n.minobsinnode, shrinkage) {
      if( !(distribution %in% c('multinomial', 'gaussian')) )
        stop(paste(distribution, "is not a valid distribution; only [multinomial, gaussian] are supported"))
      if(!(missing(x) || class(x) == "numeric" || class(x) == "character"))
         stop(paste("x cannot be of class", class(x)))
      else if(!(missing(n.trees) || class(n.trees) == "numeric"))
         stop(paste("n.trees cannot be of class", class(n.trees)))
      else if(!(missing(interaction.depth) || class(interaction.depth) == "numeric"))
         stop(paste("interaction.depth cannot be of class", class(interaction.depth)))
      else if(!(missing(shrinkage) || class(shrinkage) == "numeric"))
         stop(paste("shrinkage cannot be of class", class(shrinkage)))
      else if(!(missing(n.minobsinnode) || class(n.minobsinnode) == "numeric"))
         stop(paste("n.minobsinnode cannot be of class", class(n.minobsinnode)))
      if(missing(x)) x = setdiff(colnames(data), y)
      h2o.gbm(x, y, distribution, data, n.trees, interaction.depth, n.minobsinnode, shrinkage)
  })

setMethod("h2o.gbm", signature(x="ANY", y="numeric", distribution='ANY', data="H2OParsedData", n.trees="ANY", interaction.depth="ANY", n.minobsinnode="ANY", shrinkage="ANY"),
    function(x, y, distribution, data, n.trees, interaction.depth, n.minobsinnode, shrinkage) {
      if( y < 1 || y > ncol( data ) ) stop(paste(y, 'is not a valid column index'))
      h2o.gbm(x, colnames(data)[y], distribution, data, n.trees, interaction.depth, n.minobsinnode, shrinkage)
    })

#----------------------------- Generalized Linear Models (GLM) ---------------------------#
# Internally called glm to allow games with method dispatch
h2o.glm.internal <- function(x, y, data, family, nfolds, alpha, lambda, expert_settings, beta_epsilon, standardize, tweedie.p) {
      if(family == 'tweedie' && (tweedie.p < 1 || tweedie.p > 2 ))
          stop('tweedie.p must be in (1,2)')
      if(family != "tweedie" && !(missing(tweedie.p) || is.na(tweedie.p) ) )
          stop('tweedie.p may only be set for family tweedie')

      if(family != 'tweedie')
          res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLM, key=data@key, y=y, x=paste(x, sep="", collapse=","), family=family, n_folds=nfolds, alpha=alpha, lambda=lambda, expert_settings=expert_settings, beta_epsilon=beta_epsilon, standardize=as.numeric(standardize), case_mode="=", case=1.0)
      else
          res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLM, key=data@key, y=y, x=paste(x, sep="", collapse=","), family=family, n_folds=nfolds, alpha=alpha, lambda=lambda, expert_settings=expert_settings, beta_epsilon=beta_epsilon, standardize=as.numeric(standardize), case_mode="=", case=1.0, tweedie_power=tweedie.p)
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
      new("H2OGLMModel", key=destKey, data=data, model=modelOrig, xval=res_xval)
}

h2o.glmgrid.internal <- function(x, y, data, family, nfolds, alpha, lambda) {
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
        
        if(nfolds < 2)
          result[[i]] = new("H2OGLMModel", key=allModels[[i]]$key, data=data, model=modelOrig, xval=list())
        else {
          res_xval = list()
          for(j in 1:nfolds) {
            xvalKey = resH$GLMModel$validations[[1]]$xval_models[j]
            resX = h2o.__remoteSend(data@h2o, h2o.__PAGE_INSPECT, key=xvalKey)
            modelXval = h2o.__getGLMResults(resX$GLMModel, y, family, tweedie.p)
            res_xval[[j]] = new("H2OGLMModel", key=xvalKey, data=data, model=modelXval, xval=list())
          }
          result[[i]] = new("H2OGLMModel", key=allModels[[i]]$key, data=data, model=modelOrig, xval=res_xval)
        }
      }
      # temp = data.frame(t(sapply(allModels, c)))
      new("H2OGLMGrid", key=destKey, data=data, models=result, sumtable=allModels)
}

# Pretty formatting of H2O GLM results
h2o.__getGLMResults <- function(res, y, family, tweedie.p) {
      result = list()
      result$coefficients = unlist(res$coefficients)
      result$normalized_coefficients = unlist(res$normalized_coefficients)
      result$rank = res$nCols
      result$family = h2o.__getFamily(family, tweedie.var.p = tweedie.p)
      result$deviance = res$validations[[1]]$resDev
      result$aic = res$validations[[1]]$aic
      result$null.deviance = res$validations[[1]]$nullDev
      result$iter = res$iterations
      result$df.residual = res$dof
      result$df.null = res$dof + result$rank
      result$train.err = res$validations[[1]]$err
      result$y = y
      result$x = res$column_names
      # result$tweedie.p = ifelse(missing(tweedie.p), 'NA', tweedie.p)
      
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

setMethod("h2o.glm", signature(x="character", y="character", data="H2OParsedData", family="character", nfolds="ANY", alpha="ANY", lambda="ANY", epsilon="ANY", standardize="ANY", tweedie.p="ANY"),
    function(x, y, data, family, nfolds, alpha, lambda, epsilon, standardize, tweedie.p) {
      if(!(missing(nfolds) || class(nfolds) == "numeric"))
        stop(paste("nfolds cannot be of class", class(nfolds)))
      if(!(missing(alpha) || class(alpha) == "numeric"))
        stop(paste("alpha cannot be of class", class(alpha)))
      if(!(missing(lambda) || class(lambda) == "numeric"))
        stop(paste("lambda cannot be of class", class(lambda)))
      if(!(missing(epsilon) || class(epsilon) == "numeric"))
        stop(paste("epsilon cannot be of class", class(epsilon)))
      if(!(missing(standardize) || class(standardize) == "logical"))
        stop(paste("standardize cannot be of class", class(standardize)))  
      if (!(missing(tweedie.p) || class(tweedie.p) == 'numeric'))
        stop(paste('tweedie.p cannot be of class', class(tweedie.p)))
      
      cc = colnames(data)
      if(y %in% x) stop(paste(y, 'is both an explanatory and dependent variable'))
      if(!y %in% cc) stop(paste(y, 'is not a valid column name'))
      if(any(!(x %in% cc))) stop(paste(paste(x[which(!(x %in% cc))], collapse=','), 'is not a valid column name'))
      
      if((missing(lambda) || length(lambda) == 1) && (missing(alpha) || length(alpha) == 1))
        h2o.glm.internal(x, y, data, family, nfolds, alpha, lambda, 1, epsilon, standardize)
      else {
        if(!missing(tweedie.p)) print("Tweedie variance power not available in GLM grid search")
        h2o.glmgrid.internal(x, y, data, family, nfolds, alpha, lambda)
      }
   })

setMethod("h2o.glm", signature(x="character", y="numeric", data="H2OParsedData", family="character", nfolds="ANY", alpha="ANY", lambda="ANY", epsilon="ANY", standardize="ANY", tweedie.p="ANY"),
    function(x, y, data, family, nfolds, alpha, lambda, epsilon, standardize, tweedie.p) {
      if(y < 1 || y > ncol(data)) stop(paste(y, "is not a valid column index"))
      h2o.glm(x, colnames(data)[y], data, family, nfolds, alpha, lambda, epsilon, standardize, tweedie.p)
    })  

setMethod("h2o.glm", signature(x="numeric", y="character", data="H2OParsedData", family="character", nfolds="ANY", alpha="ANY", lambda="ANY", epsilon="ANY", standardize="ANY", tweedie.p="ANY"),
    function(x, y, data, family, nfolds, alpha, lambda, epsilon, standardize, tweedie.p) {
      if (length(x) < 1) stop("GLM requires at least one explanatory variable")
      if(any( x < 1 | x > ncol(data))) stop(paste('Out of range explanatory variable', paste(x[which(x < 1 || x > ncol(data))], collapse=',')))
      h2o.glm(colnames(data)[x], y, data, family, nfolds, alpha, lambda, epsilon, standardize, tweedie.p)
    })

setMethod("h2o.glm", signature(x="numeric", y="numeric", data="H2OParsedData", family="character", nfolds="ANY", alpha="ANY", lambda="ANY", epsilon="ANY", standardize="ANY", tweedie.p="ANY"),
    function(x, y, data, family, nfolds, alpha, lambda, epsilon, standardize, tweedie.p) {
      if(y < 1 || y > ncol(data)) stop(paste(y, "is not a valid column index"))
      h2o.glm(x, colnames(data)[y], data, family, nfolds, alpha, lambda, epsilon, standardize, tweedie.p)
    })

#----------------------------- K-Means Clustering -------------------------------#
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
      
      new("H2OKMeansModel", key=destKey, data=data, model=result)
   })

setMethod("h2o.kmeans", signature(data="H2OParsedData", centers="numeric", cols="ANY", iter.max="ANY"),
    function(data, centers, cols, iter.max) { 
      if(!(missing(cols) || class(cols) == "character" || class(cols) == "numeric"))
        stop(paste("cols cannot be of class", class(cols)))
      else if(!(missing(iter.max) || class(iter.max) == "numeric"))
        stop(paste("iter.max cannot be of class", class(iter.max)))
      h2o.kmeans(data, centers, as.character(cols), iter.max) 
    })

#------------------------------- Principal Components Analysis ----------------------------------#
h2o.prcomp.internal <- function(data, x, dest, max_pc, tol, standardize) {
  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_PCA, key=data@key, x=x, destination_key=dest, max_pc=max_pc, tolerance=tol, standardize=as.numeric(standardize))
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
}

setMethod("h2o.prcomp", signature(data="H2OParsedData", tol="numeric", standardize="logical", retx="logical"), 
    function(data, tol, standardize, retx) {
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
      if(retx) result$x = h2o.predict(new("H2OPCAModel", key=destKey, data=data, model=result))
      
      new("H2OPCAModel", key=destKey, data=data, model=result)
    })

setMethod("h2o.prcomp", signature(data="H2OParsedData", tol="ANY", standardize="ANY", retx="ANY"), 
    function(data, tol, standardize, retx) {
      if(!(missing(tol) || class(tol) == "numeric"))
        stop(paste("tol cannot be of class", class(tol)))
      if(!(missing(standardize) || class(standardize) == "logical"))
        stop(paste("standardize cannot be of class", class(standardize)))
      if(!(missing(retx) || class(retx) == "logical"))
        stop(paste("retx cannot be of class", class(retx)))
      h2o.prcomp(data, tol, standardize, retx)
    })

setMethod("h2o.pcr", signature(x="character", y="character", data="H2OParsedData", ncomp="numeric", family="character", nfolds="ANY", alpha="ANY", lambda="ANY", tweedie.p="ANY"),
    function(x, y, data, ncomp, family, nfolds, alpha, lambda, tweedie.p) {
      myCol = colnames(data)
      if(!y %in% myCol) stop(paste(y, "is not a valid column name"))
      if(y %in% x) stop(paste(y, "is both an explanatory and dependent variable"))
      if(any(!x %in% myCol)) stop("Invalid column names: ", paste(x[which(!x %in% myCol)], collapse=", "))
      if(ncomp < 1 || ncomp > ncol(data)) stop("Number of components must be between 1 and ", ncol(data))
      
      myXCol = which(myCol %in% x)-1
      # myModel = h2o.prcomp(data, myXCol, standardize = TRUE, retx = TRUE)
      myModel = h2o.prcomp.internal(data=data, x=myXCol, dest="", max_pc=ncomp, tol=0, standardize=TRUE)
      myScore = h2o.predict(myModel)
      
      myYCol = which(myCol == y)-1
      rand_cbind_key = paste("__PCABind_", UUIDgenerate(), sep="")
      res = h2o.__remoteSend(data@h2o, "DataManip.json", source=myScore@key, source2=data@key, destination_key=rand_cbind_key, cols=myYCol, destination_key=rand_cbind_key, operation="cbind")
      myGLMData = new("H2OParsedData", h2o=data@h2o, key=res$response$redirect_request_args$src_key)
      h2o.glm.FV(paste("PC", 0:(ncomp-1), sep=""), y, myGLMData, family, nfolds, alpha, lambda, tweedie.p)
    })

#-------------------------------------- Random Forest ----------------------------------------------#
setMethod("h2o.randomForest", signature(x="character", y="character", data="H2OParsedData", ntree="numeric", depth="numeric", classwt="numeric"),
    function(x, y, data, ntree, depth, classwt) {
      # Set randomized model_key
      rand_model_key = paste("__RF_Model__", UUIDgenerate(), sep="")
      
      # Determine predictors to ignore (excluding response column)
      myCol = colnames(data)
      if(!y %in% myCol) stop(paste(y, "is not a valid column name"))
      if(y %in% x) stop(paste(y, "is both an explanatory and dependent variable"))
      myXCol = myCol[-which(y == myCol)]
      if(any(!x %in% myXCol)) stop("Invalid column names: ", paste(x[which(!x %in% myXCol)], collapse=", "))
      x_ignore = setdiff(myXCol, x)
      
      # If no class weights, then default to all 1.0
      if(!any(is.na(classwt))) {
        myWeights = rep(NA, length(classwt))
        for(i in 1:length(classwt))
          myWeights[i] = paste(names(classwt)[i], classwt[i], sep="=")
        res = h2o.__remoteSend(data@h2o, h2o.__PAGE_RF, data_key=data@key, response_variable=y, ignore=paste(x_ignore, collapse=","), ntree=ntree, depth=depth, class_weights=paste(myWeights, collapse=","), model_key = rand_model_key)
      }
      else {
        myWeights = ""
        res = h2o.__remoteSend(data@h2o, h2o.__PAGE_RF, data_key=data@key, response_variable=y, ignore=paste(x_ignore, collapse=","), ntree=ntree, depth=depth, class_weights="", model_key = rand_model_key)
      }
      while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
      destKey = res$destination_key
      res = h2o.__remoteSend(data@h2o, h2o.__PAGE_RFVIEW, model_key=destKey, data_key=data@key, response_variable=y, ntree=ntree, class_weights=paste(myWeights, collapse=","), out_of_bag_error_estimate=1)
      
      result = list()
      result$type = "Classification"
      result$ntree = ntree
      result$oob_err = res$confusion_matrix$classification_error
      result$x = paste(x, collapse = ", ")
      # if(x_ignore[1] != "") result$x_ignore = paste(x_ignore, collapse = ", ")
      
      rf_matrix = cbind(matrix(unlist(res$trees$depth), nrow=3), matrix(unlist(res$trees$leaves), nrow=3))
      rownames(rf_matrix) = c("Min.", "Mean.", "Max.")
      colnames(rf_matrix) = c("Depth", "Leaves")
      result$forest = rf_matrix
      
      # Must check confusion matrix is finished calculating!
      cf = res$confusion_matrix
      cf_scores = unlist(lapply(cf$scores, as.numeric))
      cf_err = unlist(as.numeric(cf$classes_errors))
      # cf_err = rapply(cf$classes_errors, function(x) { ifelse(x == "NaN", NaN, x) }, how = "replace")
      cf_matrix = t(matrix(cf_scores, nrow=length(cf$header)))
      cf_tot = apply(cf_matrix, 2, sum)
      cf_tot.err = 1-sum(diag(cf_matrix))/sum(cf_tot)
      
      cf_matrix = cbind(cf_matrix, cf_err)
      cf_matrix = rbind(cf_matrix, c(cf_tot, cf_tot.err))
      dimnames(cf_matrix) = list(Actual = c(cf$header, "Totals"), Predicted = c(cf$header, "Error"))
      result$confusion = cf_matrix
      
      new("H2ORForestModel", key=destKey, data=data, model=result)
    })

setMethod("h2o.randomForest", signature(x="character", y="numeric", data="H2OParsedData", ntree="numeric", depth="numeric", classwt="numeric"),
    function(x, y, data, ntree, depth, classwt) {
      if(y < 1 || y > ncol(data)) stop(paste(y, "must be between 1 and", ncol(data)))
      h2o.randomForest(x, colnames(data)[y], data, ntree, depth, classwt)
    })

setMethod("h2o.randomForest", signature(x="numeric", y="character", data="H2OParsedData", ntree="numeric", depth="numeric", classwt="numeric"),
    function(x, y, data, ntree, depth, classwt) {
      if(any(x < 1 | x > ncol(data))) stop(paste("x must be between 1 and", ncol(data)))
      h2o.randomForest(colnames(data)[x], y, data, ntree, depth, classwt)
    })

setMethod("h2o.randomForest", signature(x="numeric", y="numeric", data="H2OParsedData", ntree="numeric", depth="numeric", classwt="numeric"),
    function(x, y, data, ntree, depth, classwt) {
      if(y < 1 || y > ncol(data)) stop(paste("y must be between 1 and", ncol(data)))
      if(any(x < 1 | x > ncol(data))) stop(paste("x must be between 1 and", ncol(data)))
      myCol = colnames(data)
      h2o.randomForest(myCol[x], myCol[y], data, ntree, depth, classwt)
    })

setMethod("h2o.randomForest", signature(x="ANY", y="ANY", data="H2OParsedData", ntree="ANY", depth="ANY", classwt="ANY"),
    function(x, y, data, ntree, depth, classwt) {
      if(missing(y)) stop("Must specify a response variable y!")
      if(!(class(y) %in% c("character", "numeric", "integer")))
        stop(paste("y cannot be of class", class(y)))
      
      if(missing(x)) stop("Must specify a predictor variable x!")
      if(!(class(x) %in% c("character", "numeric", "integer")))
        stop(paste("x cannot be of class", class(x)))
        
      if(!(missing(ntree) || class(ntree) == "numeric"))
        stop(paste("ntree cannot be of class", class(ntree)))
      if(!(missing(depth) || class(depth) == "numeric"))
        stop(paste("depth cannot be of class", class(depth)))
      if(!(missing(classwt) || class(classwt) == "numeric"))
        stop(paste("classwt cannot be of class", class(classwt)))
      
      if(class(x) == "integer") x = as.numeric(x)
      if(class(y) == "integer") y = as.numeric(y)
      h2o.randomForest(x, y, data, ntree, depth, classwt)
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

#------------------------------- Prediction ----------------------------------------#
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
      } else if(class(object) == "H2OGBMModel") {
        # Set randomized prediction key
        rand_pred_key = paste("__GBM_Predict_", UUIDgenerate(), sep="")
        res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_PREDICT2, model=object@key, data=newdata@key, prediction=rand_pred_key)
        res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_INSPECT2, src_key=rand_pred_key)
        new("H2OParsedData2", h2o=object@data@h2o, key=rand_pred_key)
        # res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_PREDICT2, model=object@key, data=newdata@key)
        # res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_INSPECT2, key=res$response$redirect_request_args$key)
        # h2o.__pollAll(object@data@h2o, 60)
        # new("H2OParsedData2", h2o=object@data@h2o, key=res$key)
      } else if(class(object) == "H2OPCAModel") {
        # Set randomized prediction key
        rand_pred_key = paste("__PCA_Predict_", UUIDgenerate(), sep="")
        # numMatch = colnames(newdata) %in% colnames(object@data)
        numMatch = colnames(newdata) %in% rownames(object@model$rotation)
        numPC = min(length(numMatch[numMatch == TRUE]), length(object@model$sdev))
        res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_PCASCORE, model_key=object@key, key=newdata@key, destination_key=rand_pred_key, num_pc=numPC)
        h2o.__pollAll(object@data@h2o, timeout = 60)     # Poll until all jobs finished
        new("H2OParsedData2", h2o=object@data@h2o, key=rand_pred_key)
        # res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_PCASCORE, model_key=object@key, key=newdata@key, num_pc=numPC)
        # while(h2o.__poll(object@data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
        # res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_INSPECT2, key=res$response$redirect_request_args$key)
        # new("H2OParsedData2", h2o=object@data@h2o, key=res$key)
      } else
        stop(paste("Prediction has not yet been implemented for", class(object)))
    })

setMethod("h2o.predict", signature(object="H2OModel", newdata="missing"), 
    function(object) { h2o.predict(object, object@data) })

#------------------------------- FluidVecs -------------------------------------#
setMethod("h2o.glm.FV", signature(x="character", y="character", data="H2OParsedData", family="character", nfolds="ANY", alpha="ANY", lambda="ANY", tweedie.p="ANY"),
    function(x, y, data, family, nfolds = 10, alpha = 0.5, lambda = 1.0e-5, tweedie.p = ifelse(family == "tweedie", 0, NA)) {
      if(family != "tweedie" && !(missing(tweedie.p) || is.na(tweedie.p)))
        stop("tweedie.p may only be set for family tweedie")
      
      x_ignore = which(!colnames(data) %in% c(x, y)) - 1
      if(length(x_ignore) == 0) x_ignore = ""
      rand_glm_key = paste("__GLM2Model_", UUIDgenerate(), sep="")
      
      if(family != "tweedie")
        res = h2o.__remoteSend(data@h2o, "GLM2.json", source = data@key, destination_key = rand_glm_key, vresponse = y, ignored_cols = paste(x_ignore, sep="", collapse=","), family = family, n_folds = nfolds, alpha = alpha, lambda = lambda, standardize = as.numeric(FALSE))
      else
        res = h2o.__remoteSend(data@h2o, "GLM2.json", source = data@key, destination_key = rand_glm_key, vresponse = y, ignored_cols = paste(x_ignore, sep="", collapse=","), family = family, n_folds = nfolds, alpha = alpha, lambda = lambda, tweedie_variance_power = tweedie.p, standardize = as.numeric(FALSE))
      while(h2o.__poll(data@h2o, res$job_key) != -1) { Sys.sleep(1) }
      
      res = h2o.__remoteSend(data@h2o, "GLMModelView.json", '_modelKey'=rand_glm_key)
      resModel = res$glm_model
      res = h2o.__remoteSend(data@h2o, "GLMValidationView.json", '_valKey'=resModel$validations)
      modelOrig = h2o.__getGLM2Results(resModel, y, res$glm_val)
      new("H2OGLMModel", key=resModel$'_selfKey', data=data, model=modelOrig, xval=list())
  })

# Pretty formatting of H2O GLM results
h2o.__getGLM2Results <- function(model, y, valid) {
  result = list()
  result$y = y
  result$x = model$'_names'
  result$coefficients = unlist(model$beta)
  result$rank = length(result$coefficients) + 1
  if(model$glm$family == "tweedie")
    result$family = h2o.__getFamily(model$glm$family, model$glm$link, model$glm$tweedie_variance_power, model$glm$tweedie_link_power)
  else
    result$family = h2o.__getFamily(model$glm$family, model$glm$link)
  result$iter = model$iteration
  
  result$deviance = valid$residual_deviance
  result$aic = valid$aic
  result$null.deviance = valid$null_deviance
  result$train.err = valid$avg_err
  # result$df.residual = res$dof
  # result$df.null = res$dof + result$rank
  
  if(model$glm$family == "binomial") {
    result$threshold = model$threshold
    result$auc = valid$auc
    # result$class.err = res$validations[[1]]$classErr
    
    # Construct confusion matrix
    # temp = t(data.frame(sapply(res$validations[[1]]$cm, c)))
    # dn = list(Actual = temp[-1,1], Predicted = temp[1,-1])
    # temp = temp[-1,]; temp = temp[,-1]
    # dimnames(temp) = dn
    # result$cm = temp
  }
  return(result)
}
