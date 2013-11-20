# Model-building operations and algorithms

#----------------------- Generalized Boosting Machines (GBM) -----------------------#
#TODO: dont support missing x; default to everything?
h2o.gbm <- function(x, y, distribution='multinomial', data, n.trees=10, interaction.depth=5, n.minobsinnode=10, shrinkage=0.02) {
  args <- verify_dataxy(data, x, y)

  if( class(n.trees) != "numeric" ) stop('n.trees must be numeric')
  if( n.trees < 1 ) stop('n.trees must be >=1')
  if( class(interaction.depth) != "numeric") stop('interaction.depth must be numeric')
  if( interaction.depth < 1 ) stop('interaction.depth must be >= 1')
  if( class(n.minobsinnode) != 'numeric' ) stop('n.minobsinnode must be numeric')
  if( n.minobsinnode < 1) stop('n.minobsinnode must be >= 1')
  if( class(shrinkage) != 'numeric') stop('shrinkage must be numeric')
  if( shrinkage < 0 ) stop('shrinkage must be >= 0')

  # NB: externally, 1 based indexing; internally, 0 based
  cols <- paste(args$x_i - 1,collapse=',')

  if( !(distribution %in% c('multinomial', 'gaussian')) )
    stop(paste(distribution, "is not a valid distribution; only [multinomial, gaussian] are supported"))
  classification <- ifelse(distribution == 'multinomial', 1, ifelse(distribution=='gaussian', 0, -1))

  destKey = h2o.__uniqID("GBMModel")
  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GBM, destination_key=destKey, source=data@key, response=args$y, cols=cols, ntrees=n.trees, max_depth=interaction.depth, learn_rate=shrinkage, min_rows=n.minobsinnode, classification=classification)
  while(h2o.__poll(data@h2o, res$job_key) != -1) { Sys.sleep(1) }
  res2 = h2o.__remoteSend(data@h2o, h2o.__PAGE_GBMModelView, '_modelKey'=destKey)

  result=list()
  categories=length(res2$gbm_model$cm)
  cf_matrix = t(matrix(unlist(res2$gbm_model$cm), nrow=categories))
  cf_names <- res2$gbm_model[['_domains']]
  cf_names <- cf_names[[ length(cf_names) ]]

  dimnames(cf_matrix) = list(Actual = cf_names, Predicted = cf_names)
  result$confusion = cf_matrix

  result$err = res2$gbm_model$errs
  new("H2OGBMModel", key=destKey, data=data, model=result)
}

#----------------------------- Generalized Linear Models (GLM) ---------------------------#
# Internally called GLM to allow games with method dispatch
# x should be TODO; y should be TODO
h2o.glm.internal <- function(x, y, data, family, nfolds, alpha, lambda, expert_settings, beta_epsilon, standardize, tweedie.p) {
  if(family == 'tweedie' && (tweedie.p < 1 || tweedie.p > 2 )) stop('tweedie.p must be in (1,2)')
  if(family != "tweedie" && !(missing(tweedie.p) || is.na(tweedie.p) ) ) stop('tweedie.p may only be set for family tweedie')

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
    result$confusion = temp
  }
  return(result)
}


h2o.glm <- function(x, y, data, family, nfolds=10, alpha=0.5, lambda=1e-5, epsilon=1e-5, standardize=T, tweedie.p=ifelse(family=='tweedie', 1.5, as.numeric(NA))) {
  args <- verify_dataxy(data, x, y)
  if( nfolds < 0 ) stop('nfolds must be >= 0')
  if( alpha < 0 ) stop('alpha must be >= 0')
  if( lambda < 0 ) stop('lambda must be >= 0')
  if( epsilon < 0 ) stop('epsilon must be >= 0')
  if( class(nfolds) != 'numeric' ) stop('nfolds must be numeric')
  if( nfolds < 0 ) stop('nfolds must be >= 0')
  if( class(standardize) != 'logical' ) stop('standardize must be T or F')
  if( class(tweedie.p) != 'numeric' ) stop('tweedie.p must be numeric')

  # NB: externally, 1 based indexing; internally, 0 based
  if((missing(lambda) || length(lambda) == 1) && (missing(alpha) || length(alpha) == 1))
    h2o.glm.internal(args$x_i - 1, args$y, data, family, nfolds, alpha, lambda, 1, epsilon, standardize)
  else {
    if(!missing(tweedie.p)) print('Tweedie variance power not available in GLM grid search')
    h2o.glmgrid.internal(args$x_i - 1, args$y, data, family, nfolds, alpha, lambda)
  }
}


#----------------------------- K-Means Clustering -------------------------------#
#setGeneric("h2o.kmeans", function(data, centers, cols = "", iter.max = 10) { standardGeneric("h2o.kmeans") })
#TODO: limitation: centers must be # of clusters; don't support cluster init values yet
h2o.kmeans <- function(data, centers, cols, iter.max=10){
  if( missing(data) ) stop('must specify data')
  if( class(data) != 'H2OParsedData' ) stop('data must be an h2o dataset')

  if( missing( cols ) ) stop('must provide cols')
  if(!( class(cols) %in% c('character', 'numeric', 'integer') )) stop('cols must be character or numeric')
  if( class(iter.max) != 'numeric') stop('iter.max must be numeric')
  if( iter.max < 1 ) stop('iter.max must be >= 1')
  if( missing( centers ) ) stop('must provide centers')

  if( class(centers) != 'numeric' || length(centers) > 1 || is.na(centers) || centers < 1 )
    stop('centers must be the number of clusters to create')

  cc <- colnames( data )
  if( class(cols) == 'character' ){
    if(any(!(cols %in% cc))) stop(paste(paste(cols[!(cols %in% cc)], collapse=','), 'is not a valid column name'))
  } else {
    if(any( cols < 1 | cols > length(cc) )) stop(paste('Out of range explanatory variable', paste(cols[cols < 1 | cols > length(cc)], collapse=',')))
    cols <- cols - 1
  }

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
}

#------------------------------- Neural Network ----------------------------------#
h2o.nn <- function(x, y,  data, classification=T, activation='Tanh', layers=500, rate=0.01, regularization=1e-4, epoch=100, validation) {
  args <- verify_dataxy(data, x, y)

  if( class(classification) != 'logical' ) stop('classification must be true or false')
  if( class(activation) != 'character') stop('activation must be [Tanh, Rectifier]')
  if(!( activation %in% c('Tanh', 'Rectifier')) ) stop(paste('invalid activation', activation))
  if( class(layers) != 'numeric') stop('layers must be numeric')
  if( layers < 1 ) stop('layers must be >= 1')
  if( class(rate) != 'numeric') stop('rate must be numeric')
  if( rate < 0 ) stop('rate must be >= 1')
  if( class(regularization) != 'numeric') stop('regularization must be numeric')
  if( regularization < 0 ) stop('regularization must be >= 1')
  if( class(epoch) != 'numeric') stop('epoch must be numeric')
  if( epoch < 0 ) stop('epoch must be >= 1')

  if( !(activation %in% c('Tanh', 'Rectifier')) )
    stop(paste(activation, "is not a valid activation; only [Tanh, Rectifier] are supported"))
  if( !(classification %in% c( 0, 1)) )
    stop(paste(classification, "is not a valid classification index; only [0, 1] are supported"))

  destKey = h2o.__uniqID("NNModel")
  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_NN, destination_key=destKey, source=data@key, response=args$y, cols=paste(args$x_i - 1, collapse=','),
      classification=as.numeric(classification), activation=activation, rate=rate,
      hidden=paste(layers, sep="", collapse=","), l2=regularization, epochs=epoch, validation=data@key)
  while(h2o.__poll(data@h2o, res$job_key) != -1) { Sys.sleep(1) }
  res2 = h2o.__remoteSend(data@h2o, h2o.__PAGE_NNModelView, model=destKey)

  result=list()

  categories=length(res2$model$confusion_matrix)
  cf_matrix = t(matrix(unlist(res2$model$confusion_matrix), nrow=categories))
  cf_names <- res2$model[['_domains']]
  cf_names <- cf_names[[ length(cf_names) ]]
  dimnames(cf_matrix) = list(Actual = cf_names, Predicted = cf_names)

  result$confusion = cf_matrix
  result$items = res2$model$items
  result$train_class_error = res2$model$train_classification_error
  result$train_sqr_error = res2$model$train_mse
  result$valid_class_error = res2$model$validation_classification_error
  result$valid_sqr_error = res2$model$validation_mse
  new("H2ONNModel", key=destKey, data=data, model=result)
}



#------------------------------- Principal Components Analysis ----------------------------------#
h2o.prcomp.internal <- function(data, x_ignore, dest, max_pc, tol, standardize) {
  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_PCA, source=data@key, ignored_cols_by_name=x_ignore, destination_key=dest, max_pc=max_pc, tolerance=tol, standardize=as.numeric(standardize))
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

h2o.prcomp <- function(data, tol=0, standardize=T, retx=F) {
  if( missing(data) ) stop('must specify data')
  if( class(data) != 'H2OParsedData' ) stop('data must be an h2o dataset')
  if( class(tol) != 'numeric' ) stop('tol must be numeric')
  if( class( standardize ) != 'logical' ) stop('standardize must be TRUE or FALSE')
  if( class( retx ) != 'logical' ) stop('retx must be TRUE or FALSE')

  destKey = h2o.__uniqID("PCAModel")
  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_PCA, source=data@key, destination_key=destKey, tolerance=tol, standardize=as.numeric(standardize))
  while(h2o.__poll(data@h2o, res$job_key) != -1) { Sys.sleep(1) }
  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_PCAModelView, '_modelKey'=destKey)
  res = res$pca_model

  result = list()
  result$standardized = standardize
  result$sdev = res$sdev
  nfeat = length(res$eigVec[[1]])
  temp = t(matrix(unlist(res$eigVec), nrow = nfeat))
  rownames(temp) = res$'_names'
  colnames(temp) = paste("PC", seq(1, ncol(temp)), sep="")
  result$rotation = temp
  if(retx) result$x = h2o.predict(new("H2OPCAModel", key=destKey, data=data, model=result))

  new("H2OPCAModel", key=destKey, data=data, model=result)
}

#setGeneric("h2o.pcr", function(x, y, data, ncomp, family, nfolds = 10, alpha = 0.5, lambda = 1.0e-5, tweedie.p = ifelse(family=="tweedie", 0, NA)) { standardGeneric("h2o.pcr") })
h2o.pcr <- function(x, y, data, ncomp, family, nfolds=10, alpha=0.5, lambda=1e-5, tweedie.p=ifelse(family=="tweedie", 0, NA)) {
  args <- verify_dataxy(data, x, y)

  if( class(nfolds) != 'numeric' ) stop('nfolds must be numeric')
  if( nfolds < 0 ) stop('nfolds must be >= 0')
  if( class(alpha) != 'numeric' ) stop('alpha must be numeric')
  if( alpha < 0 ) stop('alpha must be >= 0')
  if( class(lambda) != 'numeric' ) stop('lambda must be numeric')
  if( lambda < 0 ) stop('lambda must be >= 0')
  if( class(epsilon) != 'numeric' ) stop('epsilon must be numeric')
  if( epsilon < 0 ) stop('epsilon must be >= 0')

  y <- args$y
  if( ncomp < 1 || ncomp > length(cc) ) stop("Number of components must be between 1 and ", ncol(data))

  x_ignore <- args$x_ignore
  x_ignore <- ifelse( x_ignore=='', y, c(x_ignore,y) )
  myModel <- h2o.prcomp.internal(data=data, x_ignore=x_ignore, dest="", max_pc=ncomp, tol=0, standardize=TRUE)
  myScore <- h2o.predict(myModel)

  rand_cbind_key = h2o.__uniqID("PCABind")
  res <- h2o.__remoteSend(data@h2o, h2o.__PAGE_FVEXEC, source=myScore@key, source2=data@key, destination_key=rand_cbind_key, cols=args$y_i - 1, destination_key=rand_cbind_key, operation="cbind")
  myGLMData <- new("H2OParsedData", h2o=data@h2o, key=res$response$redirect_request_args$src_key)
  h2o.glm.FV(paste("PC", 0:(ncomp-1), sep=""), y, myGLMData, family, nfolds, alpha, lambda, tweedie.p)
}

#-------------------------------------- Random Forest ----------------------------------------------#
#setGeneric("h2o.randomForest", function(x, y, data, ntree = 50, depth = 2147483647, classwt = as.numeric(NA)) { standardGeneric("h2o.randomForest") })
# x,y <- character for this one
h2o.randomForest <- function(x, y, data, ntree=50, depth=2147483647, classwt=as.numeric(NA)) {
  args <- verify_dataxy(data, x, y)

  if( class(ntree) != 'numeric' ) stop('ntree must be a number')
  if( ntree < 1 ) stop('ntree must be >= 1')
  if( class(depth) != 'numeric' ) stop('depth must be a number')
  if( depth < 1 ) stop('depth must be >= 1')
  if( class(classwt) != 'numeric' ) stop('classwt must be numeric')
  if( any(!is.na(classwt) & classwt < 0) ) stop('classwt must be >= 0')

  # Set randomized model_key
  rand_model_key = h2o.__uniqID("RFModel")

  # If no class weights, then default to all 1.0
  if( !any(is.na(classwt)) ){
    myWeights <- rep(NA, length(classwt))
    for(i in 1:length(classwt))
      myWeights[i] = paste(names(classwt)[i], classwt[i], sep="=")
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_RF, data_key=data@key, response_variable=args$y, ignore=paste(args$x_ignore, collapse=","), ntree=ntree, depth=depth, class_weights=paste(myWeights, collapse=","), model_key = rand_model_key)
  } else {
    myWeights = ""
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_RF, data_key=data@key, response_variable=args$y, ignore=paste(args$x_ignore, collapse=","), ntree=ntree, depth=depth, class_weights="", model_key = rand_model_key)
  }
  while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
  destKey = res$destination_key
  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_RFVIEW, model_key=destKey, data_key=data@key, response_variable=args$y, ntree=ntree, class_weights=paste(myWeights, collapse=","), out_of_bag_error_estimate=1)

  result = list()
  result$type = "Classification"
  result$ntree = ntree
  result$oob_err = res$confusion_matrix$classification_error
  result$x = paste(args$x, collapse = ", ")

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
}

#setMethod("h2o.getTree", signature(forest="H2ORForestModel", k="numeric", plot="logical"),
h2o.getTree <- function(forest, k, plot=FALSE) {
  if( missing(forest) ) stop('must specify forest')
  if( class(forest) != 'H2ORForestModel' ) stop('forest must be an h2o forest model')
  if( missing(k) ) stop('must specify k')
  if(k < 1 || k > forest@model$ntree) stop(paste("k must be between 1 and", forest@model$ntree))
  if( class(plot) != 'logical' ) stop('plot must be logical')

  res = h2o.__remoteSend(forest@data@h2o, h2o.__PAGE_RFTREEVIEW, model_key=forest@key, tree_number=k-1, data_key=forest@data@key)
  if(plot) browseURL(paste0("http://", forest@data@h2o@ip, ":", forest@data@h2o@port, "/RFTreeView.html?model_key=", forest@key, "&data_key=", forest@data@key, "&tree_number=", k-1))

  result = list()
  result$depth = res$depth
  result$leaves = res$leaves
  result
}


#------------------------------- Prediction ----------------------------------------#
#setMethod("h2o.predict", signature(object="H2OModel", newdata="H2OParsedData"),
h2o.predict <- function(object, newdata) {
  if( missing(object) ) stop('must specify object')
  if(!( class(object) %in% c('H2OPCAModel', 'H2OGBMModel', 'H2OKMeansModel', 'H2OModel', 'H2OGLMModel', 'H2ORForestModel', 'H2ODRFModel') )) stop('object must be an H2OModel')
  if( missing(newdata) ) newdata <- object@data
  if(!( class(newdata) %in% c('H2OParsedData', 'H2OParsedData2') )) stop('newdata must be h2o data')

  # fv require certain models
  if( class(object) %in% c('H2OGBMModel', 'H2OPCAModel', 'H2ODRFModel') ){
    if( class(newdata) != 'H2OParsedData2' ) stop(paste('Prediction in FluidVecs has not yet been implemented for', class(object)))
    newdata <- new('H2OParsedData', h2o=newdata@h2o, key=newdata@key)
  }
  if( class(newdata) == 'H2OParsedData2' ) stop(paste('Prediction in FluidVecs has not been implemented for', class(object)))

  if(class(object) == "H2OGLMModel" || class(object) == "H2ORForestModel") {
    res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_PREDICT, model_key=object@key, data_key=newdata@key)
    res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_INSPECT, key=res$response$redirect_request_args$key)
    new("H2OParsedData", h2o=object@data@h2o, key=res$key)
  } else if(class(object) == "H2OKMeansModel") {
    res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_KMAPPLY, model_key=object@key, data_key=newdata@key)
    while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
    new("H2OParsedData", h2o=object@data@h2o, key=res$key)
  } else if(class(object) == "H2OGBMModel" || class(object) == "H2ODRFModel") {
    # Set randomized prediction key
    key_prefix = ifelse(class(object) == "H2OGBMModel", "GBMPredict", "DRFPredict")
    rand_pred_key = h2o.__uniqID(key_prefix)
    res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_PREDICT2, model=object@key, data=newdata@key, prediction=rand_pred_key)
    res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_INSPECT2, src_key=rand_pred_key)
    new("H2OParsedData2", h2o=object@data@h2o, key=rand_pred_key)
  } else if(class(object) == "H2OPCAModel") {
    # Set randomized prediction key
    rand_pred_key = h2o.__uniqID("PCAPredict")
    numMatch = colnames(newdata) %in% rownames(object@model$rotation)
    numPC = min(length(numMatch[numMatch == TRUE]), length(object@model$sdev))
    res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_PCASCORE, source=newdata@key, model=object@key, destination_key=rand_pred_key, num_pc=numPC)
    while(h2o.__poll(object@data@h2o, res$job_key) != -1) { Sys.sleep(1) }
    new("H2OParsedData2", h2o=object@data@h2o, key=rand_pred_key)
  } else
    stop(paste("Prediction has not yet been implemented for", class(object)))
}



#------------------------------- FluidVecs -------------------------------------#
#setGeneric("h2o.glm.FV", function(x, y, data, family, nfolds = 10, alpha = 0.5, lambda = 1.0e-5, tweedie.p=ifelse(family=='tweedie', 0, NA)) { standardGeneric("h2o.glm.FV") })
#setMethod("h2o.glm.FV", signature(x="character", y="character", data="H2OParsedData", family="character", nfolds="ANY", alpha="ANY", lambda="ANY", tweedie.p="ANY"),
h2o.glm.FV <- function(x, y, data, family, nfolds = 10, alpha = 0.5, lambda = 1.0e-5, tweedie.p = ifelse(family == "tweedie", 0, NA)) {
  if( missing(data) ) stop('must specify data')
  if(!( class(data) %in% c('H2OParsedData','H2OParsedData2') )) stop('data must be an h2o dataset')
  if( missing(x) ) stop('must specify x')
  if(!( class(x) %in% c('numeric', 'character') )) stop('x must be a vector of column names or indices')
  if( missing(y) ) stop('must specify y')
  if(!( class(y) %in% c('numeric', 'character') )) stop('y must be a column name or index')

  if( class(data) == 'H2OParsedData2' ) data <- new("H2OParsedData", h2o=data@h2o, key=data@key)
  if( class(nfolds) != 'numeric' ) stop('nfolds must be numeric')
  if( nfolds < 0 ) stop('nfolds must be >= 0')
  if( class(alpha) != 'numeric' ) stop('alpha must be numeric')
  if( alpha < 0 ) stop('alpha must be >= 0')
  if( class(lambda) != 'numeric' ) stop('lambda must be numeric')
  if( lambda < 0 ) stop('lambda must be >= 0')

  if(! class(tweedie.p) %in% c('logical','numeric')) stop('tweedie.p must be numeric')
  if( family != 'tweedie' && !(missing(tweedie.p) || is.na(tweedie.p)) ) stop("tweedie.p may only be set for family tweedie")

  cc <- colnames(data)
  if( class(y) == 'numeric' ){
    if( y < 1 || y > length(cc) ) stop ('y is out of range of the columns')
    y <- cc[y]
  }
  if(!y %in% cc) stop(paste(y, "is not a valid column name"))

  if( class(x) == 'character' ){
    if( any(!(x %in% cc)) ) stop(paste( paste(setdiff(x, cc), sep=','), 'are not valid column names'))
    x <- match(x, cc)
  }
  if( which(y==cc) %in% x ) stop(paste(y, 'is both an explanatory and dependent variable'))
  if( any( x < 1 | x > length(cc) ) ) stop( paste(x[ x < 1 | x > length(cc)], sep=','), 'is out of range of the columns' )
  x_ignore <- setdiff(x, 1:length(cc)) - 1
  if(length(x_ignore) == 0) x_ignore <- ''
  rand_glm_key = h2o.__uniqID("GLM2Model")

  if(family != "tweedie")
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLM2, source = data@key, destination_key = rand_glm_key, response = y, ignored_cols = paste(x_ignore, sep="", collapse=","), family = family, n_folds = nfolds, alpha = alpha, lambda = lambda, standardize = as.numeric(FALSE))
  else
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLM2, source = data@key, destination_key = rand_glm_key, response = y, ignored_cols = paste(x_ignore, sep="", collapse=","), family = family, n_folds = nfolds, alpha = alpha, lambda = lambda, tweedie_variance_power = tweedie.p, standardize = as.numeric(FALSE))
  while(h2o.__poll(data@h2o, res$job_key) != -1) { Sys.sleep(1) }

  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLMModelView, '_modelKey'=rand_glm_key)
  resModel = res$glm_model
  modelOrig = h2o.__getGLM2Results(resModel, y, list())
  new("H2OGLMModel", key=resModel$'_selfKey', data=data, model=modelOrig, xval=list())
}


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

  if(model$glm$family == "binomial") {
    result$threshold = model$threshold
    result$auc = valid$auc
  }
  return(result)
}

#setGeneric("h2o.kmeans.FV", function(data, centers, cols = "", iter.max = 10) { standardGeneric("h2o.kmeans.FV") })
#setMethod("h2o.kmeans.FV", signature(data="H2OParsedData", centers="numeric", cols="character", iter.max="numeric"),
h2o.kmeans.FV <- function(data, centers, cols='', iter.max=10) {
  if( missing(data) ) stop('must specify data')
  if(!( class(data) %in% c('H2OParsedData','H2OParsedData2') )) stop('data must be an h2o dataset')

  if( missing(centers) ) stop('must specify centers')
  if( class(center) != 'numeric' ) stop('must specify centers')
  if( centers <= 0 ) stop("centers must be an integer greater than 0")
  if( class(iter.max) != 'numeric' ) stop('iter.max must be numeric')
  if( iter.max < 1) stop('iter.max must be >=1')

  cc <- colnames(data)
  if( class(cols) == 'numeric' ){
    if( any( x < 1 | x > length(cc) ) ) stop( paste(x[ x < 1 | x > length(cc)], sep=','), 'is out of range of the columns' )
    x <- cc[ x ]
  }
  if( any(!x %in% cc) ) stop("Invalid column names: ", paste(x[which(!x %in% cc)], collapse=", "))


  myIgnore <- ifelse(cols == '', '', paste(setdiff(cols, cc), sep=','))

  rand_kmeans_key = h2o.__uniqID("KMeans2Model")
  res = h2o.__remoteSend(data@h2o, "2/KMeans2.json", source=data@key, destination_key=rand_kmeans_key, ignored_cols=myIgnore, k=centers, max_iter=iter.max)
  while(h2o.__poll(data@h2o, res$job_key) != -1) { Sys.sleep(1) }
  res = h2o.__remoteSend(data@h2o, "2/KMeans2ModelView.json", model=rand_kmeans_key)
  res = res$model

  rand_pred_key = h2o.__uniqID("KMeans2Clusters")
  h2o.__remoteSend(data@h2o, h2o.__PAGE_PREDICT2, model=res$'_selfKey', data=data@key, prediction=rand_pred_key)

  result = list()
  result$clusters = new("H2OParsedData2", h2o=data@h2o, key=rand_pred_key)
  feat = res$'_names'[-length(res$'_names')]     # Get rid of response column name
  result$centers = t(matrix(unlist(res$clusters), ncol = centers))
  dimnames(result$centers) = list(seq(1,centers), feat)
  result$withinss = res$cluster_variances    # TODO: Not sure if this is within or between SS?
  new("H2OKMeansModel", key=res$'_selfKey', data=data, model=result)
}

h2o.randomForest.FV <- function(x, y, data, n.trees=50, interaction.depth=50, nodesize=1, sample.rate=2/3) {
  args <- verify_dataxy(data, x, y)
  
  if( class(n.trees) != 'numeric' ) stop('n.trees must be a number')
  if( n.trees < 1 ) stop('n.trees must be >= 1')
  if( class(interaction.depth) != 'numeric' ) stop('interaction.depth must be a number')
  if( interaction.depth < 1 ) stop('interaction.depth must be >= 1')
  if( class(nodesize) != 'numeric' ) stop('nodesize must be a number')
  if( nodesize < 1 ) stop('nodesize must be >= 1')
  if( class(sample.rate) != 'numeric' ) stop('sample.rate must be a number')
  if( sample.rate < 0 || sample.rate > 1 ) stop('sample.rate must be between 0 and 1')
  
  # NB: externally, 1 based indexing; internally, 0 based
  cols <- paste(args$x_i - 1,collapse=',')
  
  destKey = h2o.__uniqID("DRFModel")
  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_DRF, destination_key=destKey, source=data@key, response=args$y, cols=cols, ntrees=n.trees, max_depth=interaction.depth, min_rows=nodesize, sample_rate=sample.rate)
  while(h2o.__poll(data@h2o, res$job_key) != -1) { Sys.sleep(1) }
  res2 = h2o.__remoteSend(data@h2o, h2o.__PAGE_DRFModelView, '_modelKey'=destKey)
  
  result=list()
  categories=length(res2$drf_model$cm)
  cf_matrix = t(matrix(unlist(res2$drf_model$cm), nrow=categories))
  cf_names <- res2$drf_model[['_domains']]
  cf_names <- cf_names[[ length(cf_names) ]]
  dimnames(cf_matrix) = list(Actual = cf_names, Predicted = cf_names)
  result$confusion = cf_matrix
  
  treeStats = unlist(res2$drf_model$treeStats)
  rf_matrix = rbind(treeStats[1:3], treeStats[4:6])
  colnames(rf_matrix) = c("Min.", "Max.", "Mean.")
  rownames(rf_matrix) = c("Depth", "Leaves")
  result$forest = rf_matrix
  
  result$mse = res2$drf_model$errs
  result$ntree = n.trees
  new("H2ODRFModel", key=destKey, data=data, model=result)
}

# used to verify data, x, y and turn into the appropriate things
verify_dataxy <- function(data, x, y){
  if( missing(data) ) stop('must specify data')
  if(!( class(data) %in% c('H2OParsedData', 'H2OParsedData2') )) stop('data must be an h2o dataset')

  if( missing(x) ) stop('must specify x')
  if( missing(y) ) stop('must specify y')
  if(!( class(x) %in% c('numeric', 'character', 'integer') )) stop('x must be column names or indices')
  if(!( class(y) %in% c('numeric', 'character', 'integer') )) stop('y must be a column name or index')

  cc <- colnames( data )
  if( class(x) == 'character' ){
    if(any(!(x %in% cc))) stop(paste(paste(x[!(x %in% cc)], collapse=','), 'is not a valid column name'))
    x_i <- match(x, cc)
  } else {
    if(any( x < 1 | x > length(cc) )) stop(paste('Out of range explanatory variable', paste(x[x < 1 | x > length(cc)], collapse=',')))
    x_i <- x
    x <- cc[ x_i ]
  }

  if( class(y) == 'character' ){
    if(!( y %in% cc )) stop(paste(y, 'is not a column name'))
    y_i <- which(y == cc)
  } else {
    if( y < 1 || y > length(cc) ) stop(paste('Response variable index', y, 'is out of range'))
    y_i <- y
    y <- cc[ y ]
  }
  if( y %in% x ) stop(paste( y, 'is both an explanatory and dependent variable'))

  x_ignore <- setdiff(setdiff( cc, x ), y)
  if( length(x_ignore) == 0 ) x_ignore <- ''
  list(x=x, y=y, x_i=x_i, x_ignore=x_ignore, y_i=y_i)
}
