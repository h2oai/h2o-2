# Model-building operations and algorithms
# ----------------------- Generalized Boosting Machines (GBM) -----------------------#
# TODO: dont support missing x; default to everything?
h2o.gbm <- function(x, y, distribution='multinomial', data, n.trees=10, interaction.depth=5, n.minobsinnode=10, shrinkage=0.02) {
  args <- verify_dataxy(data, x, y)

  if(!is.numeric(n.trees)) stop('n.trees must be numeric')
  if( n.trees < 1 ) stop('n.trees must be >=1')
  if(!is.numeric(interaction.depth)) stop('interaction.depth must be numeric')
  if( interaction.depth < 1 ) stop('interaction.depth must be >= 1')
  if(!is.numeric(n.minobsinnode)) stop('n.minobsinnode must be numeric')
  if( n.minobsinnode < 1) stop('n.minobsinnode must be >= 1')
  if(!is.numeric(shrinkage)) stop('shrinkage must be numeric')
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
# setGeneric("h2o.glm", function(x, y, data, family, nfolds = 10, alpha = 0.5, lambda = 1.0e-5, tweedie.p=ifelse(family=='tweedie', 0, NA)) { standardGeneric("h2o.glm") })
# setMethod("h2o.glm", signature(x="character", y="character", data="H2OParsedData", family="character", nfolds="ANY", alpha="ANY", lambda="ANY", tweedie.p="ANY"),
h2o.glm.FV <- function(x, y, data, family, nfolds = 10, alpha = 0.5, lambda = 1.0e-5, tweedie.p = ifelse(family == "tweedie", 0, as.numeric(NA))) {
  if( missing(data) ) stop('must specify data')
  if(class(data) != 'H2OParsedData' ) stop('data must be an h2o dataset')
  if( missing(x) ) stop('must specify x')
  if(!( class(x) %in% c('numeric', 'character', 'integer') )) stop('x must be a vector of column names or indices')
  if( missing(y) ) stop('must specify y')
  if(!( class(y) %in% c('numeric', 'character', 'integer') )) stop('y must be a column name or index')
  
  if(!is.numeric(nfolds)) stop('nfolds must be numeric')
  if( nfolds < 0 ) stop('nfolds must be >= 0')
  if(!is.numeric(alpha)) stop('alpha must be numeric')
  if( alpha < 0 ) stop('alpha must be >= 0')
  if(!is.numeric(lambda)) stop('lambda must be numeric')
  if( lambda < 0 ) stop('lambda must be >= 0')
  
  if(!is.numeric(tweedie.p)) stop('tweedie.p must be numeric')
  if( family != 'tweedie' && !(missing(tweedie.p) || is.na(tweedie.p)) ) stop("tweedie.p may only be set for family tweedie")
  
  cc <- colnames(data)
  if(is.numeric(y)){
    if( y < 1 || y > length(cc) ) stop ('y is out of range of the columns')
    y <- cc[y]
  }
  if(!y %in% cc) stop(paste(y, "is not a valid column name"))
  
  if(is.character(x)) {
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
  resModel = res$glm_model; destKey = resModel$'_selfKey'
  modelOrig = h2o.__getGLM2Results(resModel, x, y, resModel$submodels[[1]]$validation)
  
  # Get results from cross-validation
  if(nfolds < 2)
    return(new("H2OGLMModel", key=destKey, data=data, model=modelOrig, xval=list()))
  
  res_xval = list()
  for(i in 1:nfolds) {
    xvalKey = paste(destKey, "_xval", seq(0,nfolds-1), sep="")
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLMModelView, '_modelKey'=xvalKey[i])
    modelXval = h2o.__getGLM2Results(res$glm_model, x, y, res$glm_model$submodels[[1]]$validation)
    res_xval[[i]] = new("H2OGLMModel", key=xvalKey, data=data, model=modelXval, xval=list())
  }
  new("H2OGLMModel", key=destKey, data=data, model=modelOrig, xval=res_xval)
}

# Pretty formatting of H2O GLM2 results
h2o.__getGLM2Results <- function(model, x, y, valid) {
  result = list()
  result$y = y
  result$x = x
  # result$coefficients = unlist(model$beta)
  result$coefficients = as.numeric(unlist(model$submodels[[1]]$beta))
  names(result$coefficients) = model$coefficients_names
  result$rank = valid$'_rank'
  if(model$glm$family == "tweedie")
    result$family = h2o.__getFamily(model$glm$family, model$glm$link, model$glm$tweedie_variance_power, model$glm$tweedie_link_power)
  else
    result$family = h2o.__getFamily(model$glm$family, model$glm$link)
  # result$iter = model$iteration
  result$iter = model$submodels[[1]]$iteration
  
  result$deviance = as.numeric(valid$residual_deviance)
  result$null.deviance = as.numeric(valid$null_deviance)
  result$df.residual = max(valid$nobs-result$rank,0)
  result$df.null = valid$nobs-1
  result$aic = as.numeric(valid$aic)
  result$train.err = as.numeric(valid$avg_err)
  
  if(model$glm$family == "binomial") {
    result$threshold = as.numeric(model$threshold)
    result$best_threshold = as.numeric(valid$best_threshold)
    result$auc = as.numeric(valid$auc)
    
    # Construct confusion matrix
    cm_ind = trunc(100*result$best_threshold) + 1
    temp = data.frame(t(sapply(valid$'_cms'[[cm_ind]]$'_arr', c)))
    temp[,3] = c(temp[1,2], temp[2,1])/apply(temp, 1, sum)
    temp[3,] = c(temp[2,1], temp[1,2], 0)/apply(temp, 2, sum)
    temp[3,3] = (temp[1,2] + temp[2,1])/valid$nobs
    dn = list(Actual = c("false", "true", "Err"), Predicted = c("false", "true", "Err"))
    dimnames(temp) = dn
    result$confusion = temp
  }
  return(result)
}

#----------------------------- K-Means Clustering -------------------------------#
h2o.kmeans <- function(data, centers, cols='', iter.max=10) {
  if( missing(data) ) stop('must specify data')
  if(class(data) != 'H2OParsedData' ) stop('data must be an h2o dataset')
  
  if( missing(centers) ) stop('must specify centers')
  if(!is.numeric(centers) && !is.integer(centers)) stop('must specify centers')
  if( length(centers) > 1 || centers <= 0 ) stop("centers must be an integer greater than 0")
  if(!is.numeric(iter.max)) stop('iter.max must be numeric')
  if( iter.max < 1) stop('iter.max must be >=1')
  
  if(length(cols) == 1 && cols == '') cols = colnames(data)
  cc <- colnames(data)
  if(is.numeric(cols)) {
    if( any( cols < 1 | cols > length(cc) ) ) stop( paste(cols[ cols < 1 | cols > length(cc)], sep=','), 'is out of range of the columns' )
    cols <- cc[ cols ]
  }
  if( any(!cols %in% cc) ) stop("Invalid column names: ", paste(cols[which(!cols %in% cc)], collapse=", "))
  
  temp = setdiff(cc, cols)
  myIgnore <- ifelse(cols == '' || length(temp) == 0, '', paste(temp, sep=','))
  
  rand_kmeans_key = h2o.__uniqID("KMeansModel")
  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_KMEANS2, source=data@key, destination_key=rand_kmeans_key, ignored_cols=myIgnore, k=centers, max_iter=iter.max)
  while(h2o.__poll(data@h2o, res$job_key) != -1) { Sys.sleep(1) }
  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_KMModelView, model=rand_kmeans_key)
  res = res$model
  
  rand_pred_key = h2o.__uniqID("KMeansClusters")
  res2 = h2o.__remoteSend(data@h2o, h2o.__PAGE_PREDICT2, model=res$'_selfKey', data=data@key, prediction=rand_pred_key)
  res2 = h2o.__remoteSend(data@h2o, h2o.__PAGE_SUMMARY2, source=rand_pred_key, cols=0)
  
  result = list()
  result$cluster = new("H2OParsedData", h2o=data@h2o, key=rand_pred_key)
  feat = res$'_names'[-length(res$'_names')]     # Get rid of response column name
  result$centers = t(matrix(unlist(res$clusters), ncol = centers))
  dimnames(result$centers) = list(seq(1,centers), feat)
  result$withinss = res$cluster_variances        # TODO: Not sure if this is within or between SS?
  result$size = res2$summaries[[1]]$hcnt
  new("H2OKMeansModel", key=res$'_selfKey', data=data, model=result)
}

#------------------------------- Neural Network ----------------------------------#
h2o.nn <- function(x, y,  data, classification=T, activation='Tanh', layers=500, rate=0.01, regularization=1e-4, epoch=100, validation) {
  args <- verify_dataxy(data, x, y)

  if(!is.logical(classification)) stop('classification must be true or false')
  if(!is.character(activation)) stop('activation must be [Tanh, Rectifier]')
  if(!( activation %in% c('Tanh', 'Rectifier')) ) stop(paste('invalid activation', activation))
  if(!is.numeric(layers)) stop('layers must be numeric')
  if( layers < 1 ) stop('layers must be >= 1')
  if(!is.numeric(rate)) stop('rate must be numeric')
  if( rate < 0 ) stop('rate must be >= 1')
  if(!is.numeric(regularization)) stop('regularization must be numeric')
  if( regularization < 0 ) stop('regularization must be >= 1')
  if(!is.numeric(epoch)) stop('epoch must be numeric')
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
  return(res2)

  result=list()
  categories=length(res2$model$confusion_matrix)
  cf_matrix = t(matrix(unlist(res2$model$confusion_matrix), nrow=categories))
  cf_names <- res2$model[['_domains']]
  cf_names <- cf_names[[ length(cf_names) ]]
  dimnames(cf_matrix) = list(Actual = cf_names, Predicted = cf_names)

  result$confusion = cf_matrix
  # result$items = res2$model$items
  # result$train_class_error = res2$model$train_classification_error
  # result$train_sqr_error = res2$model$train_mse
  # result$valid_class_error = res2$model$validation_classification_error
  # result$valid_sqr_error = res2$model$validation_mse
  nn_train = tail(res2$model$training_errors,1)[[1]]     # BUG: For some reason, I'm not getting the errors for training_samples = 15000 in the JSON
  nn_valid = tail(res2$model$validation_errors,1)[[1]]
  result$train_class_error = nn_train$classification
  result$train_sqr_error = nn_train$mean_square
  result$valid_class_error = nn_valid$classification
  result$valid_sqr_error = nn_valid$mean_square
  new("H2ONNModel", key=destKey, data=data, model=result)
}

#------------------------------- Principal Components Analysis ----------------------------------#
h2o.prcomp.internal <- function(data, x_ignore, dest, max_pc=10000, tol=0, standardize=T) {
  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_PCA, source=data@key, ignored_cols_by_name=x_ignore, destination_key=dest, max_pc=max_pc, tolerance=tol, standardize=as.numeric(standardize))
  # while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
  while(h2o.__poll(data@h2o, res$job_key) != -1) { Sys.sleep(1) }
  destKey = res$destination_key
  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_PCAModelView, '_modelKey'=destKey)
  res = res$pca_model

  result = list()
  result$num_pc = res$num_pc
  result$standardized = standardize
  result$sdev = res$sdev
  nfeat = length(res$eigVec[[1]])
  temp = t(matrix(unlist(res$eigVec), nrow = nfeat))
  rownames(temp) = res$'_names'
  colnames(temp) = paste("PC", seq(1, ncol(temp)), sep="")
  result$rotation = temp
  new("H2OPCAModel", key=destKey, data=data, model=result)
}

h2o.prcomp <- function(data, tol=0, standardize=T, retx=F) {
  if( missing(data) ) stop('must specify data')
  if(class(data) != "H2OParsedData") stop('data must be an H2O FluidVec dataset')
  if(!is.numeric(tol)) stop('tol must be numeric')
  if(!is.logical(standardize)) stop('standardize must be TRUE or FALSE')
  if(!is.logical(retx)) stop('retx must be TRUE or FALSE')

  destKey = h2o.__uniqID("PCAModel")
  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_PCA, source=data@key, destination_key=destKey, tolerance=tol, standardize=as.numeric(standardize))
  while(h2o.__poll(data@h2o, res$job_key) != -1) { Sys.sleep(1) }
  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_PCAModelView, '_modelKey'=destKey)
  res = res$pca_model

  result = list()
  result$num_pc = res$num_pc
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
h2o.pcr <- function(x, y, data, ncomp, family, nfolds=10, alpha=0.5, lambda=1e-5, tweedie.p=ifelse(family=="tweedie", 0, as.numeric(NA))) {
  args <- verify_dataxy(data, x, y)

  if( !is.numeric(nfolds) ) stop('nfolds must be numeric')
  if( nfolds < 0 ) stop('nfolds must be >= 0')
  if( !is.numeric(alpha) ) stop('alpha must be numeric')
  if( alpha < 0 ) stop('alpha must be >= 0')
  if( !is.numeric(lambda) ) stop('lambda must be numeric')
  if( lambda < 0 ) stop('lambda must be >= 0')

  cc = colnames(data)
  y <- args$y
  if( ncomp < 1 || ncomp > length(cc) ) stop("Number of components must be between 1 and ", ncol(data))

  x_ignore <- args$x_ignore
  x_ignore <- ifelse( x_ignore=='', y, c(x_ignore,y) )
  myModel <- h2o.prcomp.internal(data=data, x_ignore=x_ignore, dest="", max_pc=ncomp, tol=0, standardize=TRUE)
  myScore <- h2o.predict(myModel)

  myScore[,ncomp+1] = data[,args$y_i]    # Bind response to frame of principal components
  myGLMData = new("H2OParsedData", h2o=data@h2o, key=myScore@key)
  h2o.glm.FV(1:ncomp, ncomp+1, myGLMData, family, nfolds, alpha, lambda, tweedie.p)
}

#-------------------------------------- Random Forest ----------------------------------------------#
h2o.randomForest <- function(x, y, data, ntree=50, depth=50, nodesize=1, sample.rate=2/3) {
  args <- verify_dataxy(data, x, y)
  
  if(!is.numeric(ntree)) stop('ntree must be a number')
  if( ntree < 1 ) stop('ntree must be >= 1')
  if(!is.numeric(depth)) stop('depth must be a number')
  if( depth < 1 ) stop('depth must be >= 1')
  if(!is.numeric(nodesize)) stop('nodesize must be a number')
  if( nodesize < 1 ) stop('nodesize must be >= 1')
  if(!is.numeric(sample.rate)) stop('sample.rate must be a number')
  if( sample.rate < 0 || sample.rate > 1 ) stop('sample.rate must be between 0 and 1')
  
  # NB: externally, 1 based indexing; internally, 0 based
  cols <- paste(args$x_i - 1,collapse=',')
  
  destKey = h2o.__uniqID("DRFModel")
  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_DRF, destination_key=destKey, source=data@key, response=args$y, cols=cols, ntrees=ntree, max_depth=depth, min_rows=nodesize, sample_rate=sample.rate)
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
  result$ntree = ntree
  new("H2ODRFModel", key=destKey, data=data, model=result)
}

#------------------------------- Prediction ----------------------------------------#
#setMethod("h2o.predict", signature(object="H2OModel", newdata="H2OParsedData"),
h2o.predict <- function(object, newdata) {
  if( missing(object) ) stop('must specify object')
  if(!( class(object) %in% c('H2OPCAModel', 'H2OGBMModel', 'H2OKMeansModel', 'H2OModel', 'H2OGLMModel', 'H2ODRFModel', 'H2OGLMModelVA') )) stop('object must be an H2OModel')
  if( missing(newdata) ) newdata <- object@data
  if(!class(newdata) %in% c('H2OParsedData', 'H2OParsedDataVA')) stop('newdata must be h2o data')

  if(class(object) == "H2OGLMModelVA") {
    if(class(newdata) != 'H2OParsedDataVA')
      stop("Prediction requires newdata to be type H2OParsedDataVA")
    res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_PREDICT, model_key=object@key, data_key=newdata@key)
    res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_INSPECT, key=res$response$redirect_request_args$key)
    new("H2OParsedDataVA", h2o=object@data@h2o, key=res$key)
  } else if(class(object) %in% c("H2OGBMModel", "H2OKMeansModel", "H2ODRFModel", "H2OGLMModel")) {
    # Set randomized prediction key
    key_prefix = switch(class(object), "H2OGBMModel" = "GBMPredict", "H2OKMeansModel" = "KMeansPredict",
                                       "H2ODRFModel" = "DRFPredict", "GLM2Predict")
    rand_pred_key = h2o.__uniqID(key_prefix)
    res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_PREDICT2, model=object@key, data=newdata@key, prediction=rand_pred_key)
    res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_INSPECT2, src_key=rand_pred_key)
    new("H2OParsedData", h2o=object@data@h2o, key=rand_pred_key)
  } else if(class(object) == "H2OPCAModel") {
    # Set randomized prediction key
    rand_pred_key = h2o.__uniqID("PCAPredict")
    numMatch = colnames(newdata) %in% rownames(object@model$rotation)
    numPC = min(length(numMatch[numMatch == TRUE]), object@model$num_pc)
    res = h2o.__remoteSend(object@data@h2o, h2o.__PAGE_PCASCORE, source=newdata@key, model=object@key, destination_key=rand_pred_key, num_pc=numPC)
    while(h2o.__poll(object@data@h2o, res$job_key) != -1) { Sys.sleep(1) }
    new("H2OParsedData", h2o=object@data@h2o, key=rand_pred_key)
  } else
    stop(paste("Prediction has not yet been implemented for", class(object)))
}

#------------------------------- ValueArray -------------------------------------#
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
    return(new("H2OGLMModelVA", key=destKey, data=data, model=modelOrig, xval=list()))
  
  res_xval = list()
  for(i in 1:nfolds) {
    xvalKey = resModel$validations[[1]]$xval_models[i]
    resX = h2o.__remoteSend(data@h2o, h2o.__PAGE_INSPECT, key=xvalKey)
    modelXval = h2o.__getGLMResults(resX$GLMModel, y, family, tweedie.p)
    res_xval[[i]] = new("H2OGLMModelVA", key=xvalKey, data=data, model=modelXval, xval=list())
  }
  new("H2OGLMModelVA", key=destKey, data=data, model=modelOrig, xval=res_xval)
}

h2o.glmgrid.internal <- function(x, y, data, family, nfolds, alpha, lambda) {
  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLMGrid, key = data@key, y = y, x = paste(x, sep="", collapse=","), family = family, n_folds = nfolds, alpha = alpha, lambda = lambda, case_mode="=", case=1.0, parallel= 1 )
  while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
  destKey = res$destination_key
  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLMGridProgress, destination_key=res$destination_key)
  allModels = res$models
  
  result = list()
  tweedie.p = as.numeric(NA)
  # result$Summary = t(sapply(res$models,c))
  for(i in 1:length(allModels)) {
    resH = h2o.__remoteSend(data@h2o, h2o.__PAGE_INSPECT, key=allModels[[i]]$key)
    modelOrig = h2o.__getGLMResults(resH$GLMModel, y, family, tweedie.p)
    
    if(nfolds < 2)
      result[[i]] = new("H2OGLMModelVA", key=allModels[[i]]$key, data=data, model=modelOrig, xval=list())
    else {
      res_xval = list()
      for(j in 1:nfolds) {
        xvalKey = resH$GLMModel$validations[[1]]$xval_models[j]
        resX = h2o.__remoteSend(data@h2o, h2o.__PAGE_INSPECT, key=xvalKey)
        modelXval = h2o.__getGLMResults(resX$GLMModel, y, family, tweedie.p)
        res_xval[[j]] = new("H2OGLMModelVA", key=xvalKey, data=data, model=modelXval, xval=list())
      }
      result[[i]] = new("H2OGLMModelVA", key=allModels[[i]]$key, data=data, model=modelOrig, xval=res_xval)
    }
  }
  new("H2OGLMGridVA", key=destKey, data=data, models=result, sumtable=allModels)
}

# Pretty formatting of H2O GLM results
h2o.__getGLMResults <- function(res, y, family, tweedie.p) {
  result = list()
  result$coefficients = unlist(res$coefficients)
  result$normalized_coefficients = unlist(res$normalized_coefficients)
  result$rank = res$nCols
  result$family = h2o.__getFamily(family, tweedie.var.p = tweedie.p)
  result$deviance = as.numeric(res$validations[[1]]$resDev)
  result$aic = as.numeric(res$validations[[1]]$aic)
  result$null.deviance = as.numeric(res$validations[[1]]$nullDev)
  result$iter = res$iterations
  result$df.residual = res$dof
  result$df.null = res$dof + result$rank
  result$train.err = as.numeric(res$validations[[1]]$err)
  result$y = y
  result$x = res$column_names
  # result$tweedie.p = ifelse(missing(tweedie.p), 'NA', tweedie.p)
  
  if(family == "binomial") {
    result$auc = as.numeric(res$validations[[1]]$auc)
    result$threshold = as.numeric(res$validations[[1]]$threshold)
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
  if(class(data) != "H2OParsedDataVA")
    stop("GLM currently only working under ValueArray. Please import data via h2o.importFile.VA or h2o.importFolder.VA")
  args <- verify_dataxy(data, x, y)
  if( nfolds < 0 ) stop('nfolds must be >= 0')
  if( alpha < 0 ) stop('alpha must be >= 0')
  if( lambda < 0 ) stop('lambda must be >= 0')
  if( epsilon < 0 ) stop('epsilon must be >= 0')
  if( !is.numeric(nfolds) ) stop('nfolds must be numeric')
  if( nfolds < 0 ) stop('nfolds must be >= 0')
  if( !is.logical(standardize) ) stop('standardize must be T or F')
  if( !is.numeric(tweedie.p) ) stop('tweedie.p must be numeric')
  
  # NB: externally, 1 based indexing; internally, 0 based
  if((missing(lambda) || length(lambda) == 1) && (missing(alpha) || length(alpha) == 1))
    h2o.glm.internal(args$x_i - 1, args$y, data, family, nfolds, alpha, lambda, 1, epsilon, standardize)
  else {
    if(!missing(tweedie.p)) print('Tweedie variance power not available in GLM grid search')
    h2o.glmgrid.internal(args$x_i - 1, args$y, data, family, nfolds, alpha, lambda)
  }
}

# Used to verify data, x, y and turn into the appropriate things
verify_dataxy <- function(data, x, y){
  if( missing(data) ) stop('must specify data')
  if(!class(data) %in% c("H2OParsedData", "H2OParsedDataVA")) stop('data must be an h2o dataset')

  if( missing(x) ) stop('must specify x')
  if( missing(y) ) stop('must specify y')
  if(!( class(x) %in% c('numeric', 'character', 'integer') )) stop('x must be column names or indices')
  if(!( class(y) %in% c('numeric', 'character', 'integer') )) stop('y must be a column name or index')

  cc <- colnames( data )
  if(is.character(x)) {
    if(any(!(x %in% cc))) stop(paste(paste(x[!(x %in% cc)], collapse=','), 'is not a valid column name'))
    x_i <- match(x, cc)
  } else {
    if(any( x < 1 | x > length(cc) )) stop(paste('Out of range explanatory variable', paste(x[x < 1 | x > length(cc)], collapse=',')))
    x_i <- x
    x <- cc[ x_i ]
  }

  if(is.character(y)){
    if(!( y %in% cc )) stop(paste(y, 'is not a column name'))
    y_i <- which(y == cc)
  } else {
    if( y < 1 || y > length(cc) ) stop(paste('Response variable index', y, 'is out of range'))
    y_i <- y
    y <- cc[ y ]
  }
  if( y %in% x ) stop(y, 'is both an explanatory and dependent variable')

  x_ignore <- setdiff(setdiff( cc, x ), y)
  if( length(x_ignore) == 0 ) x_ignore <- ''
  list(x=x, y=y, x_i=x_i, x_ignore=x_ignore, y_i=y_i)
}