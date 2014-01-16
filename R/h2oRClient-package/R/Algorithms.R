# Model-building operations and algorithms
# ----------------------- Generalized Boosting Machines (GBM) ----------------------- #
# TODO: don't support missing x; default to everything?
h2o.gbm <- function(x, y, distribution='multinomial', data, n.trees=10, interaction.depth=5, n.minobsinnode=10, shrinkage=0.02, n.bins=100, validation) {
  args <- verify_dataxy(data, x, y)

  if(!is.numeric(n.trees)) stop('n.trees must be numeric')
  if( any(n.trees < 1) ) stop('n.trees must be >= 1')
  if(!is.numeric(interaction.depth)) stop('interaction.depth must be numeric')
  if( any(interaction.depth < 1) ) stop('interaction.depth must be >= 1')
  if(!is.numeric(n.minobsinnode)) stop('n.minobsinnode must be numeric')
  if( any(n.minobsinnode < 1) ) stop('n.minobsinnode must be >= 1')
  if(!is.numeric(shrinkage)) stop('shrinkage must be numeric')
  if( any(shrinkage < 0 | shrinkage > 1) ) stop('shrinkage must be in [0,1]')
  if(!is.numeric(n.bins)) stop('n.bins must be numeric')
  if(any(n.bins < 1)) stop('n.bins must be >= 1')

  if(missing(validation)) validation = data
  else if(class(validation) != "H2OParsedData") stop("validation must be an H2O dataset")

  if( !(distribution %in% c('multinomial', 'gaussian')) )
    stop(paste(distribution, "is not a valid distribution; only [multinomial, gaussian] are supported"))
  classification <- ifelse(distribution == 'multinomial', 1, ifelse(distribution=='gaussian', 0, -1))

  # NB: externally, 1 based indexing; internally, 0 based
  cols = paste(args$x_i - 1, collapse=",")

  if(length(n.trees) == 1 && length(interaction.depth) == 1 && length(n.minobsinnode) == 1 && length(shrinkage) == 1) {
    # destKey = h2o.__uniqID("GBMModel")
    # res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GBM, destination_key=destKey, source=data@key, response=args$y, cols=cols, ntrees=n.trees, max_depth=interaction.depth, learn_rate=shrinkage, min_rows=n.minobsinnode, classification=classification, nbins=n.bins, validation=validation@key)
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GBM, source=data@key, response=args$y, cols=cols, ntrees=n.trees, max_depth=interaction.depth, learn_rate=shrinkage, min_rows=n.minobsinnode, classification=classification, nbins=n.bins, validation=validation@key)
    on.exit(h2o.__cancelJob(data@h2o, res$job_key))
    while(h2o.__poll(data@h2o, res$job_key) != -1) { Sys.sleep(1) }
    # while(!h2o.__isDone(data@h2o, "GBM", res)) { Sys.sleep(1) }
    res2 = h2o.__remoteSend(data@h2o, h2o.__PAGE_GBMModelView, '_modelKey'=res$destination_key)

    result = h2o.__getGBMResults(res2$gbm_model, distribution == 'multinomial')
    result$distribution <- distribution
    new("H2OGBMModel", key=res$destination_key, data=data, model=result, valid=validation)
  } else {
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GBM, source=data@key, response=args$y, cols=cols, ntrees=n.trees, max_depth=interaction.depth, learn_rate=shrinkage, min_rows=n.minobsinnode, classification=classification, nbins=n.bins, validation=validation@key)
    # h2o.gridsearch.internal("GBM", data, res$job_key, res$destination_key, validation, forGBMIsClassificationAndYesTheBloodyModelShouldReportIt= (distribution=='multinomial'))
    h2o.gridsearch.internal("GBM", data, res, validation, forGBMIsClassificationAndYesTheBloodyModelShouldReportIt = (distribution == 'multinomial'))
  }
}

h2o.__getGBMSummary <- function(res, isClassificationAndYesTheBloodyModelShouldReportIt) {
  mySum = list()
  mySum$model_key = res$'_selfKey'
  mySum$ntrees = res$N
  mySum$max_depth = res$max_depth
  mySum$min_rows = res$min_rows
  mySum$nbins = res$nbins
  mySum$learn_rate = res$learn_rate

  if( isClassificationAndYesTheBloodyModelShouldReportIt ){
    temp = matrix(unlist(res$cm), nrow = length(res$cm))
    mySum$prediction_error = 1-sum(diag(temp))/sum(temp)
  }
  return(mySum)
}

h2o.__getGBMResults <- function(res, isClassificationAndYesTheBloodyModelShouldReportIt) {
  result = list()
  if( isClassificationAndYesTheBloodyModelShouldReportIt ){
    result$confusion = build_cm(tail(res$cm, 1)[[1]], tail(res$'_domains', 1)[[1]])  #res$'_domains'[[length(res$'_domains')]])
    result$classification <- T
  } else
    result$classification <- F

  result$err = res$errs
  return(result)
}

# -------------------------- Generalized Linear Models (GLM) ------------------------ #
h2o.glm.FV <- function(x, y, data, family, nfolds = 10, alpha = 0.5, lambda = 1.0e-5, tweedie.p = ifelse(family == "tweedie", 0, as.numeric(NA))) {
  args <- verify_dataxy(data, x, y)

  if(!is.numeric(nfolds)) stop('nfolds must be numeric')
  if( nfolds < 0 ) stop('nfolds must be >= 0')
  if(!is.numeric(alpha)) stop('alpha must be numeric')
  if( any(alpha < 0) ) stop('alpha must be >= 0')
  if(!is.numeric(lambda)) stop('lambda must be numeric')
  if( any(lambda < 0) ) stop('lambda must be >= 0')
  if(!is.numeric(tweedie.p)) stop('tweedie.p must be numeric')
  if( family != 'tweedie' && !(missing(tweedie.p) || is.na(tweedie.p)) ) stop("tweedie.p may only be set for family tweedie")

  x_ignore = setdiff(1:ncol(data), c(args$x_i, args$y_i)) - 1
  if(length(x_ignore) == 0) x_ignore = ''

  if(length(alpha) == 1) {
    rand_glm_key = h2o.__uniqID("GLM2Model")
    if(family != "tweedie")
      res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLM2, source = data@key, destination_key = rand_glm_key, response = args$y, ignored_cols = paste(x_ignore, sep="", collapse=","), family = family, n_folds = nfolds, alpha = alpha, lambda = lambda, standardize = as.numeric(FALSE))
    else
      res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLM2, source = data@key, destination_key = rand_glm_key, response = args$y, ignored_cols = paste(x_ignore, sep="", collapse=","), family = family, n_folds = nfolds, alpha = alpha, lambda = lambda, tweedie_variance_power = tweedie.p, standardize = as.numeric(FALSE))
    on.exit(h2o.__cancelJob(data@h2o, res$job_key))
    while(h2o.__poll(data@h2o, res$job_key) != -1) { Sys.sleep(1) }
    # while(!h2o.__isDone(data@h2o, "GLM2", res)) { Sys.sleep(1) }

    res2 = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLMModelView, '_modelKey'=res$destination_key)
    resModel = res2$glm_model; destKey = resModel$'_selfKey'
    modelOrig = h2o.__getGLM2Results(resModel, x, y)

    # Get results from cross-validation
    if(nfolds < 2)
      return(new("H2OGLMModel", key=destKey, data=data, model=modelOrig, xval=list()))

    res_xval = list()
    for(i in 1:nfolds) {
      xvalKey = resModel$submodels[[resModel$best_lambda_idx+1]]$validation$xval_models
      resX = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLMModelView, '_modelKey'=xvalKey[i])
      modelXval = h2o.__getGLM2Results(resX$glm_model, x, y)
      res_xval[[i]] = new("H2OGLMModel", key=xvalKey[i], data=data, model=modelXval, xval=list())
    }
    new("H2OGLMModel", key=destKey, data=data, model=modelOrig, xval=res_xval)
  } else
    h2o.glm2grid.internal(x_ignore, args$y, data, family, nfolds, alpha, lambda, tweedie.p)
}

h2o.glm2grid.internal <- function(x_ignore, y, data, family, nfolds, alpha, lambda, tweedie.p) {
  if(family != "tweedie")
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLM2, source = data@key, response = y, ignored_cols = paste(x_ignore, sep="", collapse=","), family = family, n_folds = nfolds, alpha = alpha, lambda = lambda, standardize = as.numeric(FALSE))
  else
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLM2, source = data@key, response = y, ignored_cols = paste(x_ignore, sep="", collapse=","), family = family, n_folds = nfolds, alpha = alpha, lambda = lambda, tweedie_variance_power = tweedie.p, standardize = as.numeric(FALSE))
  
  pb = txtProgressBar(style = 3)
  while((prog = h2o.__poll(data@h2o, res$job_key)) != -1) { Sys.sleep(1); setTxtProgressBar(pb, prog) }
  # while(!h2o.__isDone(data@h2o, "GLM2", res)) { Sys.sleep(1); prog = h2o.__poll(data@h2o, res$job_key); setTxtProgressBar(pb, prog) }
  setTxtProgressBar(pb, 1.0); close(pb)

  res2 = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLM2GridView, grid_key=res$destination_key)
  destKey = res$destination_key
  allModels = res2$grid$destination_keys

  result = list(); myModelSum = list()
  for(i in 1:length(allModels)) {
    resH = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLMModelView, '_modelKey'=allModels[i])
    myModelSum[[i]] = h2o.__getGLM2Summary(resH$glm_model)
    x = colnames(data)[-(x_ignore+1)]
    modelOrig = h2o.__getGLM2Results(resH$glm_model, x, y)

    # Get results from cross-validation
    if(nfolds < 2)
      result[[i]] = new("H2OGLMModel", key=allModels[i], data=data, model=modelOrig, xval=list())
    else {
      res_xval = list()
      for(j in 1:nfolds) {
         xvalKey = resH$glm_model$submodels[[resH$glm_model$best_lambda_idx+1]]$validation$xval_models
         resX = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLMModelView, '_modelKey'=xvalKey[j])
         modelXval = h2o.__getGLM2Results(resX$glm_model, x, y)
         res_xval[[j]] = new("H2OGLMModel", key=xvalKey, data=data, model=modelXval, xval=list())
      }
      result[[i]] = new("H2OGLMModel", key=allModels[i], data=data, model=modelOrig, xval=res_xval)
    }
  }
  new("H2OGLMGrid", key=destKey, data=data, model=result, sumtable=myModelSum)
}

h2o.__getGLM2Summary <- function(model) {
  mySum = list()
  mySum$model_key = model$'_selfKey'
  mySum$alpha = model$alpha
  mySum$lambda_min = min(model$lambda)
  mySum$lambda_max = max(model$lambda)
  mySum$lambda_best = model$lambda[model$best_lambda_idx+1]
  
  submod = model$submodels[[model$best_lambda_idx+1]]
  mySum$iterations = submod$iteration
  valid = submod$validation
  
  if(model$glm$family == "binomial")
    mySum$auc = as.numeric(valid$auc)
  mySum$aic = as.numeric(valid$aic)
  mySum$dev_explained = 1-as.numeric(valid$residual_deviance)/as.numeric(valid$null_deviance)
  return(mySum)
}

# Pretty formatting of H2O GLM2 results
h2o.__getGLM2Results <- function(model, x, y) {
  submod = model$submodels[[model$best_lambda_idx+1]]
  valid = submod$validation

  result = list()
  result$y = y
  result$x = x
  result$coefficients = as.numeric(unlist(submod$beta))
  result$normalized_coefficients = as.numeric(unlist(submod$norm_beta))
  names(result$coefficients) = model$coefficients_names
  result$rank = valid$'_rank'
  if(model$glm$family == "tweedie")
    result$family = h2o.__getFamily(model$glm$family, model$glm$link, model$glm$tweedie_variance_power, model$glm$tweedie_link_power)
  else
    result$family = h2o.__getFamily(model$glm$family, model$glm$link)
  result$iter = submod$iteration

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
    cm_ind = trunc(100*result$best_threshold) + 2
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

# ------------------------------ K-Means Clustering --------------------------------- #
h2o.kmeans <- function(data, centers, cols='', iter.max=10) {
  if( missing(data) ) stop('must specify data')
  if(class(data) != 'H2OParsedData' ) stop('data must be an h2o dataset')

  if( missing(centers) ) stop('must specify centers')
  if(!is.numeric(centers) && !is.integer(centers)) stop('must specify centers')
  if( any(centers < 1) ) stop("centers must be an integer greater than 0")
  if(!is.numeric(iter.max)) stop('iter.max must be numeric')
  if( any(iter.max < 1)) stop('iter.max must be >= 1')

  if(length(cols) == 1 && cols == '') cols = colnames(data)
  cc <- colnames(data)
  if(is.numeric(cols)) {
    if( any( cols < 1 | cols > length(cc) ) ) stop( paste(cols[ cols < 1 | cols > length(cc)], sep=','), 'is out of range of the columns' )
    cols <- cc[ cols ]
  }
  if( any(!cols %in% cc) ) stop("Invalid column names: ", paste(cols[which(!cols %in% cc)], collapse=", "))

  temp = setdiff(cc, cols)
  myIgnore <- ifelse(cols == '' || length(temp) == 0, '', paste(temp, sep=','))

  if(length(centers) == 1 && length(iter.max) == 1) {
    # rand_kmeans_key = h2o.__uniqID("KMeansModel")
    # res = h2o.__remoteSend(data@h2o, h2o.__PAGE_KMEANS2, source=data@key, destination_key=rand_kmeans_key, ignored_cols=myIgnore, k=centers, max_iter=iter.max)
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_KMEANS2, source=data@key, ignored_cols=myIgnore, k=centers, max_iter=iter.max)
    on.exit(h2o.__cancelJob(data@h2o, res$job_key))
    while(h2o.__poll(data@h2o, res$job_key) != -1) { Sys.sleep(1) }
    # while(!h2o.__isDone(data@h2o, "KM", res)) { Sys.sleep(1) }
    res2 = h2o.__remoteSend(data@h2o, h2o.__PAGE_KM2ModelView, model=res$destination_key)
    res2 = res2$model

    result = h2o.__getKMResults(res2, data) #, centers)
    new("H2OKMeansModel", key=res2$'_selfKey', data=data, model=result)
  } else {
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_KMEANS2, source=data@key, ignored_cols=myIgnore, k=centers, max_iter=iter.max)
    # h2o.gridsearch.internal("KM", data, res$job_key, res$destination_key)
    h2o.gridsearch.internal("KM", data, res)
  }
}

h2o.__getKMSummary <- function(res) {
  mySum = list()
  mySum$model_key = res$'_selfKey'
  mySum$k = res$k
  mySum$max_iter = res$iterations
  mySum$error = res$error
  return(mySum)
}

h2o.__getKMResults <- function(res, data) {
  #rand_pred_key = h2o.__uniqID("KMeansClusters")
  #res2 = h2o.__remoteSend(data@h2o, h2o.__PAGE_PREDICT2, model=res$'_selfKey', data=data@key, prediction=rand_pred_key)
  #res2 = h2o.__remoteSend(data@h2o, h2o.__PAGE_SUMMARY2, source=rand_pred_key, cols=0)

  clusters_key <- paste(res$'_clustersKey', sep = "")
  result = list()
  result$cluster = new("H2OParsedData", h2o=data@h2o, key=clusters_key)
  feat = res$'_names'[-length(res$'_names')]     # Get rid of response column name
  result$centers = t(matrix(unlist(res$centers), ncol = res$k))
  dimnames(result$centers) = list(seq(1,res$k), feat)
  result$totss <- res$total_SS
  result$withinss <- res$within_cluster_variances
  result$tot.withinss <- res$total_within_SS
  result$betweenss <- res$between_cluster_SS
  result$size <- res$size
  #result$size = res2$summaries[[1]]$hcnt
  return(result)
}

# ------------------------------- Neural Network ------------------------------------ #
h2o.nn <- function(x, y,  data, classification=T, activation='Tanh', layers=500, rate=0.01, l1_reg=1e-4, l2_reg=0.0010, epoch=100, validation) {
  args <- verify_dataxy(data, x, y)

  if(!is.logical(classification)) stop('classification must be true or false')
  if(!is.character(activation)) stop('activation must be [Tanh, Rectifier]')
  if(!(activation %in% c('Tanh', 'Rectifier')) ) stop(paste('invalid activation', activation))
  if(!is.numeric(layers)) stop('layers must be numeric')
  if( any(layers < 1) ) stop('layers must be >= 1')
  if(!is.numeric(rate)) stop('rate must be numeric')
  if( any(rate < 0) ) stop('rate must be >= 1')
  if(!is.numeric(l1_reg)) stop('l1_reg must be numeric')
  if( any(l1_reg < 0) ) stop('l1_reg must be >= 0')
  if(!is.numeric(l2_reg)) stop('l2_reg must be numeric')
  if( any(l2_reg < 0) ) stop('l2_reg must be >= 0')
  if(!is.numeric(epoch)) stop('epoch must be numeric')
  if( any(epoch < 0) ) stop('epoch must be >= 1')

  if(missing(validation)) validation = data
  if(class(validation) != "H2OParsedData") stop("validation must be an H2O dataset")

  if(!(activation %in% c('Tanh', 'Rectifier')) )
    stop(paste(activation, "is not a valid activation; only [Tanh, Rectifier] are supported"))
  if(!(classification %in% c(0, 1)) )
    stop(paste(classification, "is not a valid classification index; only [0,1] are supported"))

  # BUG: Resulting numbers are off from browser. I don't think the algorithm is finished even when progress = -1
  if(length(layers) == 1 && length(rate) == 1 && length(l1_reg) == 1 && length(l2_reg) == 1 && length(epoch) == 1) {
    # destKey = h2o.__uniqID("NNModel")
    # res = h2o.__remoteSend(data@h2o, h2o.__PAGE_NN, destination_key=destKey, source=data@key, response=args$y, cols=paste(args$x_i - 1, collapse=','),
    #    classification=as.numeric(classification), activation=activation, rate=rate,
    #    hidden=paste(layers, sep="", collapse=","), l1=l1_reg, l2=l2_reg, epochs=epoch, validation=validation@key)
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_NN, source=data@key, response=args$y, cols=paste(args$x_i - 1, collapse=','),
        classification=as.numeric(classification), activation=activation, rate=rate,
        hidden=paste(layers, sep="", collapse=","), l1=l1_reg, l2=l2_reg, epochs=epoch, validation=validation@key)
    on.exit(h2o.__cancelJob(data@h2o, res$job_key))
    while(h2o.__poll(data@h2o, res$job_key) != -1) { Sys.sleep(1) }
    # while(!h2o.__isDone(data@h2o, "NN", res)) { Sys.sleep(1) }
    res2 = h2o.__remoteSend(data@h2o, h2o.__PAGE_NNProgress, destination_key=res$destination_key)

    result = h2o.__getNNResults(res2)
    new("H2ONNModel", key=res$destination_key, data=data, model=result, valid=validation)
  } else {
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_NN, source=data@key, response=args$y, cols=paste(args$x_i - 1, collapse=','),
                           classification=as.numeric(classification), activation=activation, rate=rate,
                           hidden=paste(layers, sep="", collapse=","), l1=l1_reg, l2=l2_reg, epochs=epoch, validation=validation@key)
    # h2o.gridsearch.internal("NN", data, res$job_key, res$destination_key, validation)
    h2o.gridsearch.internal("NN", data, res, validation)
  }
}

h2o.__getNNSummary <- function(res) {
  mySum = list()
  mySum$model_key = res$destination_key
  mySum$activation = res$activation
  mySum$hidden = res$hidden
  mySum$rate = res$rate
  mySum$rate_annealing = res$rate_annealing
  mySum$momentum_start = res$momentum_start
  mySum$momentum_ramp = res$momentum_ramp
  mySum$momentum_stable = res$momentum_stable
  mySum$l1_reg = res$l1
  mySum$l2_reg = res$l2
  mySum$epochs = res$epochs

  temp = matrix(unlist(res$confusion_matrix), nrow = length(res$confusion_matrix))
  mySum$prediction_error = 1-sum(diag(temp))/sum(temp)
  return(mySum)
}

h2o.__getNNResults <- function(res) {
  result = list()
  result$confusion = build_cm(res$confusion_matrix, res$class_names)
  nn_train = tail(res$training_errors,1)[[1]]
  nn_valid = tail(res$validation_errors,1)[[1]]
  result$train_class_error = nn_train$classification
  result$train_sqr_error = nn_train$mean_square
  result$valid_class_error = nn_valid$classification
  result$valid_sqr_error = nn_valid$mean_square
  return(result)
}

# ----------------------- Principal Components Analysis ----------------------------- #
h2o.prcomp.internal <- function(data, x_ignore, dest, max_pc=10000, tol=0, standardize=T) {
  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_PCA, source=data@key, ignored_cols_by_name=x_ignore, destination_key=dest, max_pc=max_pc, tolerance=tol, standardize=as.numeric(standardize))
  on.exit(h2o.__cancelJob(data@h2o, res$job_key))
  while(h2o.__poll(data@h2o, res$job_key) != -1) { Sys.sleep(1) }
  # while(!h2o.__isDone(data@h2o, "PCA", res)) { Sys.sleep(1) }
  destKey = res$destination_key
  res2 = h2o.__remoteSend(data@h2o, h2o.__PAGE_PCAModelView, '_modelKey'=destKey)
  res2 = res2$pca_model

  result = list()
  result$num_pc = res2$num_pc
  result$standardized = standardize
  result$sdev = res2$sdev
  nfeat = length(res2$eigVec[[1]])
  temp = t(matrix(unlist(res2$eigVec), nrow = nfeat))
  rownames(temp) = res2$'_names'
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
  on.exit(h2o.__cancelJob(data@h2o, res$job_key))
  while(h2o.__poll(data@h2o, res$job_key) != -1) { Sys.sleep(1) }
  # while(!h2o.__isDone(data@h2o, "PCA", res)) { Sys.sleep(1) }
  res2 = h2o.__remoteSend(data@h2o, h2o.__PAGE_PCAModelView, '_modelKey'=destKey)
  res2 = res2$pca_model

  result = list()
  result$num_pc = res2$num_pc
  result$standardized = standardize
  result$sdev = res2$sdev
  nfeat = length(res2$eigVec[[1]])
  temp = t(matrix(unlist(res2$eigVec), nrow = nfeat))
  rownames(temp) = res2$'_names'
  colnames(temp) = paste("PC", seq(1, ncol(temp)), sep="")
  result$rotation = temp

  if(retx) result$x = h2o.predict(new("H2OPCAModel", key=destKey, data=data, model=result))
  new("H2OPCAModel", key=destKey, data=data, model=result)
}

# setGeneric("h2o.pcr", function(x, y, data, ncomp, family, nfolds = 10, alpha = 0.5, lambda = 1.0e-5, tweedie.p = ifelse(family=="tweedie", 0, NA)) { standardGeneric("h2o.pcr") })
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

# ----------------------------------- Random Forest --------------------------------- #
h2o.randomForest <- function(x, y, data, ntree=50, depth=50, nodesize=1, sample.rate=2/3, nbins=100, seed=-1, validation) {
  args <- verify_dataxy(data, x, y)

  if(!is.numeric(ntree)) stop('ntree must be a number')
  if( any(ntree < 1) ) stop('ntree must be >= 1')
  if(!is.numeric(depth)) stop('depth must be a number')
  if( any(depth < 1) ) stop('depth must be >= 1')
  if(!is.numeric(nodesize)) stop('nodesize must be a number')
  if( any(nodesize < 1) ) stop('nodesize must be >= 1')
  if(!is.numeric(sample.rate)) stop('sample.rate must be a number')
  if( any(sample.rate < 0 || sample.rate > 1) ) stop('sample.rate must be between 0 and 1')
  if(!is.numeric(nbins)) stop('nbins must be a number')
  if( any(nbins < 1)) stop('nbins must be an integer >= 1')
  if(!is.numeric(seed)) stop("seed must be an integer >= 0")

  if(missing(validation)) validation = data
  else if(class(validation) != "H2OParsedData") stop("validation must be an H2O dataset")

  # NB: externally, 1 based indexing; internally, 0 based
  cols <- paste(args$x_i - 1, collapse=',')

  if(length(ntree) == 1 && length(depth) == 1 && length(nodesize) == 1 && length(sample.rate) == 1 && length(nbins) == 1) {
    # destKey = h2o.__uniqID("DRFModel")
    # res = h2o.__remoteSend(data@h2o, h2o.__PAGE_DRF, destination_key=destKey, source=data@key, response=args$y, cols=cols, ntrees=ntree, max_depth=depth, min_rows=nodesize, sample_rate=sample.rate, nbins=nbins)
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_DRF, source=data@key, response=args$y, cols=cols, ntrees=ntree, max_depth=depth, min_rows=nodesize, sample_rate=sample.rate, nbins=nbins, seed=seed)
    on.exit(h2o.__cancelJob(data@h2o, res$job_key))
    while(h2o.__poll(data@h2o, res$job_key) != -1) { Sys.sleep(1) }
    # while(!h2o.__isDone(data@h2o, "RF2", res)) { Sys.sleep(1) }
    res2 = h2o.__remoteSend(data@h2o, h2o.__PAGE_DRFModelView, '_modelKey'=res$destination_key)

    result = h2o.__getDRFResults(res2$drf_model)
    new("H2ODRFModel", key=res$destination_key, data=data, model=result, valid=validation)
  } else {
    res = h2o.__remoteSend(data@h2o, h2o.__PAGE_DRF, source=data@key, response=args$y, cols=cols, ntrees=ntree, max_depth=depth, min_rows=nodesize, sample_rate=sample.rate, nbins=nbins, seed=seed)
    # h2o.gridsearch.internal("RF", data, res$job_key, res$destination_key, validation)
    h2o.gridsearch.internal("RF", data, res, validation)
  }
}

h2o.__getDRFSummary <- function(res) {
  mySum = list()
  mySum$model_key = res$'_selfKey'
  mySum$ntrees = res$N
  mySum$max_depth = res$max_depth
  mySum$min_rows = res$min_rows
  mySum$nbins = res$nbins

  temp = matrix(unlist(res$cm), nrow = length(res$cm))
  mySum$prediction_error = 1-sum(diag(temp))/sum(temp)
  return(mySum)
}

h2o.__getDRFResults <- function(res) {
  result = list()
  treeStats = unlist(res$treeStats)
  rf_matrix = rbind(treeStats[1:3], treeStats[4:6])
  colnames(rf_matrix) = c("Min.", "Max.", "Mean.")
  rownames(rf_matrix) = c("Depth", "Leaves")
  result$forest = rf_matrix

  result$confusion = build_cm(tail(res$cm, 1)[[1]], tail(res$'_domains', 1)[[1]])  #res$'_domains'[[length(res$'_domains')]])
  result$mse = res$errs
  result$ntree = res$N
  return(result)
}

# ------------------------------- Prediction ---------------------------------------- #
#setMethod("h2o.predict", signature(object="H2OModel", newdata="H2OParsedData"),
h2o.predict <- function(object, newdata) {
  if( missing(object) ) stop('must specify object')
  if(!( class(object) %in% c('H2OPCAModel', 'H2OGBMModel', 'H2OKMeansModel', 'H2OModel', 'H2OGLMModel', 'H2ODRFModel', 'H2OGLMModelVA', 'H2ORFModelVA') )) stop('object must be an H2OModel')
  if( missing(newdata) ) newdata <- object@data
  if(!class(newdata) %in% c('H2OParsedData', 'H2OParsedDataVA')) stop('newdata must be h2o data')

  if(class(object) %in% c("H2OGLMModelVA", "H2ORFModelVA")) {
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
    on.exit(h2o.__cancelJob(data@h2o, res$job_key))
    while(h2o.__poll(object@data@h2o, res$job_key) != -1) { Sys.sleep(1) }   # TODO: Replace with poll of Progress2 page
    new("H2OParsedData", h2o=object@data@h2o, key=rand_pred_key)
  } else
    stop(paste("Prediction has not yet been implemented for", class(object)))
}

# --------------------------------- ValueArray -------------------------------------- #
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
  # while(!h2o.__isDone(data@h2o, "GLM1", res)) { Sys.sleep(1) }
  destKey = res$destination_key
  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_INSPECT, key=destKey)
  resModel = res$GLMModel
  
  # Check for any warnings
  if(!is.null(resModel$warnings) && length(resModel$warnings) > 0) {
    for(i in 1:length(resModel$warnings))
      warning(resModel$warnings[[i]])
  }
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
  # while(!h2o.__isDone(data@h2o, "GLM1Grid", res)) { Sys.sleep(1) }
  destKey = res$destination_key
  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_GLMGridProgress, destination_key=res$destination_key)
  allModels = res$models

  result = list()
  tweedie.p = as.numeric(NA)
  # result$Summary = t(sapply(res$models,c))
  for(i in 1:length(allModels)) {
    resH = h2o.__remoteSend(data@h2o, h2o.__PAGE_INSPECT, key=allModels[[i]]$key)
    
    # Check for any warnings
    if(!is.null(resH$GLMModel$warnings) && length(resH$GLMModel$warnings) > 0) {
      for(j in 1:length(resH$GLMModel$warnings))
        warning("Model ", allModels[[i]]$key, ": ", resH$GLMModel$warnings[[j]])
    }
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
  new("H2OGLMGridVA", key=destKey, data=data, model=result, sumtable=allModels)
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
  if( any(alpha < 0) ) stop('alpha must be >= 0')
  if( any(lambda < 0) ) stop('lambda must be >= 0')
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

h2o.randomForest.VA <- function(x, y, data, ntree=50, depth=50, sample.rate=2/3, classwt=NULL, seed=-1, use_non_local=T) {
  if(class(data) != "H2OParsedDataVA")
    stop("h2o.randomForest.VA only works under ValueArray. Please import data via h2o.importFile.VA or h2o.importFolder.VA")
  args <- verify_dataxy(data, x, y)
  if(!is.numeric(ntree)) stop("ntree must be numeric")
  if(ntree <= 0) stop("ntree must be > 0")
  if(!is.numeric(depth)) stop("depth must be numeric")
  if(depth < 0) stop("depth must be >= 0")
  if(!is.numeric(sample.rate)) stop("sample.rate must be numeric")
  if(sample.rate < 0 || sample.rate > 1) stop("sample.rate must be in [0,1]")
  if(!is.numeric(classwt) && !is.null(classwt)) stop("classwt must be numeric")
  if(!is.numeric(seed)) stop("seed must be an integer >= 0")
  if(!is.logical(use_non_local)) stop("use_non_local must be logical indicating whether to use non-local data")

  res = h2o.__remoteSend(data@h2o, h2o.__PAGE_RF, data_key=data@key, response_variable=args$y, ignore=args$x_ignore, ntree=ntree, depth=depth, sample=round(100*sample.rate), class_weights=classwt, seed=seed, use_non_local_data=as.numeric(use_non_local))
  while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
  # while(!h2o.__isDone(data@h2o, "RF1", res)) { Sys.sleep(1) }
  res2 = h2o.__remoteSend(data@h2o, h2o.__PAGE_RFVIEW, model_key=res$destination_key, data_key=data@key, response_variable=args$y, out_of_bag_error_estimate=1)
  modelOrig = h2o.__getRFResults(res2)
  new("H2ORFModelVA", key=res$destination_key, data=data, model=modelOrig)
}

h2o.__getRFResults <- function(model) {
  result = list()
  result$ntree = model$ntree
  result$classification_error = model$confusion_matrix$classification_error
  result$confusion = build_cm(model$confusion_matrix$scores, model$confusion_matrix$header)
  result$depth_sum = unlist(model$trees$depth)
  result$leaves_sum = unlist(model$trees$leaves)
  result$tree_sum = matrix(c(model$trees$depth, model$trees$leaves), nrow=2, dimnames=list(c("Depth", "Leaves"), c("Min", "Mean", "Max")))
  return(result)
}

# Used to verify data, x, y and turn into the appropriate things
verify_dataxy <- function(data, x, y) {
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

# h2o.gridsearch.internal <- function(algo, data, job_key, dest_key, validation = NULL, forGBMIsClassificationAndYesTheBloodyModelShouldReportIt=T) {
h2o.gridsearch.internal <- function(algo, data, response, validation = NULL, forGBMIsClassificationAndYesTheBloodyModelShouldReportIt = TRUE) {
  if(!algo %in% c("GBM", "KM", "RF", "NN")) stop("General grid search not supported for ", algo)
  job_key = response$job_key; dest_key = response$destination_key
  on.exit(h2o.__cancelJob(data@h2o, job_key))
  prog_view = switch(algo, GBM = h2o.__PAGE_GBMProgress, KM = h2o.__PAGE_KM2Progress, RF = h2o.__PAGE_DRFProgress, NN = h2o.__PAGE_NNProgress)
  
  pb = txtProgressBar(style = 3)
  while((prog = h2o.__poll(data@h2o, job_key)) != -1) { Sys.sleep(1); setTxtProgressBar(pb, prog) }
  # while(!h2o.__isDone(data@h2o, algo, response)) { Sys.sleep(1); prog = h2o.__poll(data@h2o, job_key); setTxtProgressBar(pb, prog) }
  setTxtProgressBar(pb, 1.0); close(pb)

  res2 = h2o.__remoteSend(data@h2o, h2o.__PAGE_GRIDSEARCH, job_key=job_key, destination_key=dest_key)
  allModels = res2$jobs

  model_obj = switch(algo, GBM = "H2OGBMModel", KM = "H2OKMeansModel", RF = "H2ODRFModel", NN = "H2ONNModel")
  grid_obj = switch(algo, GBM = "H2OGBMGrid", KM = "H2OKMeansGrid", RF = "H2ODRFGrid", NN = "H2ONNGrid")
  model_view = switch(algo, GBM = h2o.__PAGE_GBMModelView, KM = h2o.__PAGE_KM2ModelView, RF = h2o.__PAGE_DRFModelView, NN = h2o.__PAGE_NNProgress)

  result = list(); myModelSum = list()
  for(i in 1:length(allModels)) {
    if(algo == "KM")
      resH = h2o.__remoteSend(data@h2o, model_view, model=allModels[[i]]$destination_key)
    else if(algo == "NN")
      resH = h2o.__remoteSend(data@h2o, model_view, job_key=allModels[[i]]$job_key, destination_key=allModels[[i]]$destination_key)
    else
      resH = h2o.__remoteSend(data@h2o, model_view, '_modelKey'=allModels[[i]]$destination_key)
    myModelSum[[i]] = switch(algo, GBM = h2o.__getGBMSummary(resH[[3]],forGBMIsClassificationAndYesTheBloodyModelShouldReportIt), KM = h2o.__getKMSummary(resH[[3]]), RF = h2o.__getDRFSummary(resH[[3]]), NN = h2o.__getNNSummary(resH))
    myModelSum[[i]]$run_time = allModels[[i]]$end_time - allModels[[i]]$start_time
    modelOrig = switch(algo, GBM = h2o.__getGBMResults(resH[[3]],forGBMIsClassificationAndYesTheBloodyModelShouldReportIt), KM = h2o.__getKMResults(resH[[3]], data), RF = h2o.__getDRFResults(resH[[3]]), NN = h2o.__getNNResults(resH))

    if(algo == "KM")
      result[[i]] = new(model_obj, key=allModels[[i]]$destination_key, data=data, model=modelOrig)
    else
      result[[i]] = new(model_obj, key=allModels[[i]]$destination_key, data=data, model=modelOrig, valid=validation)
  }
  new(grid_obj, key=dest_key, data=data, model=result, sumtable=myModelSum)
}

build_cm <- function(cm, cf_names) {
  #browser()
  categories = length(cm)
  cf_matrix = t(matrix(unlist(cm), nrow=categories))

  cf_total = apply(cf_matrix, 2, sum)
  # cf_error = c(apply(cf_matrix, 1, sum)/diag(cf_matrix)-1, 1-sum(diag(cf_matrix))/sum(cf_matrix))
  cf_error = c(1-diag(cf_matrix)/apply(cf_matrix,1,sum), 1-sum(diag(cf_matrix))/sum(cf_matrix))
  cf_matrix = rbind(cf_matrix, cf_total)
  cf_matrix = cbind(cf_matrix, round(cf_error, 3))

  # dimnames(cf_matrix) = list(Actual = cf_names, Predicted = cf_names)
  dimnames(cf_matrix) = list(Actual = c(cf_names, "Totals"), Predicted = c(cf_names, "Error"))
  return(cf_matrix)
}

