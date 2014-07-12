# Model-building operations and algorithms
# ----------------------- Generalized Boosting Machines (GBM) ----------------------- #
# TODO: don't support missing x; default to everything?
h2o.gbm <- function(x, y, distribution = 'multinomial', data, n.trees = 10, interaction.depth = 5, n.minobsinnode = 10, shrinkage = 0.1,
                    n.bins = 100, importance = FALSE, nfolds = 0, validation, balance.classes = FALSE, max.after.balance.size = 5) {
  args <- .verify_dataxy(data, x, y)
  
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
  if(!is.logical(importance)) stop('importance must be logical (TRUE or FALSE)')
  
  if(!(distribution %in% c('multinomial', 'gaussian', 'bernoulli')))
    stop(paste(distribution, "is not a valid distribution; only [multinomial, gaussian, bernoulli] are supported"))
  classification <- ifelse(distribution %in% c('multinomial', 'bernoulli'), 1, ifelse(distribution=='gaussian', 0, -1))
  family <- ifelse(distribution == "bernoulli", "bernoulli", "AUTO")
  
  if(!is.logical(balance.classes)) stop('balance.classes must be logical (TRUE or FALSE)')
  if(!is.numeric(max.after.balance.size)) stop('max.after.balance.size must be a number')
  if( any(max.after.balance.size <= 0) ) stop('max.after.balance.size must be >= 0')
  if(balance.classes && !classification) stop('balance.classes can only be used for classification')
  
  if(!is.numeric(nfolds)) stop("nfolds must be numeric")
  if(nfolds == 1) stop("nfolds cannot be 1")
  if(!missing(validation) && class(validation) != "H2OParsedData")
    stop("validation must be an H2O parsed dataset")
  
  # NB: externally, 1 based indexing; internally, 0 based
  cols = paste(args$x_i - 1, collapse=",")
  if(missing(validation) && nfolds == 0) {
    # Default to using training data as validation
    validation = data
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GBM, source=data@key, response=args$y, cols=cols, ntrees=n.trees, max_depth=interaction.depth, learn_rate=shrinkage, family=family,
                            min_rows=n.minobsinnode, classification=classification, nbins=n.bins, importance=as.numeric(importance), validation=data@key, balance_classes=as.numeric(balance.classes), max_after_balance_size=as.numeric(max.after.balance.size))
  } else if(missing(validation) && nfolds >= 2) {
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GBM, source=data@key, response=args$y, cols=cols, ntrees=n.trees, max_depth=interaction.depth, learn_rate=shrinkage, family=family,
                            min_rows=n.minobsinnode, classification=classification, nbins=n.bins, importance=as.numeric(importance), n_folds=nfolds, balance_classes=as.numeric(balance.classes), max_after_balance_size=as.numeric(max.after.balance.size))
  } else if(!missing(validation) && nfolds == 0) {
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GBM, source=data@key, response=args$y, cols=cols, ntrees=n.trees, max_depth=interaction.depth, learn_rate=shrinkage, family=family,
                            min_rows=n.minobsinnode, classification=classification, nbins=n.bins, importance=as.numeric(importance), validation=validation@key, balance_classes=as.numeric(balance.classes), max_after_balance_size=as.numeric(max.after.balance.size))
  } else stop("Cannot set both validation and nfolds at the same time")
  params = list(x=args$x, y=args$y, distribution=distribution, n.trees=n.trees, interaction.depth=interaction.depth, shrinkage=shrinkage, n.minobsinnode=n.minobsinnode, n.bins=n.bins, importance=importance, nfolds=nfolds, balance.classes=balance.classes, max.after.balance.size=max.after.balance.size,
                h2o = data@h2o)
  
  if(.is_singlerun("GBM", params))
    .h2o.singlerun.internal("GBM", data, res, nfolds, validation, params)
  else
    .h2o.gridsearch.internal("GBM", data, res, nfolds, validation, params)
}

.h2o.__getGBMSummary <- function(res, params) {
  mySum = list()
  mySum$model_key = res$'_key'
  mySum$ntrees = res$N
  mySum$max_depth = res$max_depth
  mySum$min_rows = res$min_rows
  mySum$nbins = res$nbins
  mySum$learn_rate = res$learn_rate
  mySum$balance_classes = res$balance_classes
  mySum$max_after_balance_size = res$max_after_balance_size
  
  # if(params$distribution == "multinomial") {
  # temp = matrix(unlist(res$cm), nrow = length(res$cm))
  # mySum$prediction_error = 1-sum(diag(temp))/sum(temp)
  # mySum$prediction_error = tail(res$'cms', 1)[[1]]$'_predErr'
  # }
  return(mySum)
}

.h2o.__getGBMResults <- function(res, params) {
  if(res$parameters$state == "CRASHED")
    stop(res$parameters$exception)
  result = list()
  params$n.trees = res$N
  params$interaction.depth = res$max_depth
  params$n.minobsinnode = res$min_rows
  params$shrinkage = res$learn_rate
  params$n.bins = res$nbins
  result$params = params
  extra_json <- doNotCallThisMethod...Unsupported(params$h2o, res$'_key')
  result$priorDistribution <- extra_json$gbm_model$"_priorClassDist"
  result$modelDistribution <- extra_json$gbm_model$"_modelClassDist"
  params$balance.classes = res$balance_classes
  params$max.after.balance.size = res$max_after_balance_size
  
  if(result$params$distribution %in% c("multinomial", "bernoulli")) {
    class_names = res$'cmDomain' # tail(res$'_domains', 1)[[1]]
    result$confusion = .build_cm(tail(res$'cms', 1)[[1]]$'_arr', class_names)  # res$'_domains'[[length(res$'_domains')]])
    result$classification <- T
    
    if(!is.null(res$validAUC)) {
      tmp <- .h2o.__getPerfResults(res$validAUC)
      tmp$confusion <- NULL
      result <- c(result, tmp)
    }
  } else
    result$classification <- F
  
  if(params$importance) {
    result$varimp = data.frame(res$varimp$varimp)
    result$varimp[,2] = result$varimp[,1]/max(result$varimp[,1])
    result$varimp[,3] = 100*result$varimp[,1]/sum(result$varimp[,1])
    rownames(result$varimp) = res$'_names'[-length(res$'_names')]
    colnames(result$varimp) = c(res$varimp$method, "Scaled.Values", "Percent.Influence")
    result$varimp = result$varimp[order(result$varimp[,1], decreasing = TRUE),]
  }
  
  result$err = as.numeric(res$errs)
  return(result)
}

# -------------------------- Generalized Linear Models (GLM) ------------------------ #
h2o.glm <- function(x, y, data, family, nfolds = 0, alpha = 0.5, nlambda = -1, lambda.min.ratio = -1, lambda = 1e-5,
                    epsilon = 1e-4, standardize = TRUE, prior, variable_importances = 1, use_all_factor_levels = 0,
                    tweedie.p = ifelse(family == "tweedie", 1.5, as.numeric(NA)), iter.max = 100,
                    higher_accuracy = FALSE, lambda_search = FALSE, return_all_lambda = FALSE, max_predictors=-1) {
  args <- .verify_dataxy(data, x, y)

  if(!(variable_importances %in% c(0,1)))  stop(paste("variable_importances must be either 0 or 1. Got: ", variable_importances, sep = ""))
  if(!(use_all_factor_levels %in% c(0,1))) stop(paste("use_all_factor_levels must be either 0 or 1. Got: ", use_all_factor_levels, sep = ""))
  if(!is.numeric(nfolds)) stop('nfolds must be numeric')
  if( nfolds < 0 ) stop('nfolds must be >= 0')
  if(!is.numeric(alpha)) stop('alpha must be numeric')
  if( any(alpha < 0) ) stop('alpha must be >= 0')
  
  if(!is.numeric(nlambda)) stop("nlambda must be numeric")
  if((nlambda != -1) && (length(nlambda) > 1 || nlambda < 0)) stop("nlambda must be a single number >= 0")
  if(!is.numeric(lambda.min.ratio)) stop("lambda.min.ratio must be numeric")
  if((lambda.min.ratio != -1) && (length(lambda.min.ratio) > 1 || lambda.min.ratio < 0 || lambda.min.ratio > 1))
    stop("lambda.min.ratio must be a single number in [0,1]")
  if(!is.numeric(lambda)) stop('lambda must be numeric')
  if( any(lambda < 0) ) stop('lambda must be >= 0')
  
  if(!is.numeric(epsilon)) stop("epsilon must be numeric")
  if( epsilon < 0 ) stop('epsilon must be >= 0')
  if(!is.logical(standardize)) stop("standardize must be logical")
  if(!missing(prior)) {
    if(!is.numeric(prior)) stop("prior must be numeric")
    if(prior < 0 || prior > 1) stop("prior must be in [0,1]")
    if(family != "binomial") stop("prior may only be set for family binomial")
  }
  if(!is.numeric(tweedie.p)) stop('tweedie.p must be numeric')
  if( family != 'tweedie' && !(missing(tweedie.p) || is.na(tweedie.p)) ) stop("tweedie.p may only be set for family tweedie")
  if(!is.numeric(iter.max)) stop('iter.max must be numeric')
  if(!is.logical(higher_accuracy)) stop("higher_accuracy must be logical")
  if(!is.logical(lambda_search)) stop("lambda_search must be logical")
  if(lambda_search && length(lambda) > 1) stop("When automatically searching, must specify single numeric value as lambda, which is interpreted as minimum lambda in generated sequence")
  if(!is.logical(return_all_lambda)) stop("return_all_lambda must be logical")
  
  x_ignore = setdiff(1:ncol(data), c(args$x_i, args$y_i)) - 1
  if(length(x_ignore) == 0) x_ignore = ''
  
  if(length(alpha) == 1) {
    rand_glm_key = .h2o.__uniqID("GLM2Model")
    if(family == "tweedie")
      res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GLM2, source = data@key, destination_key = rand_glm_key,
                              response = args$y, ignored_cols = paste(x_ignore, sep="", collapse=","), family = family,
                              n_folds = nfolds, alpha = alpha, nlambdas = nlambda, lambda_min_ratio = lambda.min.ratio,
                              lambda = lambda, beta_epsilon = epsilon, standardize = as.numeric(standardize),
                              max_iter = iter.max, higher_accuracy = as.numeric(higher_accuracy),
                              lambda_search = as.numeric(lambda_search), tweedie_variance_power = tweedie.p,
                              max_predictors = max_predictors, variable_importances = variable_importances,
                              use_all_factor_levels = use_all_factor_levels)
    else if(family == "binomial") {
      if(missing(prior)) prior = -1
      res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GLM2, source = data@key, destination_key = rand_glm_key,
                              response = args$y, ignored_cols = paste(x_ignore, sep="", collapse=","), family = family,
                              n_folds = nfolds, alpha = alpha, nlambdas = nlambda, lambda_min_ratio = lambda.min.ratio,
                              lambda = lambda, beta_epsilon = epsilon, standardize = as.numeric(standardize),
                              max_iter = iter.max, higher_accuracy = as.numeric(higher_accuracy),
                              lambda_search = as.numeric(lambda_search), prior = prior,
                              max_predictors = max_predictors, variable_importances = variable_importances,
                              use_all_factor_levels = use_all_factor_levels)
    } else
      res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GLM2, source = data@key, destination_key = rand_glm_key,
                              response = args$y, ignored_cols = paste(x_ignore, sep="", collapse=","), family = family,
                              n_folds = nfolds, alpha = alpha, nlambdas = nlambda, lambda_min_ratio = lambda.min.ratio,
                              lambda = lambda, beta_epsilon = epsilon, standardize = as.numeric(standardize),
                              max_iter = iter.max, higher_accuracy = as.numeric(higher_accuracy),
                              lambda_search = as.numeric(lambda_search),
                              max_predictors = max_predictors, variable_importances = variable_importances,
                              use_all_factor_levels = use_all_factor_levels)

    params = list(x=args$x, y=args$y, family = .h2o.__getFamily(family, tweedie.var.p=tweedie.p), nfolds=nfolds,
                  alpha=alpha, nlambda=nlambda, lambda.min.ratio=lambda.min.ratio, lambda=lambda,
                  beta_epsilon=epsilon, standardize=standardize, max_predictors = max_predictors,
                  variable_importances = variable_importances, use_all_factor_levels = use_all_factor_levels, h2o = data@h2o)
    .h2o.__waitOnJob(data@h2o, res$job_key)
    h2o.glm.get_model(data, res$destination_key, return_all_lambda, params)
  } else
    .h2o.glm2grid.internal(x_ignore, args$y, data, family, nfolds, alpha, nlambda, lambda.min.ratio, lambda, epsilon,
                           standardize, prior, tweedie.p, iter.max, higher_accuracy, lambda_search, return_all_lambda,
                           variable_importances = variable_importances, use_all_factor_levels = use_all_factor_levels)
}

h2o.glm.get_model <- function (data, model_key, return_all_lambda = TRUE, params = list()) {
  res2 = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GLMModelView, '_modelKey'=model_key)
  resModel = res2$glm_model; destKey = resModel$'_key'
  if(!is.null(resModel$warnings))
    tmp = lapply(resModel$warnings, warning)
  
  make_model <- function(x, params) {
    m = .h2o.__getGLM2Results(resModel, params, x);
    res_xval = list()
    if(!is.null(resModel$submodels[[x]]$xvalidation)) {
      xvalKey = resModel$submodels[[x]]$xvalidation$xval_models
      # Get results from cross-validation
      if(!is.null(xvalKey) && length(xvalKey) >= 2) {
        for(j in 1:length(xvalKey)) {
          resX = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GLMModelView, '_modelKey'=xvalKey[j])
          modelXval = .h2o.__getGLM2Results(resX$glm_model, params, 1)
          res_xval[[j]] = new("H2OGLMModel", key=xvalKey[j], data=data, model=modelXval, xval=list())
        }
      }
    }
    new("H2OGLMModel", key=model_key, data=data, model=m, xval=res_xval)
  }
  if(return_all_lambda) {
    new("H2OGLMModelList", models=lapply(1:length(resModel$submodels), make_model, params), best_model=resModel$best_lambda_idx+1)
  } else {
    make_model(resModel$best_lambda_idx+1, params)
  }
}

.h2o.glm2grid.internal <- function(x_ignore, y, data, family, nfolds, alpha, nlambda, lambda.min.ratio, lambda, epsilon, standardize, prior, tweedie.p, iter.max, higher_accuracy, lambda_search, return_all_lambda,
                                   variable_importances, use_all_factor_levels) {
  if(family == "tweedie")
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GLM2, source = data@key, response = y,
                            ignored_cols = paste(x_ignore, sep="", collapse=","), family = family, n_folds = nfolds,
                            alpha = alpha, nlambdas = nlambda, lambda_min_ratio = lambda.min.ratio, lambda = lambda,
                            beta_epsilon = epsilon, standardize = as.numeric(standardize), max_iter = iter.max,
                            higher_accuracy = as.numeric(higher_accuracy), lambda_search = as.numeric(lambda_search),
                            tweedie_variance_power = tweedie.p,
                            variable_importances = variable_importances, use_all_factor_levels = use_all_factor_levels)
  else if(family == "binomial") {
    if(missing(prior)) prior = -1
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GLM2, source = data@key, response = y,
                            ignored_cols = paste(x_ignore, sep="", collapse=","), family = family, n_folds = nfolds,
                            alpha = alpha, nlambdas = nlambda, lambda_min_ratio = lambda.min.ratio, lambda = lambda,
                            beta_epsilon = epsilon, standardize = as.numeric(standardize), max_iter = iter.max,
                            higher_accuracy = as.numeric(higher_accuracy),
                            lambda_search = as.numeric(lambda_search), prior = prior,
                            variable_importances = variable_importances, use_all_factor_levels = use_all_factor_levels)
  }
  else
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GLM2, source = data@key, response = y,
                            ignored_cols = paste(x_ignore, sep="", collapse=","), family = family, n_folds = nfolds,
                            alpha = alpha, nlambdas = nlambda, lambda_min_ratio = lambda.min.ratio, lambda = lambda,
                            beta_epsilon = epsilon, standardize = as.numeric(standardize), max_iter = iter.max,
                            higher_accuracy = as.numeric(higher_accuracy), lambda_search = as.numeric(lambda_search),
                            variable_importances = variable_importances, use_all_factor_levels = use_all_factor_levels)

  params = list(x=setdiff(colnames(data)[-(x_ignore+1)], y), y=y,
                family=.h2o.__getFamily(family, tweedie.var.p=tweedie.p), nfolds=nfolds, alpha=alpha, nlambda=nlambda,
                lambda.min.ratio=lambda.min.ratio, lambda=lambda, beta_epsilon=epsilon, standardize=standardize,
                variable_importances = variable_importances, use_all_factor_levels = use_all_factor_levels, h2o = data@h2o)
  
  .h2o.__waitOnJob(data@h2o, res$job_key)
  # while(!.h2o.__isDone(data@h2o, "GLM2", res)) { Sys.sleep(1); prog = .h2o.__poll(data@h2o, res$job_key); setTxtProgressBar(pb, prog) }
  
  res2 = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GLM2GridView, grid_key=res$destination_key)
  destKey = res$destination_key
  allModels = res2$grid$destination_keys
  
  result = list(); myModelSum = list()
  for(i in 1:length(allModels)) {
    resH = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GLMModelView, '_modelKey'=allModels[i])
    resHModel = resH$glm_model
    if(!is.null(resHModel$warnings)) {
      cat("Model key", allModels[i], "generated the following messages:")
      tmp = lapply(resHModel$warnings, warning)
    }
    myModelSum[[i]] = .h2o.__getGLM2Summary(resHModel)
    # modelOrig = .h2o.__getGLM2Results(resHModel, params)
    
    # BUG: For some reason, H2O always uses default number of lambda (100) during grid search
    if(return_all_lambda) {
      # lambda_all = sapply(resHModel$submodels, function(x) { x$lambda_value })
      # allLambdaModels = lapply(lambda_all, .h2o.__getGLM2LambdaModel, data=data, model_key=allModels[i], params=params)
      # if(length(allLambdaModels) <= 1) result[[i]] = allLambdaModels[[1]]
      # else result[[i]] = allLambdaModels
      
      make_model <- function(x, params) {
        m = .h2o.__getGLM2Results(resHModel, params, x);
        res_xval = list()
        if(!is.null(resHModel$submodels[[x]]$xvalidation)) {
          xvalKey = resHModel$submodels[[x]]$xvalidation$xval_models
          # Get results from cross-validation
          if(!is.null(xvalKey) && length(xvalKey) >= 2) {
            for(j in 1:length(xvalKey)) {
              resX = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GLMModelView, '_modelKey'=xvalKey[j])
              modelXval = .h2o.__getGLM2Results(resX$glm_model, params, 1)
              res_xval[[j]] = new("H2OGLMModel", key=xvalKey[j], data=data, model=modelXval, xval=list())
            }
          }
        }
        new("H2OGLMModel", key=destKey, data=data, model=m, xval=res_xval)
      }
      allLambdaModels = lapply(1:length(resHModel$submodels), make_model, params)
      result[[i]] = new("H2OGLMModelList", models=allLambdaModels, best_model=resHModel$best_lambda_idx+1)
    } else {
      params$lambda_all = sapply(resHModel$submodels, function(x) { x$lambda_value })
      best_lambda_idx = resHModel$best_lambda_idx+1
      # best_lambda = resHModel$parameters$lambda[best_lambda_idx]
      best_lambda = params$lambda_all[best_lambda_idx]
      result[[i]] = .h2o.__getGLM2LambdaModel(best_lambda, data, allModels[i], params)
    }
  }
  new("H2OGLMGrid", key=destKey, data=data, model=result, sumtable=myModelSum)
}

h2o.getGLMLambdaModel <- function(model, lambda) {
  if(missing(model) || length(model) == 0) stop("model must be specified")
  if(class(model) == "list") model = model[[1]]
  if(class(model) != "H2OGLMModel") stop("model must be of class H2OGLMModel")
  .h2o.__getGLM2LambdaModel(lambda, model@data, model@key, model@model$params)
}

.h2o.__getGLM2LambdaModel <- function(lambda, data, model_key, params = list()) {
  if(missing(lambda) || length(lambda) > 1 || !is.numeric(lambda)) stop("lambda must be a single number")
  if(lambda < 0) stop("lambda must non-negative")
  
  res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GLMModelView, '_modelKey'=model_key, lambda=lambda)
  resModel = res$glm_model
  lambda_all = sapply(resModel$submodels, function(x) { x$lambda_value })
  lambda_idx = which(lambda_all == lambda)
  if(is.null(res) || length(lambda_idx) == 0)
    stop("Cannot find ", lambda, " in list of lambda searched over for this model")
  
  modelOrig = .h2o.__getGLM2Results(resModel, params, lambda_idx)
  xvalKey = resModel$submodels[[lambda_idx]]$validation$xval_models
  
  # Get results from cross-validation
  res_xval = list()
  if(!is.null(xvalKey) && length(xvalKey) >= 2) {
    for(j in 1:length(xvalKey)) {
      resX = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GLMModelView, '_modelKey'=xvalKey[j])
      modelXval = .h2o.__getGLM2Results(resX$glm_model, params, 1)
      res_xval[[j]] = new("H2OGLMModel", key=xvalKey[j], data=data, model=modelXval, xval=list())
    }
  }
  new("H2OGLMModel", key=model_key, data=data, model=modelOrig, xval=res_xval)
}

.h2o.__getGLM2Summary <- function(model) {
  mySum = list()
  mySum$model_key = model$'_key'
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
.h2o.__getGLM2Results <- function(model, params = list(), lambda_idx = 1) {
  submod <- model$submodels[[lambda_idx]]
  if(!is.null(submod$xvalidation)){
    valid <- submod$xvalidation
  } else {
    valid  <- submod$validation
  }
  
  result <- list()
#  extra_json <- doNotCallThisMethod...Unsupported(params$h2o, model$'_key')
#  result$priorDistribution <- extra_json$speedrf_model$"_priorClassDist"
#  result$modelDistribution <- extra_json$speedrf_model$"_modelClassDist"
  params$alpha  <- model$alpha
  params$lambda <- model$submodels[[lambda_idx]]$lambda_value
  # if(!is.null(model$parameters$lambda))
  #  params$lambda_all <- model$parameters$lambda
  # else
  params$lambda_all <- sapply(model$submodels, function(x) { x$lambda_value })
  params$lambda_best <- params$lambda_all[[model$best_lambda_idx+1]]
  
  result$params <- params
  if(model$glm$family == "tweedie")
    result$params$family <- .h2o.__getFamily(model$glm$family, model$glm$link, model$glm$tweedie_variance_power, model$glm$tweedie_link_power)
  else
    result$params$family <- .h2o.__getFamily(model$glm$family, model$glm$link)
  result$coefficients <- as.numeric(unlist(submod$beta))
  idxes <- submod$idxs + 1
  names(result$coefficients) <- model$coefficients_names[idxes]
  if(model$parameters$standardize == "true" && !is.null(submod$norm_beta)) {
    result$normalized_coefficients = as.numeric(unlist(submod$norm_beta))
    names(result$normalized_coefficients) = model$coefficients_names[idxes]
  }
  result$rank = valid$'_rank'
  result$iter = submod$iteration
  result$lambda = submod$lambda
  result$deviance = as.numeric(valid$residual_deviance)
  result$null.deviance = as.numeric(valid$null_deviance)
  result$df.residual = max(valid$nobs-result$rank,0)
  result$df.null = valid$nobs-1
  result$aic = as.numeric(valid$aic)
  result$train.err = as.numeric(valid$avg_err)
  
  if(model$glm$family == "binomial") {
    result$params$prior = as.numeric(model$prior)
    result$threshold = as.numeric(model$threshold)
    result$best_threshold = as.numeric(valid$best_threshold)
    result$auc = as.numeric(valid$auc)
    
    # Construct confusion matrix
    cm_ind = trunc(100*result$best_threshold) + 1
    #     temp = data.frame(t(sapply(valid$'_cms'[[cm_ind]]$'_arr', c)))
    #     temp[,3] = c(temp[1,2], temp[2,1])/apply(temp, 1, sum)
    #     temp[3,] = c(temp[2,1], temp[1,2], 0)/apply(temp, 2, sum)
    #     temp[3,3] = (temp[1,2] + temp[2,1])/valid$nobs
    #     dn = list(Actual = c("false", "true", "Err"), Predicted = c("false", "true", "Err"))
    #     dimnames(temp) = dn
    #    result$confusion = temp
    result$confusion = .build_cm(valid$'_cms'[[cm_ind]]$'_arr', c("false", "true"))
  }
  return(result)
}

# ------------------------------ K-Means Clustering --------------------------------- #
h2o.kmeans <- function(data, centers, cols = '', iter.max = 10, normalize = FALSE, init = "none", seed = 0, dropNACols = FALSE) {
  args <- .verify_datacols(data, cols)
  if( missing(centers) ) stop('must specify centers')
  if(!is.numeric(centers) && !is.integer(centers)) stop('centers must be a positive integer')
  if( any(centers < 1) ) stop("centers must be an integer greater than 0")
  if(!is.numeric(iter.max)) stop('iter.max must be numeric')
  if( any(iter.max < 1)) stop('iter.max must be >= 1')
  if(!is.logical(normalize)) stop("normalize must be logical")
  if(length(init) > 1 || !init %in% c("none", "plusplus", "furthest"))
    stop("init must be one of 'none', 'plusplus', or 'furthest'")
  if(!is.numeric(seed)) stop("seed must be numeric")
  if(!is.logical(dropNACols)) stop("dropNACols must be logical")
  
  if(h2o.anyFactor(data[,args$cols_ind])) stop("Unimplemented: K-means can only model on numeric data")
  myInit = switch(init, none = "None", plusplus = "PlusPlus", furthest = "Furthest")
  
  res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_KMEANS2, source=data@key, ignored_cols=args$cols_ignore, k=centers, max_iter=iter.max, normalize=as.numeric(normalize), initialization=myInit, seed=seed, drop_na_cols=as.numeric(dropNACols))
  params = list(cols=args$cols, centers=centers, iter.max=iter.max, normalize=normalize, init=myInit, seed=seed)
  
  if(.is_singlerun("KM", params)) {
    .h2o.__waitOnJob(data@h2o, res$job_key)
    # while(!.h2o.__isDone(data@h2o, "KM", res)) { Sys.sleep(1) }
    res2 = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_KM2ModelView, model=res$destination_key)
    res2 = res2$model
    
    result = .h2o.__getKM2Results(res2, data, params)
    new("H2OKMeansModel", key=res2$'_key', data=data, model=result)
  } else {
    # .h2o.gridsearch.internal("KM", data, res$job_key, res$destination_key)
    .h2o.gridsearch.internal("KM", data, res, params=params)
  }
}

.h2o.__getKM2Summary <- function(res) {
  mySum = list()
  mySum$model_key = res$'_key'
  mySum$k = res$k
  mySum$max_iter = res$iterations
  mySum$error = res$error
  return(mySum)
}

.h2o.__getKM2Results <- function(res, data, params) {
  # rand_pred_key = .h2o.__uniqID("KMeansClusters")
  # res2 = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_PREDICT2, model=res$'_key', data=data@key, prediction=rand_pred_key)
  # res2 = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_SUMMARY2, source=rand_pred_key, cols=0)
  clusters_key <- paste(res$'_clustersKey', sep = "")
  
  result = list()
  params$centers = res$k
  params$iter.max = res$max_iter
  result$params = params
  
  result$cluster = new("H2OParsedData", h2o=data@h2o, key=clusters_key)
  feat = res$'_names'[-length(res$'_names')]     # Get rid of response column name
  result$centers = t(matrix(unlist(res$centers), ncol = res$k))
  dimnames(result$centers) = list(seq(1,res$k), feat)
  result$totss <- res$total_SS
  result$withinss <- res$within_cluster_variances
  result$tot.withinss <- res$total_within_SS
  result$betweenss <- res$between_cluster_SS
  result$size <- res$size
  result$iter <- res$iterations
  return(result)
}

.addParm <- function(parms, k, v) {
  cmd = sprintf("parms$%s = v", k)
  eval(parse(text=cmd))
  return(parms)
}

.addStringParm <- function(parms, k, v) {
  if (! missing(v)) {
    if (! is.character(v)) stop(sprintf("%s must be of type character"), k)
    parms = .addParm(parms, k, v)
  }
  return(parms)
}

.addBooleanParm <- function(parms, k, v) {
  if (! missing(v)) {
    if (! is.logical(v)) stop(sprintf("%s must be of type logical"), k)
    parms = .addParm(parms, k, as.numeric(v))
  }
  return(parms)
}

.addNumericParm <- function(parms, k, v) {
  if (! missing(v)) {
    if (! is.numeric(v)) stop(sprintf("%s must be of type numeric"), k)
    parms = .addParm(parms, k, v)
  }
  return(parms)
}

.addDoubleParm <- function(parms, k, v) {
  parms = .addNumericParm(parms, k, v)
  return(parms)
}

.addFloatParm <- function(parms, k, v) {
  parms = .addNumericParm(parms, k, v)
  return(parms)
}

.addLongParm <- function(parms, k, v) {
  parms = .addNumericParm(parms, k, v)
  return(parms)
}

.addIntParm <- function(parms, k, v) {
  parms = .addNumericParm(parms, k, v)
  return(parms)
}

.addNumericArrayParm <- function(parms, k, v) {
  if (! missing(v)) {
    # if (! is.numeric(v)) stop(sprintf("%s must be of type numeric"), k)
    # arrAsString = paste(v, collapse=",")
    if(!all(sapply(v, is.numeric))) stop(sprintf("%s must contain all numeric elements"), k)
    # if(is.list(v) && length(v) == 1) v = v[[1]]
    arrAsString = sapply(v, function(x) {
        if(length(x) <= 1) return(x)
        paste("(", paste(x, collapse=","), ")", sep = "")
      })
    arrAsString = paste(arrAsString, collapse = ",")
    parms = .addParm(parms, k, arrAsString)
  }
  return(parms)
}

.addDoubleArrayParm <- function(parms, k, v) {
  parms = .addNumericArrayParm(parms, k, v)
}

.addIntArrayParm <- function(parms, k, v) {
  parms = .addNumericArrayParm(parms, k, v)
}

# ---------------------------- Deep Learning - Neural Network ------------------------- #
h2o.deeplearning <- function(x, y, data, classification = TRUE, nfolds = 0, validation,
                             # ----- AUTOGENERATED PARAMETERS BEGIN -----
                             autoencoder,
                             use_all_factor_levels,
                             activation,
                             hidden,
                             epochs,
                             train_samples_per_iteration,
                             seed,
                             adaptive_rate,
                             rho,
                             epsilon,
                             rate,
                             rate_annealing,
                             rate_decay,
                             momentum_start,
                             momentum_ramp,
                             momentum_stable,
                             nesterov_accelerated_gradient,
                             input_dropout_ratio,
                             hidden_dropout_ratios,
                             l1,
                             l2,
                             max_w2,
                             initial_weight_distribution,
                             initial_weight_scale,
                             loss,
                             score_interval,
                             score_training_samples,
                             score_validation_samples,
                             score_duty_cycle,
                             classification_stop,
                             regression_stop,
                             quiet_mode,
                             max_confusion_matrix_size,
                             max_hit_ratio_k,
                             balance_classes,
                             max_after_balance_size,
                             score_validation_sampling,
                             diagnostics,
                             variable_importances,
                             fast_mode,
                             ignore_const_cols,
                             force_load_balance,
                             replicate_training_data,
                             single_node_mode,
                             shuffle_training_data,
                             sparse,
                             col_major
                             # ----- AUTOGENERATED PARAMETERS END -----
)
{
  colargs <- .verify_dataxy(data, x, y, autoencoder)
  parms = list()

  parms$'source' = data@key
  parms$response = colargs$y
  parms$ignored_cols = colargs$x_ignore
  parms$expert_mode = ifelse(!missing(autoencoder) && autoencoder, 1, 0)
  
  if (! missing(classification)) {
    if (! is.logical(classification)) stop('classification must be TRUE or FALSE')
    parms$classification = as.numeric(classification)
  }
  
  if(!is.numeric(nfolds)) stop("nfolds must be numeric")
  if(nfolds == 1) stop("nfolds cannot be 1")
  if(!missing(validation) && class(validation) != "H2OParsedData")
    stop("validation must be an H2O parsed dataset")
  
  if(missing(validation) && nfolds == 0) {
    # validation = data
    # parms$validation = validation@key
    validation = new ("H2OParsedData", key = as.character(NA))
    parms$n_folds = nfolds
  } else if(missing(validation) && nfolds >= 2) {
    validation = new("H2OParsedData", key = as.character(NA))
    parms$n_folds = nfolds
  } else if(!missing(validation) && nfolds == 0)
    parms$validation = validation@key
  else stop("Cannot set both validation and nfolds at the same time")
  
  # ----- AUTOGENERATED PARAMETERS BEGIN -----
  parms = .addBooleanParm(parms, k="autoencoder", v=autoencoder)
  parms = .addBooleanParm(parms, k="use_all_factor_levels", v=use_all_factor_levels)
  parms = .addStringParm(parms, k="activation", v=activation)
  parms = .addIntArrayParm(parms, k="hidden", v=hidden)
  parms = .addDoubleParm(parms, k="epochs", v=epochs)
  parms = .addLongParm(parms, k="train_samples_per_iteration", v=train_samples_per_iteration)
  parms = .addLongParm(parms, k="seed", v=seed)
  parms = .addBooleanParm(parms, k="adaptive_rate", v=adaptive_rate)
  parms = .addDoubleParm(parms, k="rho", v=rho)
  parms = .addDoubleParm(parms, k="epsilon", v=epsilon)
  parms = .addDoubleParm(parms, k="rate", v=rate)
  parms = .addDoubleParm(parms, k="rate_annealing", v=rate_annealing)
  parms = .addDoubleParm(parms, k="rate_decay", v=rate_decay)
  parms = .addDoubleParm(parms, k="momentum_start", v=momentum_start)
  parms = .addDoubleParm(parms, k="momentum_ramp", v=momentum_ramp)
  parms = .addDoubleParm(parms, k="momentum_stable", v=momentum_stable)
  parms = .addBooleanParm(parms, k="nesterov_accelerated_gradient", v=nesterov_accelerated_gradient)
  parms = .addDoubleParm(parms, k="input_dropout_ratio", v=input_dropout_ratio)
  parms = .addDoubleArrayParm(parms, k="hidden_dropout_ratios", v=hidden_dropout_ratios)
  parms = .addDoubleParm(parms, k="l1", v=l1)
  parms = .addDoubleParm(parms, k="l2", v=l2)
  parms = .addFloatParm(parms, k="max_w2", v=max_w2)
  parms = .addStringParm(parms, k="initial_weight_distribution", v=initial_weight_distribution)
  parms = .addDoubleParm(parms, k="initial_weight_scale", v=initial_weight_scale)
  parms = .addStringParm(parms, k="loss", v=loss)
  parms = .addDoubleParm(parms, k="score_interval", v=score_interval)
  parms = .addLongParm(parms, k="score_training_samples", v=score_training_samples)
  parms = .addLongParm(parms, k="score_validation_samples", v=score_validation_samples)
  parms = .addDoubleParm(parms, k="score_duty_cycle", v=score_duty_cycle)
  parms = .addDoubleParm(parms, k="classification_stop", v=classification_stop)
  parms = .addDoubleParm(parms, k="regression_stop", v=regression_stop)
  parms = .addBooleanParm(parms, k="quiet_mode", v=quiet_mode)
  parms = .addIntParm(parms, k="max_confusion_matrix_size", v=max_confusion_matrix_size)
  parms = .addIntParm(parms, k="max_hit_ratio_k", v=max_hit_ratio_k)
  parms = .addBooleanParm(parms, k="balance_classes", v=balance_classes)
  parms = .addFloatParm(parms, k="max_after_balance_size", v=max_after_balance_size)
  parms = .addStringParm(parms, k="score_validation_sampling", v=score_validation_sampling)
  parms = .addBooleanParm(parms, k="diagnostics", v=diagnostics)
  parms = .addBooleanParm(parms, k="variable_importances", v=variable_importances)
  parms = .addBooleanParm(parms, k="fast_mode", v=fast_mode)
  parms = .addBooleanParm(parms, k="ignore_const_cols", v=ignore_const_cols)
  parms = .addBooleanParm(parms, k="force_load_balance", v=force_load_balance)
  parms = .addBooleanParm(parms, k="replicate_training_data", v=replicate_training_data)
  parms = .addBooleanParm(parms, k="single_node_mode", v=single_node_mode)
  parms = .addBooleanParm(parms, k="shuffle_training_data", v=shuffle_training_data)
  parms = .addBooleanParm(parms, k="sparse", v=sparse)
  parms = .addBooleanParm(parms, k="col_major", v=col_major)
  # ----- AUTOGENERATED PARAMETERS END -----
  
  res = .h2o.__remoteSendWithParms(data@h2o, .h2o.__PAGE_DeepLearning, parms)
  parms$h2o <- data@h2o
  noGrid = missing(hidden) || !(is.list(hidden) && length(hidden) > 1)
  if(noGrid)
    .h2o.singlerun.internal("DeepLearning", data, res, nfolds, validation, parms)
  else {
    .h2o.gridsearch.internal("DeepLearning", data, res, nfolds, validation, parms)
  }
}

.h2o.__getDeepLearningSummary <- function(res) {
  mySum = list()
  resP = res$parameters
  
  mySum$model_key = resP$destination_key
  mySum$activation = resP$activation
  mySum$hidden = resP$hidden
  mySum$rate = resP$rate
  mySum$rate_annealing = resP$rate_annealing
  mySum$momentum_start = resP$momentum_start
  mySum$momentum_ramp = resP$momentum_ramp
  mySum$momentum_stable = resP$momentum_stable
  mySum$l1_reg = resP$l1
  mySum$l2_reg = resP$l2
  mySum$epochs = resP$epochs
  
  # temp = matrix(unlist(res$confusion_matrix), nrow = length(res$confusion_matrix))
  # mySum$prediction_error = 1-sum(diag(temp))/sum(temp)
  return(mySum)
}

.h2o.__getDeepLearningResults <- function(res, params = list()) {
  result = list()
  #   model_params = res$model_info$parameters
  #   params$activation = model_params$activation
  #   params$rate = model_params$rate
  #   params$annealing_rate = model_params$rate_annealing
  #   params$l1_reg = model_params$l1
  #   params$l2_reg = model_params$l2
  #   params$mom_start = model_params$momentum_start
  #   params$mom_ramp = model_params$momentum_ramp
  #   params$mom_stable = model_params$momentum_stable
  #   params$epochs = model_params$epochs
  
  # result$params = params
  # model_params = res$model_info$parameters
  model_params = res$model_info$job
  model_params$Request2 = NULL; model_params$response_info = NULL
  model_params$'source' = NULL; model_params$validation = NULL
  model_params$job_key = NULL; model_params$destination_key = NULL
  model_params$response = NULL; model_params$description = NULL
  if(!is.null(model_params$exception)) stop(model_params$exception)
  model_params$exception = NULL; model_params$state = NULL
  
  # Remove all NULL elements and cast to logical value
  if(length(model_params) > 0)
    model_params = model_params[!sapply(model_params, is.null)]
  for(i in 1:length(model_params)) {
    x = model_params[[i]]
    if(length(x) == 1 && is.character(x))
      model_params[[i]] = switch(x, true = TRUE, false = FALSE, "Inf" = Inf, "-Inf" = -Inf, x)
  }
  result$params = model_params
  # result$params = unlist(model_params, recursive = FALSE)
  # result$params = lapply(model_params, function(x) { if(is.character(x)) { switch(x, true = TRUE, false = FALSE, "Inf" = Inf, "-Inf" = -Inf, x) }
  #                                                    else return(x) })
  result$params$nfolds = model_params$n_folds
  result$params$n_folds = NULL
  extra_json <- doNotCallThisMethod...Unsupported(params$h2o, res$'_key')
  result$priorDistribution <- extra_json$deeplearning_model$"_priorClassDist"
  result$modelDistribution <- extra_json$deeplearning_model$"_modelClassDist"
  errs = tail(res$errors, 1)[[1]]

  # BUG: Why is the confusion matrix returning an extra row and column with all zeroes?
  if(is.null(errs$valid_confusion_matrix))
    confusion = errs$train_confusion_matrix
  else
    confusion = errs$valid_confusion_matrix
  
  if(!is.null(confusion$cm)) {
    cm = confusion$cm[-length(confusion$cm)]
    cm = lapply(cm, function(x) { x[-length(x)] })
    # result$confusion = .build_cm(cm, confusion$actual_domain, confusion$predicted_domain)
    result$confusion = .build_cm(cm, confusion$domain)
  }
  
  result$train_class_error = as.numeric(errs$train_err)
  result$train_sqr_error = as.numeric(errs$train_mse)
  result$valid_class_error = as.numeric(errs$valid_err)
  result$valid_sqr_error = as.numeric(errs$valid_mse)
  
  if(!is.null(errs$validAUC)) {
    tmp <- .h2o.__getPerfResults(errs$validAUC)
    tmp$confusion <- NULL 
    result <- c(result, tmp) 
  }
  
  if(!is.null(errs$valid_hitratio)) {
    max_k <- errs$valid_hitratio$max_k
    hit_ratios <- errs$valid_hitratio$hit_ratios
    result$hit_ratios <- data.frame(k = 1:max_k, hit_ratios = hit_ratios)
  }
  return(result)
}

# -------------------------------- Naive Bayes ----------------------------- #
h2o.naiveBayes <- function(x, y, data, laplace = 0, dropNACols = FALSE) {
  args <- .verify_dataxy(data, x, y)
  if(!is.numeric(laplace)) stop("laplace must be numeric")
  if(laplace < 0) stop("laplace must be a non-negative number")
  
  res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_BAYES, source = data@key, response = args$y, ignored_cols = args$x_ignore, laplace = laplace, drop_na_cols = as.numeric(dropNACols))
  .h2o.__waitOnJob(data@h2o, res$job_key)
  res2 = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_NBModelView, '_modelKey' = res$destination_key)
  result = .h2o.__getNBResults(res2$nb_model)
  new("H2ONBModel", key = res$destination_key, data = data, model = result)
}

.h2o.__getNBResults <- function(res) {
  result = list()
  result$laplace = res$laplace
  result$levels = tail(res$'_domains',1)[[1]]
  result$apriori_prob = as.table(as.numeric(res$pprior))
  result$apriori = as.table(as.numeric(res$rescnt))
  dimnames(result$apriori) = dimnames(result$apriori_prob) = list(Y = result$levels)
  
  pred_names = res$'_names'[-length(res$'_names')]
  pred_domains = res$'_domains'[-length(res$'_domains')]
  result$tables = mapply(function(dat, nam, doms) {
    if(is.null(doms))
      doms = c("Mean", "StdDev")
    temp = t(matrix(unlist(dat), nrow = length(doms)))
    myList = list(result$levels, doms); names(myList) = c("Y", nam)
    dimnames(temp) = myList
    return(as.table(temp)) }, 
    res$pcond, pred_names, pred_domains, SIMPLIFY = FALSE)
  names(result$tables) = pred_names
  return(result)
}

# ----------------------- Principal Components Analysis ----------------------------- #
h2o.prcomp <- function(data, tol=0, cols = "", standardize=TRUE, retx=FALSE) {
  args <- .verify_datacols(data, cols)
  if(!is.numeric(tol)) stop('tol must be numeric')
  if(!is.logical(standardize)) stop('standardize must be TRUE or FALSE')
  if(!is.logical(retx)) stop('retx must be TRUE or FALSE')
  
  destKey = .h2o.__uniqID("PCAModel")
  res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_PCA, source=data@key, destination_key=destKey, ignored_cols = args$cols_ignore, tolerance=tol, standardize=as.numeric(standardize))
  .h2o.__waitOnJob(data@h2o, res$job_key)
  # while(!.h2o.__isDone(data@h2o, "PCA", res)) { Sys.sleep(1) }
  res2 = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_PCAModelView, '_modelKey'=destKey)
  res2 = res2$pca_model
  
  result = list()
  result$num_pc = res2$num_pc
  result$standardized = standardize
  result$sdev = res2$sdev
  nfeat = length(res2$eigVec[[1]])
  temp = t(matrix(unlist(res2$eigVec), nrow = nfeat))
  rownames(temp) = res2$namesExp #'_names'
  colnames(temp) = paste("PC", seq(1, ncol(temp)), sep="")
  result$rotation = temp
  
  if(retx) result$x = h2o.predict(new("H2OPCAModel", key=destKey, data=data, model=result))
  new("H2OPCAModel", key=destKey, data=data, model=result)
}

h2o.pcr <- function(x, y, data, ncomp, family, nfolds = 10, alpha = 0.5, lambda = 1.0e-5, epsilon = 1.0e-5, tweedie.p = ifelse(family=="tweedie", 0, as.numeric(NA))) {
  args <- .verify_dataxy(data, x, y)
  
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
  myModel <- .h2o.prcomp.internal(data=data, x_ignore=x_ignore, dest="", max_pc=ncomp, tol=0, standardize=TRUE)
  myScore <- h2o.predict(myModel)
  
  myScore[,ncomp+1] = data[,args$y_i]    # Bind response to frame of principal components
  myGLMData = new("H2OParsedData", h2o=data@h2o, key=myScore@key)
  h2o.glm(x = 1:ncomp,
          y = ncomp+1,
          data = myGLMData,
          family = family,
          nfolds = nfolds,
          alpha = alpha,
          lambda = lambda,
          epsilon = epsilon,
          standardize = FALSE,
          tweedie.p = tweedie.p)
}

.h2o.prcomp.internal <- function(data, x_ignore, dest, max_pc=10000, tol=0, standardize=TRUE) {
  res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_PCA, source=data@key, ignored_cols_by_name=x_ignore, destination_key=dest, max_pc=max_pc, tolerance=tol, standardize=as.numeric(standardize))
  .h2o.__waitOnJob(data@h2o, res$job_key)
  # while(!.h2o.__isDone(data@h2o, "PCA", res)) { Sys.sleep(1) }
  destKey = res$destination_key
  res2 = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_PCAModelView, '_modelKey'=destKey)
  res2 = res2$pca_model
  
  result = list()
  result$params$x = res2$'_names'
  result$num_pc = res2$num_pc
  result$standardized = standardize
  result$sdev = res2$sdev
  nfeat = length(res2$eigVec[[1]])
  temp = t(matrix(unlist(res2$eigVec), nrow = nfeat))
  rownames(temp) = res2$'namesExp'
  colnames(temp) = paste("PC", seq(1, ncol(temp)), sep="")
  result$rotation = temp
  new("H2OPCAModel", key=destKey, data=data, model=result)
}

# ----------------------------------- Random Forest --------------------------------- #
h2o.randomForest <- function(x, y, data, classification=TRUE, ntree=50, depth=20, mtries = -1, sample.rate=2/3, nbins=100, seed=-1, importance=FALSE, nfolds=0, validation, nodesize=1, balance.classes=FALSE, max.after.balance.size=5) {
  args <- .verify_dataxy(data, x, y)
  if(!is.logical(classification)) stop("classification must be logical (TRUE or FALSE)")
  if(!is.numeric(ntree)) stop('ntree must be a number')
  if( any(ntree < 1) ) stop('ntree must be >= 1')
  if(!is.numeric(depth)) stop('depth must be a number')
  if( any(depth < 1) ) stop('depth must be >= 1')
  if(!is.numeric(sample.rate)) stop('sample.rate must be a number')
  if( any(sample.rate < 0 || sample.rate > 1) ) stop('sample.rate must be between 0 and 1')
  if(!is.numeric(nbins)) stop('nbins must be a number')
  if( any(nbins < 1)) stop('nbins must be an integer >= 1')
  if(!is.numeric(seed)) stop("seed must be an integer >= 0")
  if(!is.logical(importance)) stop("importance must be logical (TRUE or FALSE)')")
  
  if(!is.logical(balance.classes)) stop('balance.classes must be logical (TRUE or FALSE)')
  if(!is.numeric(max.after.balance.size)) stop('max.after.balance.size must be a number')
  if( any(max.after.balance.size <= 0) ) stop('max.after.balance.size must be >= 0')
  if(balance.classes && !classification) stop('balance.classes can only be used for classification')
  if(!is.numeric(nodesize)) stop('nodesize must be a number')
  if( any(nodesize < 1) ) stop('nodesize must be >= 1')
  
  if(!is.numeric(nfolds)) stop("nfolds must be numeric")
  if(nfolds == 1) stop("nfolds cannot be 1")
  if(!missing(validation) && class(validation) != "H2OParsedData")
    stop("validation must be an H2O parsed dataset")
  
  # NB: externally, 1 based indexing; internally, 0 based
  cols <- paste(args$x_i - 1, collapse=',')
  if(missing(validation) && nfolds == 0) {
    # Default to using training data as validation
    validation = data
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_DRF, source=data@key, response=args$y, cols=cols, ntrees=ntree, max_depth=depth, min_rows=nodesize, sample_rate=sample.rate, nbins=nbins, mtries = mtries, seed=seed, importance=as.numeric(importance),
                            classification=as.numeric(classification), validation=data@key, balance_classes=as.numeric(balance.classes), max_after_balance_size=as.numeric(max.after.balance.size))
  } else if(missing(validation) && nfolds >= 2) {
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_DRF, source=data@key, response=args$y, cols=cols, ntrees=ntree, mtries = mtries, max_depth=depth, min_rows=nodesize, sample_rate=sample.rate, nbins=nbins, seed=seed, importance=as.numeric(importance),
                            classification=as.numeric(classification), n_folds=nfolds, balance_classes=as.numeric(balance.classes), max_after_balance_size=as.numeric(max.after.balance.size))
  } else if(!missing(validation) && nfolds == 0) {
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_DRF, source=data@key, response=args$y, cols=cols, ntrees=ntree, max_depth=depth, min_rows=nodesize, sample_rate=sample.rate, nbins=nbins, seed=seed, importance=as.numeric(importance), 
                            classification=as.numeric(classification), validation=validation@key, balance_classes=as.numeric(balance.classes), max_after_balance_size=as.numeric(max.after.balance.size))
  } else stop("Cannot set both validation and nfolds at the same time")
  params = list(x=args$x, y=args$y, ntree=ntree, mtries = mtries, depth=depth, sample.rate=sample.rate, nbins=nbins, importance=importance, nfolds=nfolds, balance.classes=balance.classes, max.after.balance.size=max.after.balance.size, nodesize=nodesize, h2o = data@h2o)
  
  if(.is_singlerun("RF", params))
    .h2o.singlerun.internal("RF", data, res, nfolds, validation, params)
  else
    .h2o.gridsearch.internal("RF", data, res, nfolds, validation, params)
}

.h2o.__getDRFSummary <- function(res) {
  mySum = list()
  mySum$model_key = res$'_key'
  mySum$ntrees = res$N
  mySum$max_depth = res$max_depth
  mySum$min_rows = res$min_rows
  mySum$nbins = res$nbins
  mySum$balance_classes = res$balance_classes
  mySum$max_after_balance_size = res$max_after_balance_size
  
  # temp = matrix(unlist(res$cm), nrow = length(res$cm))
  # mySum$prediction_error = 1-sum(diag(temp))/sum(temp)
  mySum$prediction_error = tail(res$'cms', 1)[[1]]$'_predErr'
  return(mySum)
}

.h2o.__getDRFResults <- function(res, params) {
  result = list()
  params$ntree = res$N
  params$depth = res$max_depth
  params$nbins = res$nbins
  params$sample.rate = res$sample_rate
  params$classification = ifelse(res$parameters$classification == "true", TRUE, FALSE)
  params$balance.classes = res$balance_classes
  params$max.after.balance.size = res$max_after_balance_size
  
  result$params = params
  treeStats = unlist(res$treeStats)
  rf_matrix = rbind(treeStats[1:3], treeStats[4:6])
  colnames(rf_matrix) = c("Min.", "Max.", "Mean.")
  rownames(rf_matrix) = c("Depth", "Leaves")
  result$forest = rf_matrix
  result$mse = as.numeric(res$errs)
  
  if(params$classification) {
    if(!is.null(res$validAUC)) {
      tmp <- .h2o.__getPerfResults(res$validAUC)
      tmp$confusion <- NULL
      result <- c(result, tmp)
    }
    class_names = res$'cmDomain' # tail(res$'_domains', 1)[[1]]
    result$confusion = .build_cm(tail(res$'cms', 1)[[1]]$'_arr', class_names)  #res$'_domains'[[length(res$'_domains')]])
  }

  extra_json <- doNotCallThisMethod...Unsupported(params$h2o, res$'_key')
  result$priorDistribution <- extra_json$drf_model$"_priorClassDist"
  result$modelDistribution <- extra_json$drf_model$"_modelClassDist"
  
  if(params$importance) {
    result$varimp = data.frame(rbind(res$varimp$varimp, res$varimp$varimpSD))
    result$varimp[3,] = sqrt(params$ntree)*result$varimp[1,]/result$varimp[2,]   # Compute z-scores
    colnames(result$varimp) = res$'_names'[-length(res$'_names')]    #res$varimp$variables
    rownames(result$varimp) = c(res$varimp$method, "Standard Deviation", "Z-Scores")
  }
  return(result)
}

# -------------------------- SpeeDRF -------------------------- #
h2o.SpeeDRF <- function(x, y, data, classification=TRUE, nfolds=0, validation,
                        mtry=-1, 
                        ntree=50, 
                        depth=50, 
                        sample.rate=2/3,
                        oobee = TRUE,
                        importance = FALSE,
                        nbins=1024, 
                        seed=-1,
                        stat.type="ENTROPY",
                        balance.classes=FALSE,
                        verbose
) {
  args <- .verify_dataxy(data, x, y)
  if(!is.numeric(ntree)) stop('ntree must be a number')
  if( any(ntree < 1) ) stop('ntree must be >= 1')
  if(!is.numeric(depth)) stop('depth must be a number')
  if( any(depth < 1) ) stop('depth must be >= 1')
  if(!is.numeric(sample.rate)) stop('sample.rate must be a number')
  if( any(sample.rate < 0 || sample.rate > 1) ) stop('sample.rate must be between 0 and 1')
  if(!is.numeric(nbins)) stop('nbins must be a number')
  if( any(nbins < 1)) stop('nbins must be an integer >= 1')
  if(!is.numeric(seed)) stop("seed must be an integer")
  if(!(stat.type %in% c("ENTROPY", "GINI"))) stop(paste("stat.type must be either GINI or ENTROPY. Input was: ", stat.type, sep = ""))
  if(!(is.logical(oobee))) stop(paste("oobee must be logical (TRUE or FALSE). Input was: ", oobee, " and is of type ", mode(oobee), sep = ""))
  #if(!(sampling_strategy %in% c("RANDOM", "STRATIFIED"))) stop(paste("sampling_strategy must be either RANDOM or STRATIFIED. Input was: ", sampling_strategy, sep = ""))
  
  if(!is.numeric(nfolds)) stop("nfolds must be numeric")
  if(nfolds == 1) stop("nfolds cannot be 1")
  if(!missing(validation) && class(validation) != "H2OParsedData")
    stop("validation must be an H2O parsed dataset")

  if(missing(verbose)) {verbose <- FALSE}

  if (missing(validation) && nfolds == 0 && oobee) {
    res <- .h2o.__remoteSend(data@h2o, .h2o.__PAGE_SpeeDRF, source=data@key, response=args$y, ignored_cols=args$x_ignore, balance_classes = as.numeric(balance.classes), num_trees=ntree, max_depth=depth, importance=as.numeric(importance),
                                sample=sample.rate, bin_limit=nbins, seed=seed, select_stat_type = stat.type, oobee=as.numeric(oobee), sampling_strategy="RANDOM", verbose = as.numeric(verbose))

  } else if(missing(validation) && nfolds >= 2 && oobee) {
        res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_SpeeDRF, source=data@key, response=args$y, ignored_cols=args$x_ignore, num_trees=ntree, balance_classes = as.numeric(balance.classes), max_depth=depth, n_folds=nfolds, importance=as.numeric(importance),
                                sample=sample.rate, bin_limit=nbins, seed=seed, select_stat_type=stat.type, oobee=as.numeric(oobee), sampling_strategy="RANDOM", verbose = as.numeric(verbose))

  } else if(missing(validation) && nfolds == 0) {
    # Default to using training data as validation if oobee is false...
    validation = data
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_SpeeDRF, source=data@key, response=args$y, ignored_cols=args$x_ignore, balance_classes = as.numeric(balance.classes), num_trees=ntree, max_depth=depth, validation=data@key, importance=as.numeric(importance),
                            sample=sample.rate, bin_limit=nbins, seed=seed, select_stat_type = stat.type, oobee=as.numeric(oobee), sampling_strategy="RANDOM", verbose = as.numeric(verbose))
  } else if(missing(validation) && nfolds >= 2) {
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_SpeeDRF, source=data@key, response=args$y, ignored_cols=args$x_ignore, num_trees=ntree, balance_classes = as.numeric(balance.classes), max_depth=depth, n_folds=nfolds, importance=as.numeric(importance),
                            sample=sample.rate, bin_limit=nbins, seed=seed, select_stat_type=stat.type, oobee=as.numeric(oobee), sampling_strategy="RANDOM", verbose = as.numeric(verbose))
  } else if(!missing(validation) && nfolds == 0) {
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_SpeeDRF, source=data@key, response=args$y, ignored_cols=args$x_ignore, balance_classes = as.numeric(balance.classes), num_trees=ntree, max_depth=depth, validation=validation@key, importance=as.numeric(importance),
                            sample=sample.rate, bin_limit=nbins, seed=seed, select_stat_type = stat.type, oobee=as.numeric(oobee), sampling_strategy="RANDOM", verbose = as.numeric(verbose))
  } else stop("Cannot set both validation and nfolds at the same time")
  params = list(x=args$x, y=args$y, ntree=ntree, depth=depth, sample.rate=sample.rate, bin_limit=nbins, stat.type = stat.type, balance_classes = as.numeric(balance.classes),
                sampling_strategy="RANDOM", seed=seed, oobee=oobee, nfolds=nfolds, importance=importance, verbose = as.numeric(verbose),
                h2o = data@h2o)
  
  if(.is_singlerun("SpeeDRF", params))
    .h2o.singlerun.internal("SpeeDRF", data, res, nfolds, validation, params)
  else
    .h2o.gridsearch.internal("SpeeDRF", data, res, nfolds, validation, params)
}

.h2o.__getSpeeDRFSummary <- function(res) {
  mySum = list()
  mySum$model_key = res$'_key'
  mySum$ntrees = res$N
  mySum$max_depth = res$max_depth
  mySum$min_rows = res$min_rows
  mySum$nbins = res$bin_limit
  
  # temp = matrix(unlist(res$cm), nrow = length(res$cm))
  # mySum$prediction_error = 1-sum(diag(temp))/sum(temp)
  return(mySum)
}

.h2o.__getSpeeDRFResults <- function(res, params) {
  result = list()
  params$ntree = res$N
  params$depth = res$max_depth
  params$nbins = res$nbins
  params$classification = TRUE
  
  result$params = params
  #treeStats = unlist(res$treeStats)
  #rf_matrix = rbind(treeStats[1:3], treeStats[4:6])
  #colnames(rf_matrix) = c("Min.", "Max.", "Mean.")
  #rownames(rf_matrix) = c("Depth", "Leaves")
  #result$forest = rf_matrix
  result$mse = as.numeric(res$errs)
  #result$mse <- ifelse(result$mse == -1, NA, result$mse)
  result$mse <- result$mse[length(result$mse)]
  result$verbose_output <- res$verbose_output
  
  if(params$classification) {
    if(!is.null(res$validAUC)) {
      tmp <- .h2o.__getPerfResults(res$validAUC)
      tmp$confusion <- NULL
      result <- c(result, tmp)
    }
    
    class_names <- tail(res$'_domains', 1)[[1]]
    raw_cms <- tail(res$cms, 1)[[1]]$'_arr'
    
    dom_len <- length(class_names)
    if(length(raw_cms) > dom_len)
      raw_cms[[length(raw_cms)]] = NULL
    raw_cms <- lapply(raw_cms, function(x) { if(length(x) > dom_len) x = x[1:dom_len]; return(x) })
    
    
    #    rrr <- NULL
    #    if ( res$parameters$n_folds <= 0) {
    #      f <- function(o) { o[-length(o)] }
    #      rrr <- raw_cms
    #      rrr <- lapply(rrr, f)
    #      rrr <- rrr[-length(rrr)]
    #      raw_cms <<- rrr
    #    }
    #
    #    if (!is.null(rrr)) {raw_cms <- rrr}
    
    result$confusion = .build_cm(raw_cms, class_names)
  }
  
  if(params$importance) {
    result$varimp = data.frame(rbind(res$varimp$varimp, res$varimp$varimpSD))
    result$varimp[3,] = sqrt(params$ntree)*result$varimp[1,]/result$varimp[2,]   # Compute z-scores
    colnames(result$varimp) = res$'_names'[-length(res$'_names')]    #res$varimp$variables
    rownames(result$varimp) = c(res$varimp$method, "Standard Deviation", "Z-Scores")
  }


  extra_json <- doNotCallThisMethod...Unsupported(params$h2o, res$'_key')
  result$priorDistribution <- extra_json$speedrf_model$"_priorClassDist"
  result$modelDistribution <- extra_json$speedrf_model$"_modelClassDist"
  
  return(result)
}

# ------------------------------- Prediction ---------------------------------------- #
h2o.predict <- function(object, newdata) {  
  if( missing(object) ) stop('Must specify object')
  if(!inherits(object, "H2OModel")) stop("object must be an H2O model")
  if( missing(newdata) ) newdata <- object@data
  if(class(newdata) != "H2OParsedData") stop('newdata must be a H2O dataset')
  
  if(class(object) %in% c("H2OGBMModel", "H2OKMeansModel", "H2ODRFModel", "H2ONBModel", "H2ODeepLearningModel", "H2OSpeeDRFModel")) {
    # Set randomized prediction key
    key_prefix = switch(class(object), "H2OGBMModel" = "GBMPredict", "H2OKMeansModel" = "KMeansPredict",
                        "H2ODRFModel" = "DRFPredict", "H2OGLMModel" = "GLM2Predict", "H2ONBModel" = "NBPredict",
                        "H2ODeepLearningModel" = "DeepLearningPredict", "H2OSpeeDRFModel" = "SpeeDRFPredict")
    rand_pred_key = .h2o.__uniqID(key_prefix)
    res = .h2o.__remoteSend(object@data@h2o, .h2o.__PAGE_PREDICT2, model=object@key, data=newdata@key, prediction=rand_pred_key)
    res = .h2o.__remoteSend(object@data@h2o, .h2o.__PAGE_INSPECT2, src_key=rand_pred_key)
    new("H2OParsedData", h2o=object@data@h2o, key=rand_pred_key)
  } else if(class(object) == "H2OPCAModel") {
    # Set randomized prediction key
    rand_pred_key = .h2o.__uniqID("PCAPredict")
    numMatch = colnames(newdata) %in% object@model$params$x
    numPC = min(length(numMatch[numMatch == TRUE]), object@model$num_pc)
    res = .h2o.__remoteSend(object@data@h2o, .h2o.__PAGE_PCASCORE, source=newdata@key, model=object@key, destination_key=rand_pred_key, num_pc=numPC)
    .h2o.__waitOnJob(object@data@h2o, res$job_key)
    new("H2OParsedData", h2o=object@data@h2o, key=rand_pred_key)
  } else if(class(object) == "H2OGLMModel"){
 # Set randomized prediction key
    key_prefix = "GLM2Predict"
    rand_pred_key = .h2o.__uniqID(key_prefix)    
    res = .h2o.__remoteSend(object@data@h2o, .h2o.__PAGE_GLMPREDICT2, model=object@key, data=newdata@key, lambda=object@model$lambda,prediction=rand_pred_key)
    res = .h2o.__remoteSend(object@data@h2o, .h2o.__PAGE_INSPECT2, src_key=rand_pred_key)
    new("H2OParsedData", h2o=object@data@h2o, key=rand_pred_key)
  } else
    stop(paste("Prediction has not yet been implemented for", class(object)))
}

h2o.confusionMatrix <- function(data, reference) {
  if(class(data) != "H2OParsedData") stop("data must be an H2O parsed dataset")
  if(class(reference) != "H2OParsedData") stop("reference must be an H2O parsed dataset")
  if(ncol(data) != 1) stop("Must specify exactly one column for data")
  if(ncol(reference) != 1) stop("Must specify exactly one column for reference")
  
  res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_CONFUSION, actual = reference@key, vactual = 0, predict = data@key, vpredict = 0)
  cm = lapply(res$cm[-length(res$cm)], function(x) { x[-length(x)] })
  # .build_cm(cm, res$actual_domain, res$predicted_domain, transpose = TRUE)
  .build_cm(cm, res$domain, transpose = TRUE)
}

h2o.hitRatio <- function(prediction, reference, k = 10, seed = 0) {
  if(class(prediction) != "H2OParsedData") stop("prediction must be an H2O parsed dataset")
  if(class(reference) != "H2OParsedData") stop("reference must be an H2O parsed dataset")
  if(ncol(reference) != 1) stop("Must specify exactly one column for reference")
  if(!is.numeric(k) || k < 1) stop("k must be an integer greater than 0")
  if(!is.numeric(seed)) stop("seed must be numeric")
  
  res = .h2o.__remoteSend(prediction@h2o, .h2o.__PAGE_HITRATIO, actual = reference@key, vactual = 0, predict = prediction@key, max_k = k, seed = seed)
  temp = res$hit_ratios; names(temp) = make.names(res$actual_domain)
  return(temp)
}

h2o.gapStatistic <- function(data, cols = "", K.max = 10, B = 100, boot_frac = 0.33, seed = 0) {
  args <- .verify_datacols(data, cols)
  if(!is.numeric(B) || B < 1) stop("B must be an integer greater than 0")
  if(!is.numeric(K.max) || K.max < 2) stop("K.max must be an integer greater than 1")
  if(!is.numeric(boot_frac) || boot_frac < 0 || boot_frac > 1) stop("boot_frac must be a number between 0 and 1")
  if(!is.numeric(seed)) stop("seed must be numeric")
  
  res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GAPSTAT, source = data@key, b_max = B, k_max = K.max, bootstrap_fraction = boot_frac, seed = seed)
  .h2o.__waitOnJob(data@h2o, res$job_key)
  res2 = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GAPSTATVIEW, '_modelKey' = res$destination_key)
  
  result = list()
  result$log_within_ss = res2$gap_model$wks
  result$boot_within_ss = res2$gap_model$wkbs
  result$se_boot_within_ss = res2$gap_model$sk
  result$gap_stats = res2$gap_model$gap_stats
  result$k_opt = res2$gap_model$k_best
  return(result)
}

h2o.performance <- function(data, reference, measure = "accuracy", thresholds) {
  if(class(data) != "H2OParsedData") stop("data must be an H2O parsed dataset")
  if(class(reference) != "H2OParsedData") stop("reference must be an H2O parsed dataset")
  if(ncol(data) != 1) stop("Must specify exactly one column for data")
  if(ncol(reference) != 1) stop("Must specify exactly one column for reference")
  if(!measure %in% c("F1", "F2", "accuracy", "precision", "recall", "specificity", "mcc", "max_per_class_error"))
    stop("measure must be one of [F1, F2, accuracy, precision, recall, specificity, mcc, max_per_class_error]")
  if(!missing(thresholds) && !is.numeric(thresholds)) stop("thresholds must be a numeric vector")
  
  criterion = switch(measure, F1 = "maximum_F1", F2 = "maximum_F2", accuracy = "maximum_Accuracy", precision = "maximum_Precision",
                     recall = "maximum_Recall", specificity = "maximum_Specificity", mcc = "maximum_absolute_MCC", max_per_class_error = "minimizing_max_per_class_Error")
  if(missing(thresholds))
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_AUC, actual = reference@key, vactual = 0, predict = data@key, vpredict = 0, threshold_criterion = criterion)
  else
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_AUC, actual = reference@key, vactual = 0, predict = data@key, vpredict = 0, thresholds = .seq_to_string(thresholds), threshold_criterion = criterion)
  
  if (is.list(res$thresholds)) {
    res$thresholds <- as.numeric(unlist(res$thresholds))
  }
  
  meas = as.numeric(res[[measure]])
  result = .h2o.__getPerfResults(res, criterion)
  roc = .get_roc(res$confusion_matrices)
  new("H2OPerfModel", cutoffs = res$thresholds, measure = meas, perf = measure, model = result, roc = roc)
}

.h2o.__getPerfResults <- function(res, criterion) {
  if(missing(criterion)) criterion = res$threshold_criterion
  criterion = gsub("_", " ", res$threshold_criterion)    # Note: For some reason, underscores turned into spaces in JSON threshold_criteria
  idx = which(criterion == res$threshold_criteria)
  
  result = list()
  result$auc = res$AUC
  result$gini = res$Gini
  result$best_cutoff = res$threshold_for_criteria[[idx]]
  result$F1 = res$F1_for_criteria[[idx]]
  result$F2 = res$F2_for_criteria[[idx]]
  result$accuracy = res$accuracy_for_criteria[[idx]]
  result$precision = res$precision_for_criteria[[idx]]
  result$recall = res$recall_for_criteria[[idx]]
  result$specificity = res$specificity_for_criteria[[idx]]
  result$mcc = res$mcc_for_criteria[[idx]]
  result$max_per_class_err = res$max_per_class_error_for_criteria[[idx]]
  result = lapply(result, function(x) { if(x == "NaN") x = NaN; return(x) })   # HACK: NaNs are returned as strings, not numeric values
  
  # Note: Currently, Java assumes actual_domain = predicted_domain, but this may not always be true. Need to fix.
  result$confusion = .build_cm(res$confusion_matrix_for_criteria[[idx]], res$actual_domain)
  return(result)
}

plot.H2OPerfModel <- function(x, type = "cutoffs", ...) {
  if(!type %in% c("cutoffs", "roc")) stop("type must be either 'cutoffs' or 'roc'")
  if(type == "roc") {
    xaxis = "False Positive Rate"; yaxis = "True Positive Rate"
    plot(x@roc$FPR, x@roc$TPR, main = paste(yaxis, "vs", xaxis), xlab = xaxis, ylab = yaxis, ...)
    abline(0, 1, lty = 2)
  } else {
    xaxis = "Cutoff"; yaxis = .toupperFirst(x@perf)
    plot(x@cutoffs, x@measure, main = paste(yaxis, "vs.", xaxis), xlab = xaxis, ylab = yaxis, ...)
    abline(v = x@model$best_cutoff, lty = 2)
  }
}

h2o.anomaly <- function(data, model, key = "", threshold = -1.0) {
  if(missing(data)) stop("Must specify data")
  if(class(data) != "H2OParsedData") stop("data must be an H2O parsed dataset")
  if(missing(model)) stop("Must specify model")
  if(class(model) != "H2ODeepLearningModel") stop("model must be an H2O deep learning model")
  if(!is.character(key)) stop("key must be of class character")
  if(!is.numeric(threshold)) stop("threshold must be of class numeric")
  
  res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_ANOMALY, source = data@key, dl_autoencoder_model = model@key, destination_key = key, thresh = threshold)
  .h2o.__waitOnJob(data@h2o, res$job_key)
  new("H2OParsedData", h2o=data@h2o, key=res$destination_key)
}

# ------------------------------- Helper Functions ---------------------------------------- #
# Used to verify data, x, y and turn into the appropriate things
.verify_dataxy <- function(data, x, y) {
   .verify_dataxy(data, x, y, FALSE)
}
.verify_dataxy <- function(data, x, y, autoencoder) {
  if( missing(data) ) stop('Must specify data')
  if(class(data) != "H2OParsedData") stop('data must be an H2O parsed dataset')
  
  if( missing(x) ) stop('Must specify x')
  if( missing(y) ) stop('Must specify y')
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
  if (!autoencoder) if( y %in% x ) stop(paste(y, 'is both an explanatory and dependent variable'))
  
  x_ignore <- setdiff(setdiff( cc, x ), y)
  if( length(x_ignore) == 0 ) x_ignore <- ''
  list(x=x, y=y, x_i=x_i, x_ignore=x_ignore, y_i=y_i)
}

.verify_datacols <- function(data, cols) {
  if( missing(data) ) stop('Must specify data')
  if(class(data) != "H2OParsedData") stop('data must be an H2O parsed dataset')
  
  if( missing(cols) ) stop('Must specify cols')
  if(!( class(cols) %in% c('numeric', 'character', 'integer') )) stop('cols must be column names or indices')
  
  cc <- colnames(data)
  if(length(cols) == 1 && cols == '') cols = cc
  if(is.character(cols)) {
    # if(any(!(cols %in% cc))) stop(paste(paste(cols[!(cols %in% cc)], collapse=','), 'is not a valid column name'))
    if( any(!cols %in% cc) ) stop("Invalid column names: ", paste(cols[which(!cols %in% cc)], collapse=", "))
    cols_ind <- match(cols, cc)
  } else {
    if(any( cols < 1 | cols > length(cc))) stop(paste('Out of range explanatory variable', paste(cols[cols < 1 | cols > length(cc)], collapse=',')))
    cols_ind <- cols
    cols <- cc[cols_ind]
  }
  
  cols_ignore <- setdiff(cc, cols)
  if( length(cols_ignore) == 0 ) cols_ignore <- ''
  list(cols=cols, cols_ind=cols_ind, cols_ignore=cols_ignore)
}

.h2o.singlerun.internal <- function(algo, data, response, nfolds = 0, validation = new("H2OParsedData", key = as.character(NA)), params = list()) {
  if(!algo %in% c("GBM", "RF", "DeepLearning", "SpeeDRF")) stop("Unsupported algorithm ", algo)
  if(missing(validation)) validation = new("H2OParsedData", key = as.character(NA))
  model_obj = switch(algo, GBM = "H2OGBMModel", RF = "H2ODRFModel", DeepLearning = "H2ODeepLearningModel", SpeeDRF = "H2OSpeeDRFModel")
  model_view = switch(algo, GBM = .h2o.__PAGE_GBMModelView, RF = .h2o.__PAGE_DRFModelView, DeepLearning = .h2o.__PAGE_DeepLearningModelView, SpeeDRF = .h2o.__PAGE_SpeeDRFModelView)
  results_fun = switch(algo, GBM = .h2o.__getGBMResults, RF = .h2o.__getDRFResults, DeepLearning = .h2o.__getDeepLearningResults, SpeeDRF = .h2o.__getSpeeDRFResults)
  
  job_key = response$job_key
  dest_key = response$destination_key
  .h2o.__waitOnJob(data@h2o, job_key)
  # while(!.h2o.__isDone(data@h2o, algo, response)) { Sys.sleep(1) }
  res2 = .h2o.__remoteSend(data@h2o, model_view, '_modelKey'=dest_key)
  modelOrig = results_fun(res2[[3]], params)
  
  res_xval = .h2o.crossvalidation(algo, data, res2[[3]], nfolds, params)
  new(model_obj, key=dest_key, data=data, model=modelOrig, valid=validation, xval=res_xval)
}

.h2o.gridsearch.internal <- function(algo, data, response, nfolds = 0, validation = new("H2OParsedData", key = as.character(NA)), params = list()) {
  if(!algo %in% c("GBM", "KM", "RF", "DeepLearning", "SpeeDRF")) stop("General grid search not supported for ", algo)
  if(missing(validation)) validation = new("H2OParsedData", key = as.character(NA))
  prog_view = switch(algo, GBM = .h2o.__PAGE_GBMProgress, KM = .h2o.__PAGE_KM2Progress, RF = .h2o.__PAGE_DRFProgress, DeepLearning = .h2o.__PAGE_DeepLearningProgress, SpeeDRF = .h2o.__PAGE_SpeeDRFProgress)
  
  job_key = response$job_key
  dest_key = response$destination_key
  .h2o.__waitOnJob(data@h2o, job_key)
  # while(!.h2o.__isDone(data@h2o, algo, response)) { Sys.sleep(1); prog = .h2o.__poll(data@h2o, job_key); setTxtProgressBar(pb, prog) }
  res2 = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GRIDSEARCH, job_key=job_key, destination_key=dest_key)
  allModels = res2$jobs; allErrs = res2$prediction_error
  
  model_obj = switch(algo, GBM = "H2OGBMModel", KM = "H2OKMeansModel", RF = "H2ODRFModel", DeepLearning = "H2ODeepLearningModel", SpeeDRF = "H2OSpeeDRFModel")
  grid_obj = switch(algo, GBM = "H2OGBMGrid", KM = "H2OKMeansGrid", RF = "H2ODRFGrid", DeepLearning = "H2ODeepLearningGrid", SpeeDRF = "H2OSpeeDRFGrid")
  model_view = switch(algo, GBM = .h2o.__PAGE_GBMModelView, KM = .h2o.__PAGE_KM2ModelView, RF = .h2o.__PAGE_DRFModelView, DeepLearning = .h2o.__PAGE_DeepLearningModelView, SpeeDRF = .h2o.__PAGE_SpeeDRFModelView)
  results_fun = switch(algo, GBM = .h2o.__getGBMResults, KM = .h2o.__getKM2Results, RF = .h2o.__getDRFResults, DeepLearning = .h2o.__getDeepLearningResults, SpeeDRF = .h2o.__getSpeeDRFResults)
  
  result = list(); myModelSum = list()
  for(i in 1:length(allModels)) {
    if(algo == "KM")
      resH = .h2o.__remoteSend(data@h2o, model_view, model=allModels[[i]]$destination_key)
    else
      resH = .h2o.__remoteSend(data@h2o, model_view, '_modelKey'=allModels[[i]]$destination_key)
    
    myModelSum[[i]] = switch(algo, GBM = .h2o.__getGBMSummary(resH[[3]], params), KM = .h2o.__getKM2Summary(resH[[3]]), RF = .h2o.__getDRFSummary(resH[[3]]), DeepLearning = .h2o.__getDeepLearningSummary(resH[[3]]), .h2o.__getSpeeDRFSummary(resH[[3]]))
    myModelSum[[i]]$prediction_error = allErrs[[i]]
    myModelSum[[i]]$run_time = allModels[[i]]$end_time - allModels[[i]]$start_time
    modelOrig = results_fun(resH[[3]], params)
    
    if(algo == "KM")
      result[[i]] = new(model_obj, key=allModels[[i]]$destination_key, data=data, model=modelOrig)
    else {
      res_xval = .h2o.crossvalidation(algo, data, resH[[3]], nfolds, params)
      result[[i]] = new(model_obj, key=allModels[[i]]$destination_key, data=data, model=modelOrig, valid=validation, xval=res_xval)
    }
  }
  new(grid_obj, key=dest_key, data=data, model=result, sumtable=myModelSum)
}

.h2o.crossvalidation <- function(algo, data, resModel, nfolds = 0, params = list()) {
  if(!algo %in% c("GBM", "RF", "DeepLearning", "SpeeDRF")) stop("Cross-validation modeling not supported for ", algo)
  if(nfolds == 0) return(list())
  
  model_obj = switch(algo, GBM = "H2OGBMModel", KM = "H2OKMeansModel", RF = "H2ODRFModel", DeepLearning = "H2ODeepLearningModel", SpeeDRF = "H2OSpeeDRFModel")
  model_view = switch(algo, GBM = .h2o.__PAGE_GBMModelView, KM = .h2o.__PAGE_KM2ModelView, RF = .h2o.__PAGE_DRFModelView, DeepLearning = .h2o.__PAGE_DeepLearningModelView, SpeeDRF = .h2o.__PAGE_SpeeDRFModelView)
  results_fun = switch(algo, GBM = .h2o.__getGBMResults, KM = .h2o.__getKM2Results, RF = .h2o.__getDRFResults, DeepLearning = .h2o.__getDeepLearningResults, SpeeDRF = .h2o.__getSpeeDRFResults)
  
  res_xval = list()
  if(algo == "DeepLearning")
    xvalKey = resModel$model_info$job$xval_models
  else
    xvalKey = resModel$parameters$xval_models
  for(i in 1:nfolds) {
      resX = .h2o.__remoteSend(data@h2o, model_view, '_modelKey'=xvalKey[i])
      modelXval = results_fun(resX[[3]], params)
      res_xval[[i]] = new(model_obj, key=xvalKey[i], data=data, model=modelXval, valid=new("H2OParsedData", key=as.character(NA)), xval=list())
    }
  return(res_xval)
}

.is_singlerun <- function(algo, params = list()) {
  if(!algo %in% c("GBM", "KM", "RF", "SpeeDRF")) stop("Unrecognized algorithm: ", algo)
  if(algo == "GBM")
    my_params <- list(params$n.trees, params$interaction.depth, params$n.minobsinnode, params$shrinkage)
  else if(algo == "KM")
    my_params <- list(params$centers, params$iter.max)
  else if(algo == "RF")
    my_params <- list(params$ntree, params$depth, params$nodesize, params$sample.rate, params$nbins, params$max.after.balance.size)
  else if(algo == "SpeeDRF")
    my_params <- list(params$ntree, params$depth, params$sample.rate, params$bin_limit)
  
  isSingle <- all(sapply(my_params, function(x) { length(x) == 1 }))
  return(isSingle)
}

.build_cm <- function(cm, actual_names = NULL, predict_names = actual_names, transpose = TRUE) {
  #browser()
  categories = length(cm)
  cf_matrix = matrix(unlist(cm), nrow=categories)
  if(transpose) cf_matrix = t(cf_matrix)
  
  cf_total = apply(cf_matrix, 2, sum)
  # cf_error = c(apply(cf_matrix, 1, sum)/diag(cf_matrix)-1, 1-sum(diag(cf_matrix))/sum(cf_matrix))
  cf_error = c(1-diag(cf_matrix)/apply(cf_matrix,1,sum), 1-sum(diag(cf_matrix))/sum(cf_matrix))
  cf_matrix = rbind(cf_matrix, cf_total)
  cf_matrix = cbind(cf_matrix, round(cf_error, 3))
  
  if(!is.null(actual_names))
    dimnames(cf_matrix) = list(Actual = c(actual_names, "Totals"), Predicted = c(predict_names, "Error"))
  return(cf_matrix)
}

.get_roc <- function(cms) {
  tmp = sapply(cms, function(x) { c(TN = x[[1]][[1]], FP = x[[1]][[2]], FN = x[[2]][[1]], TP = x[[2]][[2]]) })
  tmp = data.frame(t(tmp))
  tmp$TPR = tmp$TP/(tmp$TP + tmp$FN)
  tmp$FPR = tmp$FP/(tmp$FP + tmp$TN)
  return(tmp)
}

.seq_to_string <- function(vec = as.numeric(NA)) {
  vec <- sort(vec)
  if(length(vec) > 2) {
    vec_diff = diff(vec)
    if(abs(max(vec_diff) - min(vec_diff)) < .Machine$double.eps^0.5)
      return(paste(min(vec), max(vec), vec_diff[1], sep = ":"))
  }
  return(paste(vec, collapse = ","))
}

.toupperFirst <- function(str) {
  paste(toupper(substring(str, 1, 1)), substring(str, 2), sep = "")
}
