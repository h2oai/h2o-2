# Model-building operations and algorithms
# --------------------- Cox Proportional Hazards Model (COXPH) ---------------------- #
# methods(class = "coxph")
#   anova.coxph
#   extractAIC.coxph   // done
#   logLik.coxph       // done
#   model.frame.coxph
#   model.matrix.coxph
#   predict.coxph
#   print.coxph        // show done
#   residuals.coxph
#   summary.coxph      // done
#   survfit.coxph      // done
#   vcov.coxph         // done
h2o.coxph.control <- function(lre = 9, iter.max = 20, ...)
{
  if (!is.numeric(lre) || length(lre) != 1L || is.na(lre) || lre <= 0)
    stop("'lre' must be a positive number")

  if (!is.numeric(iter.max) || length(iter.max) != 1L || is.na(iter.max) ||
      iter.max < 1)
    stop("'iter.max' must be a positive integer")

  list(lre = lre, iter.max = as.integer(iter.max))
}
h2o.coxph <- function(x, y, data, key = "", weights = NULL, offset = NULL,
                      ties = c("efron", "breslow"), init = 0,
                      control = h2o.coxph.control(...), ...)
{
  if (!is(data, "H2OParsedData"))
    stop("'data' must be an H2O parsed dataset")

  cnames <- colnames(data)
  if (!is.character(x) || length(x) == 0L || !all(x %in% cnames))
    stop("'x' must be a character vector specifying column names from 'data'")

  ny <- length(y)
  if (!is.character(y) || ny < 2L || ny > 3L || !all(y %in% cnames))
    stop("'y' must be a character vector of column names from 'data' ",
         "specifying a (start, stop, event) triplet or (stop, event) couplet")

  if (!is.null(weights) &&
      (!is.character(weights) || length(weights) != 1L || !(weights %in% cnames)))
    stop("'weights' must be NULL or a character string specifying a column name from 'data'")

  if (!is.null(offset) && (!is.character(offset) || !all(offset %in% cnames)))
    stop("'offset' must be NULL or a character vector specifying a column names from 'data'")

  if (!is.character(key) && length(key) == 1L)
    stop("'key' must be a character string")
  if (nchar(key) > 0 && !grepl("^[a-zA-Z_][a-zA-Z0-9_.]*$", key))
    stop("'key' must match the regular expression '^[a-zA-Z_][a-zA-Z0-9_.]*$'")
  if (!nzchar(key))
    key <- .h2o.__uniqID("CoxPHModel")

  ties <- match.arg(ties)

  if (!is.numeric(init) || length(init) != 1L || !is.finite(init))
    stop("'init' must be a numeric vector containing finite coefficient starting values")

  job <- .h2o.__remoteSend(data@h2o, .h2o.__PAGE_CoxPH,
                           destination_key = key,
                           source          = data@key,
                           start_column    = if (ny == 3L) y[1L] else NULL,
                           stop_column     = y[ny - 1L],
                           event_column    = y[ny],
                           x_columns       = match(x, cnames) - 1L,
                           weights_column  = weights,
                           offset_columns  = if (is.null(offset)) offset else match(offset, cnames) - 1L,
                           ties            = ties,
                           init            = init,
                           lre_min         = control$lre,
                           iter_max        = control$iter.max)
  job_key  <- job$job_key
  dest_key <- job$destination_key
  .h2o.__waitOnJob(data@h2o, job_key)
  res      <- .h2o.__remoteSend(data@h2o, .h2o.__PAGE_CoxPHModelView,
                                '_modelKey' = dest_key)
  df <- length(res[[3]]$coef)
  coef_names <- res[[3L]]$coef_names
  mcall <- match.call()
  model <-
    list(coefficients = structure(res[[3L]]$coef, names = coef_names),
         var          = do.call(rbind, as.list(res[[3L]]$var_coef)),
         loglik       = c(res[[3L]]$null_loglik, res[[3L]]$loglik),
         score        = res[[3L]]$score_test,
         iter         = res[[3L]]$iter,
         means        = structure(c(unlist(res[[3L]]$x_mean_cat),
                                    unlist(res[[3L]]$x_mean_num)),
                                  names = coef_names),
         means.offset = structure(unlist(res[[3L]]$mean_offset),
                                  names = unlist(res[[3L]]$offset_names)),
         method       = ties,
         n            = res[[3L]]$n,
         nevent       = res[[3L]]$total_event,
         wald.test    = structure(res[[3L]]$wald_test,
                                  names = if (df == 1L) coef_names else NULL),
         call         = mcall)
  summary <-
    list(call         = mcall,
         n            = model$n,
         loglik       = model$loglik,
         nevent       = model$nevent,
         coefficients = structure(cbind(res[[3L]]$coef,    res[[3L]]$exp_coef,
                                        res[[3L]]$se_coef, res[[3L]]$z_coef,
                                        1 - pchisq(res[[3L]]$z_coef^2, 1)),
                                  dimnames =
                                  list(coef_names,
                                       c("coef", "exp(coef)", "se(coef)",
                                         "z", "Pr(>|z|)"))),
         conf.int     = NULL,
         logtest      = c(test = res[[3L]]$loglik_test, df = df,
                          pvalue = 1 - pchisq(res[[3L]]$loglik_test, df)),
         sctest       = c(test = res[[3L]]$score_test,  df = df,
                          pvalue = 1 - pchisq(res[[3L]]$score_test, df)),
         rsq          = c(rsq  = res[[3L]]$rsq,     maxrsq = res[[3L]]$maxrsq),
         waldtest     = c(test = res[[3L]]$wald_test,   df = df,
                          pvalue = 1 - pchisq(res[[3L]]$wald_test, df)),
         used.robust  = FALSE)
  survfit <-
    list(n            = model$n,
         time         = res[[3L]]$time,
         n.risk       = res[[3L]]$n_risk,
         n.event      = res[[3L]]$n_event,
         n.censor     = res[[3L]]$n_censor,
         surv         = NULL,
         type         = ifelse(ny == 2L, "right", "counting"),
         cumhaz       = res[[3L]]$cumhaz_0,
         std.err      = list(var_cumhaz_1 = res[[3L]]$var_cumhaz_1, var_cumhaz_2 = res[[3L]]$var_cumhaz_2),
         upper        = NULL,
         lower        = NULL,
         conf.type    = NULL,
         conf.int     = NULL,
         call         = NULL)
  new("H2OCoxPHModel", key = key, data = data, model = model,
      summary = summary, survfit = survfit)
}

# ----------------------- Generalized Boosting Machines (GBM) ----------------------- #
# TODO: don't support missing x; default to everything?
h2o.gbm <- function(x, y, distribution = 'multinomial', data, key = "", n.trees = 10, interaction.depth = 5, n.minobsinnode = 10, shrinkage = 0.1,
                    n.bins = 20, group_split = TRUE, importance = FALSE, nfolds = 0, validation, holdout.fraction = 0, balance.classes = FALSE, 
                    max.after.balance.size = 5, class.sampling.factors = NULL, grid.parallelism = 1) {
  args <- .verify_dataxy(data, x, y)
  
  if(!is.character(key)) stop("key must be of class character")
  if(nchar(key) > 0 && regexpr("^[a-zA-Z_][a-zA-Z0-9_.]*$", key)[1] == -1)
    stop("key must match the regular expression '^[a-zA-Z_][a-zA-Z0-9_.]*$'")
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
  if(!is.numeric(holdout.fraction)) stop("holdout.fraction must be numeric")
  if(as.numeric(holdout.fraction) > 0 && (!missing(validation) || nfolds>1) ) stop("holdout.fraction cannot be combined with validation or nfolds")
  if(!is.numeric(grid.parallelism)) stop("grid.parallelism must be numeric")
  if(grid.parallelism < 1 || grid.parallelism > 4) stop("grid.parallelism must be 1, 2, 3, or 4")
  
  # NB: externally, 1 based indexing; internally, 0 based
  cols = paste(args$x_i - 1, collapse=",")
  group_split <- as.numeric(group_split)
  if(missing(validation) && nfolds == 0) {
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GBM, source=data@key, holdout_fraction = as.numeric(holdout.fraction), destination_key=key, response=args$y, cols=cols, ntrees=n.trees, max_depth=interaction.depth, learn_rate=shrinkage, family=family, group_split = group_split,
                            min_rows=n.minobsinnode, classification=classification, nbins=n.bins, importance=as.numeric(importance), balance_classes=as.numeric(balance.classes), max_after_balance_size=as.numeric(max.after.balance.size), class_sampling_factors = class.sampling.factors, grid_parallelism = grid.parallelism)
  } else if(missing(validation) && nfolds >= 2) {
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GBM, source=data@key, destination_key=key, response=args$y, cols=cols, ntrees=n.trees, max_depth=interaction.depth, learn_rate=shrinkage, family=family, group_split = group_split,
                            min_rows=n.minobsinnode, classification=classification, nbins=n.bins, importance=as.numeric(importance), n_folds=nfolds, balance_classes=as.numeric(balance.classes), max_after_balance_size=as.numeric(max.after.balance.size), class_sampling_factors = class.sampling.factors, grid_parallelism = grid.parallelism)
  } else if(!missing(validation) && nfolds == 0) {
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GBM, source=data@key, destination_key=key, response=args$y, cols=cols, ntrees=n.trees, max_depth=interaction.depth, learn_rate=shrinkage, family=family, group_split = group_split,
                            min_rows=n.minobsinnode, classification=classification, nbins=n.bins, importance=as.numeric(importance), validation=validation@key, balance_classes=as.numeric(balance.classes), max_after_balance_size=as.numeric(max.after.balance.size), class_sampling_factors = class.sampling.factors, grid_parallelism = grid.parallelism)
  } else stop("Cannot set both validation and nfolds at the same time")
  params = list(x=args$x, y=args$y, distribution=distribution, n.trees=n.trees, interaction.depth=interaction.depth, shrinkage=shrinkage, n.minobsinnode=n.minobsinnode, n.bins=n.bins, importance=importance, nfolds=nfolds, balance.classes=balance.classes, max.after.balance.size=max.after.balance.size, class.sampling.factors = class.sampling.factors,
                h2o = data@h2o, group_split = group_split, grid_parallelism = grid.parallelism)
  
  if(.is_singlerun("GBM", params))
    .h2o.singlerun.internal("GBM", data, res, nfolds, validation, params)
  else
    .h2o.gridsearch.internal("GBM", data, res, nfolds, validation, params)
}

.h2o.__getGBMSummary <- function(res, params) {
  mySum = list()
  mySum$model_key = res$'_key'
  mySum$n.trees = res$N
  mySum$interaction.depth = res$max_depth
  mySum$n.minobsinnode = res$min_rows
  mySum$n.bins = res$nbins
  mySum$shrinkage = res$learn_rate
  mySum$balance.classes = res$balance_classes
  mySum$max.after.balance.size = res$max_after_balance_size
  mySum$class.sampling.factors = res$class_sampling_factors
  
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
  extra_json <- .fetchJSON(params$h2o, res$'_key')
  result$priorDistribution <- extra_json$gbm_model$"_priorClassDist"
  result$modelDistribution <- extra_json$gbm_model$"_modelClassDist"
  params$balance.classes = res$balance_classes
  params$max.after.balance.size = res$max_after_balance_size
  params$class.sampling.factors = res$class_sampling_factors
  params$grid.parallelism = res$grid_parallelism
  result$params = params
  
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
h2o.glm <- function(x, y, data, key = "",
                    offset = NULL,
                    family,
                    link,
                    tweedie.p = ifelse(family == "tweedie", 1.5, NA_real_),
                    prior = NULL,
                    nfolds = 0,
                    alpha = 0.5,
                    lambda = 1e-5,
                    lambda_search = FALSE,
                    nlambda = -1,
                    lambda.min.ratio = -1,
                    max_predictors = -1,
                    return_all_lambda = FALSE,
                    strong_rules = TRUE,
                    standardize = TRUE,
                    intercept = TRUE,
                    non_negative = FALSE,
                    use_all_factor_levels = FALSE,
                    variable_importances = FALSE,
                    epsilon = 1e-4,
                    iter.max = 100,
                    higher_accuracy = FALSE,
                    beta_constraints = NULL,
                    disable_line_search = FALSE)
{
  args <- .verify_dataxy(data, x, y)

  if (!is.character(key) && length(key) == 1L)
    stop("'key' must be a character string")
  if (nchar(key) > 0 && !grepl("^[a-zA-Z_][a-zA-Z0-9_.]*$", key))
    stop("'key' must match the regular expression '^[a-zA-Z_][a-zA-Z0-9_.]*$'")

  if (is.null(offset)) {
    offset <- ""
    x_ignore <- seq_len(ncol(data))[- sort(unique(c(args$y_i, args$x_i)))]
  } else if(!(is.numeric(offset) || (is.character(offset) && (offset %in% colnames(data)))))
    stop("offset must be either an index or column name")
  else {
    if (is.character(offset))
      offset <- match(offset, colnames(data))
    x_ignore <- seq_len(ncol(data))[- sort(unique(c(args$y_i, args$x_i, offset)))]
    offset <- offset - 1L
  }

  if (length(x_ignore) == 0L)
    x_ignore <- ""
  else
    x_ignore <- x_ignore - 1L

  if (!is.character(family) || length(family) != 1L)
    stop("'family' must be a character string")
  family <- match.arg(family,
                      c("gaussian", "binomial", "poisson", "gamma", "tweedie"))

  if (missing(link))
    link <- "family_default"
  else if (!is.character(link) || length(link) != 1L)
    stop("'link' must be a character string")
  else {
    link <- match.arg(link, c("identity", "inverse", "log", "logit", "tweedie"))
    switch(family,
           gaussian = {
             if (!(link %in% c("identity", "log", "inverse")))
               stop("'link' must be one of 'identity', 'log', or 'inverse' when family = 'gaussian'")
           },
           binomial = {
             if (!(link %in% c("logit", "log")))
               stop("'link' must be one of 'logit' or 'log' when family = 'binomial'")
           } ,
           poisson = {
             if (!(link %in% c("log", "identity")))
               stop("'link' must be one of 'log' or 'identity' when family = 'poisson'")
           },
           gamma = {
             if (!(link %in% c("inverse", "log", "identity")))
               stop("'link' must be one of 'inverse', 'log', or 'identity' when family = 'gamma'")
           },
           tweedie = {
             if (link != "tweedie")
               stop("'link' must be one of 'tweedie' when family = 'tweedie'")
           })
  }

  if (!is.numeric(tweedie.p) || length(tweedie.p) != 1L)
    stop("'tweedie.p' must a single number")
  if (family == "tweedie" && is.na(tweedie.p))
    stop("'tweedie.p' must be non-NA when family = 'tweedie'")

  if (is.null(prior))
    prior <- -1L
  else if (family != "binomial")
    stop("'prior' may only be set when family = 'binomial'")
  else if (!is.numeric(prior) || length(prior) != 1L || is.na(prior) || prior < 0 || prior > 1)
    stop("'prior' must be a number in [0, 1]")

  if (!is.numeric(nfolds) || length(nfolds) != 1L || is.na(nfolds) || nfolds < 0)
    stop("'nfolds' must be a non-negative integer")
  nfolds <- as.integer(nfolds)

  if (!is.numeric(alpha) || length(alpha) == 0L || any(is.na(alpha) | alpha < 0 | alpha > 1))
    stop("'alpha' must be a vector of numbers in [0, 1]")

  if (!is.numeric(lambda) || length(lambda) == 0L || any(is.na(lambda) | lambda < 0))
    stop("'lambda' must be a vector of non-negative numbers")

  if (!is.logical(lambda_search) || length(lambda_search) != 1L || is.na(lambda_search))
    stop("'lambda_search' must be TRUE / FALSE")
  if (lambda_search && length(lambda) > 1L)
    stop("'lambda' must be a single non-negative value when lambda_search = TRUE")

  if (!is.numeric(nlambda) || length(nlambda) != 1L || is.na(nlambda) || (nlambda != -1 && nlambda < 0))
    stop("'nlambda' must be a non-negative integer")
  nlambda <- as.integer(nlambda)

  if (!is.numeric(lambda.min.ratio) || length(lambda.min.ratio) != 1L || is.na(lambda.min.ratio) ||
      (lambda.min.ratio != -1 && lambda.min.ratio < 0) || lambda.min.ratio > 1)
    stop("'lambda.min.ratio' must be a number in [0, 1]")

  if (!is.numeric(max_predictors) || length(max_predictors) != 1L || is.na(max_predictors) ||
      max_predictors < -1)
    stop("'max_predictors' must be a non-negative integer")
  max_predictors <- as.integer(max_predictors)

  if (!is.logical(return_all_lambda) || length(return_all_lambda) != 1L || is.na(return_all_lambda))
    stop("'return_all_lambda' must be TRUE / FALSE")

  if (!is.logical(strong_rules) || length(strong_rules) != 1L || is.na(strong_rules))
    stop("'strong_rules' must be TRUE / FALSE")

  if (!is.logical(standardize) || length(standardize) != 1L || is.na(standardize))
    stop("'standardize' must be TRUE / FALSE")

  if (!is.logical(intercept) || length(intercept) != 1L || is.na(intercept))
    stop("'intercept' must be TRUE / FALSE")

  if (!is.logical(non_negative) || length(non_negative) != 1L || is.na(non_negative))
    stop("'non_negative' must be TRUE / FALSE")

  if (!is.logical(use_all_factor_levels) || length(use_all_factor_levels) != 1L ||
      is.na(use_all_factor_levels))
    stop("'use_all_factor_levels' must be TRUE / FALSE")

  if (!is.logical(variable_importances) || length(variable_importances) != 1L ||
      is.na(variable_importances))
    stop("'variable_importances' must be TRUE / FALSE")

  if (!is.numeric(epsilon) || length(epsilon) != 1L || is.na(epsilon) || epsilon < 0)
    stop("'epsilon' must be a non-negative number")

  if (!is.numeric(iter.max) || length(iter.max) != 1L || is.na(iter.max) || iter.max < 1)
    stop("'iter.max' must be a positive integer")
  iter.max <- as.integer(iter.max)

  if (!is.logical(higher_accuracy) || length(higher_accuracy) != 1L || is.na(higher_accuracy))
    stop("'higher_accuracy' must be TRUE / FALSE")

  if (!is.null(beta_constraints)) {
    if (!inherits(beta_constraints, "data.frame") && !inherits(beta_constraints, "H2OParsedData"))
      stop(paste("`beta_constraints` must be an H2OParsedData or R data.frame. Got: ", class(beta_constraints)))
    if (inherits(beta_constraints, "data.frame"))
      beta_constraints <- as.h2o(data@h2o, beta_constraints)
  }

  params <- list(data@h2o, .h2o.__PAGE_GLM2,
                 destination_key       = key,
                 source                = data@key,
                 response              = args$y,
                 ignored_cols          = paste0(x_ignore, collapse = ","),
                 offset                = offset,
                 family                = family,
                 link                  = link,
                 n_folds               = nfolds,
                 alpha                 = alpha,
                 nlambdas              = nlambda,
                 lambda_min_ratio      = lambda.min.ratio,
                 lambda                = lambda,
                 lambda_search         = as.integer(lambda_search),
                 max_predictors        = max_predictors,
                 strong_rules          = as.integer(strong_rules),
                 standardize           = as.integer(standardize),
                 intercept             = as.integer(intercept),
                 use_all_factor_levels = as.integer(use_all_factor_levels),
                 non_negative          = as.integer(non_negative),
                 variable_importances  = as.integer(variable_importances),
                 beta_epsilon          = epsilon,
                 max_iter              = iter.max,
                 higher_accuracy       = as.integer(higher_accuracy),
                 disable_line_search   = as.integer(disable_line_search))

  if (family == "binomial")
    params <- c(params, list(prior = prior))
  else if (family == "tweedie")
    params <- c(params, list(tweedie_variance_power = tweedie.p))

  if (!is.null(beta_constraints)) params <- c(params, list(beta_constraints = beta_constraints@key))

  res <- do.call(.h2o.__remoteSend, params)
  .h2o.__waitOnJob(data@h2o, res$job_key)

  if (length(alpha) == 1L)
    .h2o.get.glm(data@h2o, as.character(res$destination_key), return_all_lambda)
  else
    .h2o.get.glm.grid(data@h2o, as.character(res$destination_key), return_all_lambda, data)
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
  all.models <- .h2o.get.glm(data@h2o, model_key, TRUE)
  lambda_idx <- which(all.models@lambdas == lambda)
  if (length(lambda_idx) == 0) stop("Cannot find ", lambda, " in list of lambda searched over for this model")
  all.models@models[[lambda_idx]]
}

# ------------------------------ K-Means Clustering --------------------------------- #
h2o.kmeans <- function(data, centers, cols = '', key = "", iter.max = 10, normalize = FALSE, init = "none", seed = 0, dropNACols = FALSE) {
  args <- .verify_datacols(data, cols)
  
  if(!is.character(key)) stop("key must be of class character")
  if(nchar(key) > 0 && regexpr("^[a-zA-Z_][a-zA-Z0-9_.]*$", key)[1] == -1)
    stop("key must match the regular expression '^[a-zA-Z_][a-zA-Z0-9_.]*$'")
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
  
  res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_KMEANS2, source=data@key, destination_key=key, ignored_cols=args$cols_ignore, k=centers, max_iter=iter.max, normalize=as.numeric(normalize), initialization=myInit, seed=seed, drop_na_cols=as.numeric(dropNACols))
  params = list(cols=args$cols, centers=centers, iter.max=iter.max, normalize=normalize, init=myInit, seed=seed)
  
  if(.is_singlerun("KM", params)) {
    .h2o.__waitOnJob(data@h2o, res$job_key)
    # while(!.h2o.__isDone(data@h2o, "KM", res)) { Sys.sleep(1) }
    res2 = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_KM2ModelView, '_modelKey'=res$destination_key)
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
  params$centers = res$parameters$k
  params$iter.max = res$parameters$max_iter
  result$params = params

  result$cluster <- if( .check.exists(data@h2o, clusters_key) ) NULL else .h2o.exec2(clusters_key, h2o = data@h2o, clusters_key)
  feat <- res$'_names'
  result$centers <- t(matrix(unlist(res$centers), ncol = res$parameters$k))
  dimnames(result$centers) <- list(seq(1,res$parameters$k), feat)
  #result$totss <- res$total_SS
  result$withinss <- res$within_cluster_variances ## FIXME: sum of squares != variances (bad name of the latter)
  result$tot.withinss <- res$total_within_SS
  #result$betweenss <- res$between_cluster_SS
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
h2o.deeplearning <- function(x, y, data, key = "",
                             override_with_best_model,
                             classification = TRUE,
                             nfolds = 0,
                             validation,
                             holdout_fraction = 0,
                             # ----- AUTOGENERATED PARAMETERS BEGIN -----
                             checkpoint,
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
                             class_sampling_factors,
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
                             col_major,
                             max_categorical_features,
                             reproducible
                             # ----- AUTOGENERATED PARAMETERS END -----
)
{
  colargs <- .verify_dataxy_full(data, x, y, autoencoder)
  parms = list()

  parms$'source' = data@key
  parms$response = colargs$y
  parms$ignored_cols = colargs$x_ignore
  #parms$expert_mode = ifelse(!missing(autoencoder) && autoencoder, 1, 0)
  parms$expert_mode = 1 #always enable expert mode from R, since all options can be set

  if (! missing(classification)) {
    if (! is.logical(classification)) stop('classification must be TRUE or FALSE')
    parms$classification = as.numeric(classification)
  }
  if(!is.character(key)) stop("key must be of class character")
  if(nchar(key) > 0 && regexpr("^[a-zA-Z_][a-zA-Z0-9_.]*$", key)[1] == -1)
    stop("key must match the regular expression '^[a-zA-Z_][a-zA-Z0-9_.]*$'")
  parms$destination_key = key

  if(!is.numeric(nfolds)) stop("nfolds must be numeric")
  if(nfolds == 1) stop("nfolds cannot be 1")
  if(!missing(validation) && class(validation) != "H2OParsedData")
    stop("validation must be an H2O parsed dataset")
  if(!is.numeric(holdout_fraction)) stop("holdout_fraction must be numeric")
  if(as.numeric(holdout_fraction) > 0 && (!missing(validation) || nfolds>1) ) stop("holdout_fraction cannot be combined with validation or nfolds")

  if(missing(validation) && nfolds == 0) {
    validation = new ("H2OParsedData", key = as.character(NA))
    parms$n_folds = nfolds
  } else if(missing(validation) && nfolds >= 2) {
    validation = new("H2OParsedData", key = as.character(NA))
    parms$n_folds = nfolds
  } else if(!missing(validation) && nfolds == 0)
    parms$validation = validation@key
  else stop("Cannot set both validation and nfolds at the same time")

  if (missing(checkpoint)) {
    parms$checkpoint = ""
  } else {
    if(is.character(checkpoint)) {
      if(nchar(checkpoint) > 0 && regexpr("^[a-zA-Z_][a-zA-Z0-9_.]*$", checkpoint)[1] == -1)
        stop("checkpoint must match the regular expression '^[a-zA-Z_][a-zA-Z0-9_.]*$'")
      parms$checkpoint = checkpoint
    } else {
      if (class(checkpoint) != "H2ODeepLearningModel") stop('checkpoint must be valid key or an object of type H2ODeepLearningModel')
      parms$checkpoint = checkpoint@key
    }
  }

  # ----- AUTOGENERATED PARAMETERS BEGIN -----
  parms = .addFloatParm(parms, k="holdout_fraction", v=holdout_fraction)
  parms = .addBooleanParm(parms, k="override_with_best_model", v=override_with_best_model)
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
  parms = .addDoubleArrayParm(parms, k="class_sampling_factors", v=class_sampling_factors)
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
  parms = .addIntParm(parms, k="max_categorical_features", v=max_categorical_features)
  parms = .addBooleanParm(parms, k="reproducible", v=reproducible)
  # ----- AUTOGENERATED PARAMETERS END -----
  
  res = .h2o.__remoteSendWithParms(data@h2o, .h2o.__PAGE_DeepLearning, parms)
  parms$h2o <- data@h2o
  noGrid <- missing(hidden) || !(is.list(hidden) && length(hidden) > 1)
  noGrid <- noGrid && (missing(l1) || length(l1) == 1)
  noGrid <- noGrid && (missing(l2) || length(l2) == 1)
  noGrid <- noGrid && (missing(activation) || length(activation) == 1)
  noGrid <- noGrid && (missing(rho) || length(rho) == 1) && (missing(epsilon) || length(epsilon) == 1)
  noGrid <- noGrid && (missing(epochs) || length(epochs) == 1) && (missing(train_samples_per_iteration) || length(train_samples_per_iteration) == 1)
  noGrid <- noGrid && (missing(adaptive_rate) || length(adaptive_rate) == 1) && (missing(rate_annealing) || length(rate_annealing) == 1)
  noGrid <- noGrid && (missing(rate_decay) || length(rate_decay) == 1)
  noGrid <- noGrid && (missing(momentum_ramp) || length(momentum_ramp) == 1)
  noGrid <- noGrid && (missing(momentum_stable) || length(momentum_stable) == 1)
  noGrid <- noGrid && (missing(momentum_start) || length(momentum_start) == 1)
  noGrid <- noGrid && (missing(nesterov_accelerated_gradient) || length(nesterov_accelerated_gradient) == 1)
  noGrid <- noGrid && (missing(override_with_best_model) || length(override_with_best_model) == 1)
  noGrid <- noGrid && (missing(seed) || length(seed) == 1)
  noGrid <- noGrid && (missing(input_dropout_ratio) || length(input_dropout_ratio) == 1)
  noGrid <- noGrid && (missing(hidden_dropout_ratios) || (!is.list(hidden_dropout_ratios) && length(hidden_dropout_ratios) > 1))
  noGrid <- noGrid && (missing(max_w2) || length(max_w2) == 1)
  noGrid <- noGrid && (missing(initial_weight_distribution) || length(initial_weight_distribution) == 1)
  noGrid <- noGrid && (missing(initial_weight_scale) || length(initial_weight_scale) == 1)
  noGrid <- noGrid && (missing(loss) || length(loss) == 1)
  noGrid <- noGrid && (missing(balance_classes) || length(balance_classes) == 1)
  noGrid <- noGrid && (missing(max_after_balance_size) || length(max_after_balance_size) == 1)
  noGrid <- noGrid && (missing(fast_mode) || length(fast_mode) == 1)
  noGrid <- noGrid && (missing(shuffle_training_data) || length(shuffle_training_data) == 1)
  noGrid <- noGrid && (missing(max_categorical_features) || length(max_categorical_features) == 1)
  if(noGrid)
    .h2o.singlerun.internal("DeepLearning", data, res, nfolds, validation, parms)
  else {
    .h2o.gridsearch.internal("DeepLearning", data, res, nfolds, validation, parms)
  }
}

.h2o.__getDeepLearningSummary <- function(res) {
    result = list()
    model_params = res$model_info$job
    model_params$Request2 = NULL; model_params$response_info = NULL
    model_params$'source' = NULL; model_params$validation = NULL
    model_params$job_key = NULL;
    model_params$start_time = NULL; model_params$end_time = NULL
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
    result = model_params

    #for backward-compatibility
    result$l1_reg = result$l1
    result$l2_reg = result$l2
    result$model_key = result$destination_key

    return(result)
}

.h2o.__getDeepLearningResults <- function(res, params = list()) {
  result = list()
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
  extra_json <- .fetchJSON(params$h2o, res$'_key')
  result$validationKey <- extra_json$deeplearning_model$"_validationKey"
  result$priorDistribution <- extra_json$deeplearning_model$"_priorClassDist"
  result$modelDistribution <- extra_json$deeplearning_model$"_modelClassDist"
  errs = tail(res$errors, 1)[[1]]

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
  
  if (result$params$classification == 0) {
    result$train_sqr_error = as.numeric(errs$train_mse)
    result$valid_sqr_error = as.numeric(errs$valid_mse)
    result$train_class_error = NULL
    result$valid_class_error = NULL
  } else {
    result$train_sqr_error = NULL
    result$valid_sqr_error = NULL
    result$train_class_error = as.numeric(errs$train_err)
    result$valid_class_error = as.numeric(errs$valid_err)
  }

  if(!is.null(errs$validAUC)) {
    tmp <- .h2o.__getPerfResults(errs$validAUC)
    tmp$confusion <- NULL 
    result <- c(result, tmp) 
  }

  result$train_auc <- res$errors[[length(res$errors)]]$trainAUC$AUC

  if(!is.null(errs$valid_hitratio)) {
    max_k <- errs$valid_hitratio$max_k
    hit_ratios <- errs$valid_hitratio$hit_ratios
    result$hit_ratios <- data.frame(k = 1:max_k, hit_ratios = hit_ratios)
  }
  
  if(!is.null(errs$variable_importances)) {
    result$varimp <- as.data.frame(t(errs$variable_importances$varimp))
    names(result$varimp) <- errs$variable_importances$variables
    result$varimp <- sort(result$varimp, decreasing = TRUE)
  }
  return(result)
}

# -------------------------------- Naive Bayes ----------------------------- #
h2o.naiveBayes <- function(x, y, data, key = "", laplace = 0, dropNACols = FALSE) {
  args <- .verify_dataxy(data, x, y)
  
  if(!is.character(key)) stop("key must be of class character")
  if(nchar(key) > 0 && regexpr("^[a-zA-Z_][a-zA-Z0-9_.]*$", key)[1] == -1)
    stop("key must match the regular expression '^[a-zA-Z_][a-zA-Z0-9_.]*$'")
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
h2o.prcomp <- function(data, tol=0, cols = "", max_pc = 5000, key = "", standardize=TRUE, retx=FALSE) {
  args <- .verify_datacols(data, cols)
  
  if(!is.character(key)) stop("key must be of class character")
  if(nchar(key) > 0 && regexpr("^[a-zA-Z_][a-zA-Z0-9_.]*$", key)[1] == -1)
    stop("key must match the regular expression '^[a-zA-Z_][a-zA-Z0-9_.]*$'")
  if(!is.numeric(tol)) stop('tol must be numeric')
  if(!is.logical(standardize)) stop('standardize must be TRUE or FALSE')
  if(!is.logical(retx)) stop('retx must be TRUE or FALSE')
  if(!is.numeric(max_pc)) stop('max_pc must be a numeric')
  
  res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_PCA, source=data@key, destination_key=key, ignored_cols = args$cols_ignore, tolerance=tol, standardize=as.numeric(standardize))
  .h2o.__waitOnJob(data@h2o, res$job_key)
  destKey = res$destination_key
  # while(!.h2o.__isDone(data@h2o, "PCA", res)) { Sys.sleep(1) }
  res2 = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_PCAModelView, '_modelKey'=destKey)
  res2 = res2$pca_model
  
  result = list()
  result$params$names = res2$'_names'
  result$params$x = res2$namesExp
  result$num_pc = res2$num_pc
  result$standardized = standardize
  result$sdev = res2$sdev
  nfeat = length(res2$eigVec[[1]])
  if(max_pc > nfeat) max_pc = nfeat
  temp = t(matrix(unlist(res2$eigVec), nrow = nfeat))[,1:max_pc]
  temp = as.data.frame(temp)
  rownames(temp) = res2$namesExp #'_names'
  colnames(temp) = paste("PC", seq(0, ncol(temp)-1), sep="")
  result$rotation = temp
  
  if(retx) result$x = h2o.predict(new("H2OPCAModel", key=destKey, data=data, model=result), num_pc = max_pc)
  new("H2OPCAModel", key=destKey, data=data, model=result)
}

h2o.pcr <- function(x, y, data, key = "", ncomp, family, nfolds = 10, alpha = 0.5, lambda = 1.0e-5, epsilon = 1.0e-5, tweedie.p = ifelse(family=="tweedie", 0, as.numeric(NA))) {
  args <- .verify_dataxy(data, x, y)
  
  if(!is.character(key)) stop("key must be of class character")
  if(nchar(key) > 0 && regexpr("^[a-zA-Z_][a-zA-Z0-9_.]*$", key)[1] == -1)
    stop("key must match the regular expression '^[a-zA-Z_][a-zA-Z0-9_.]*$'")
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
  myScore <- h2o.predict(myModel, num_pc = ncomp)
  
  myScore[,ncomp+1] = data[,args$y_i]    # Bind response to frame of principal components
  myGLMData = .h2o.exec2(myScore@key, h2o = data@h2o, myScore@key)
  h2o.glm(x = 1:ncomp,
          y = ncomp+1,
          data = myGLMData,
          key = key,
          family = family,
          nfolds = nfolds,
          alpha = alpha,
          lambda = lambda,
          epsilon = epsilon,
          standardize = FALSE,
          tweedie.p = tweedie.p)
}

.h2o.prcomp.internal <- function(data, x_ignore, dest, max_pc=5000, tol=0, standardize=TRUE) {
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

.get.pca.results <- function(data, json, destKey, params) {
  json$params <- params
  json$rotation <- t(matrix(unlist(json$eigVec), nrow = length(json$eigVec[[1]])))
  rownames(json$rotation) <- json$'namesExp'
  colnames(json$rotation) <- paste("PC", seq(1, ncol(json$rotation)), sep = "")
  new("H2OPCAModel", key = destKey, data = data, model = json)
}

# ----------------------------------- Random Forest --------------------------------- #
h2o.randomForest <- function(x, y, data, key="", classification=TRUE, ntree=50, depth=20, mtries = -1, sample.rate=2/3,
                             nbins=20, seed=-1, importance=FALSE, score.each.iteration=FALSE, nfolds=0, validation, 
                             holdout.fraction=0, nodesize=1, balance.classes=FALSE, max.after.balance.size=5, 
                             class.sampling.factors = NULL, doGrpSplit=TRUE, verbose = FALSE, oobee = TRUE, 
                             stat.type = "ENTROPY", type = "fast") {
  if (type == "fast") {
    if (!is.null(class.sampling.factors)) stop("class.sampling.factors requires type = 'BigData'.")
    if(score.each.iteration) stop("score.each.iteration = TRUE requires type = 'BigData'")
    return(h2o.SpeeDRF(x, y, data, key, classification, nfolds, validation, holdout.fraction, mtries, ntree, depth, sample.rate, oobee,
                       importance, nbins, seed, stat.type, balance.classes, verbose))
  }
  args <- .verify_dataxy(data, x, y)
  
  if(!is.character(key)) stop("key must be of class character")
  if(nchar(key) > 0 && regexpr("^[a-zA-Z_][a-zA-Z0-9_.]*$", key)[1] == -1)
    stop("key must match the regular expression '^[a-zA-Z_][a-zA-Z0-9_.]*$'")
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
  if(!is.logical(score.each.iteration)) stop("score.each.iteration must be logical (TRUE or FALSE)")
  
  if(!is.logical(balance.classes)) stop('balance.classes must be logical (TRUE or FALSE)')
  if(!is.numeric(max.after.balance.size)) stop('max.after.balance.size must be a number')
  if( any(max.after.balance.size <= 0) ) stop('max.after.balance.size must be >= 0')
  if(balance.classes && !classification) stop('balance.classes can only be used for classification')
  if(!is.numeric(nodesize)) stop('nodesize must be a number')
  if( any(nodesize < 1) ) stop('nodesize must be >= 1')
  if(!is.logical(doGrpSplit)) stop("doGrpSplit must be logical (TRUE or FALSE)")
  
  if(!is.numeric(nfolds)) stop("nfolds must be numeric")
  if(nfolds == 1) stop("nfolds cannot be 1")
  if(!missing(validation) && class(validation) != "H2OParsedData")
    stop("validation must be an H2O parsed dataset")
  if(!is.numeric(holdout.fraction)) stop("holdout.fraction must be numeric")
  if(holdout.fraction > 0 && (!missing(validation) || nfolds>1) ) stop("holdout.fraction cannot be combined with validation or nfolds")

  # NB: externally, 1 based indexing; internally, 0 based
  cols <- paste(args$x_i - 1, collapse=',')
  if(missing(validation) && nfolds == 0) {
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_DRF, source=data@key, destination_key=key, response=args$y, cols=cols, ntrees=ntree, max_depth=depth, min_rows=nodesize, sample_rate=sample.rate, nbins=nbins, mtries = mtries, seed=seed, importance=as.numeric(importance), score_each_iteration=as.numeric(score.each.iteration),
                            classification=as.numeric(classification), holdout_fraction = as.numeric(holdout.fraction), balance_classes=as.numeric(balance.classes), max_after_balance_size=as.numeric(max.after.balance.size), class_sampling_factors = class.sampling.factors, do_grpsplit=as.numeric(doGrpSplit))
  } else if(missing(validation) && nfolds >= 2) {
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_DRF, source=data@key, destination_key=key, response=args$y, cols=cols, ntrees=ntree, mtries = mtries, max_depth=depth, min_rows=nodesize, sample_rate=sample.rate, nbins=nbins, seed=seed, importance=as.numeric(importance), score_each_iteration=as.numeric(score.each.iteration),
                            classification=as.numeric(classification), n_folds=nfolds, balance_classes=as.numeric(balance.classes), max_after_balance_size=as.numeric(max.after.balance.size), class_sampling_factors = class.sampling.factors, do_grpsplit=as.numeric(doGrpSplit))
  } else if(!missing(validation) && nfolds == 0) {
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_DRF, source=data@key, destination_key=key, response=args$y, cols=cols, ntrees=ntree, mtries = mtries, max_depth=depth, min_rows=nodesize, sample_rate=sample.rate, nbins=nbins, seed=seed, importance=as.numeric(importance), score_each_iteration=as.numeric(score.each.iteration),
                            classification=as.numeric(classification), validation=validation@key, balance_classes=as.numeric(balance.classes), max_after_balance_size=as.numeric(max.after.balance.size), class_sampling_factors = class.sampling.factors, do_grpsplit=as.numeric(doGrpSplit))
  } else stop("Cannot set both validation and nfolds at the same time")
  params = list(x=args$x, y=args$y, type="BigData", ntree=ntree, mtries = mtries, depth=depth, nbins=nbins, sample.rate=sample.rate, nbins=nbins, importance=importance, score.each.iteration=score.each.iteration, nfolds=nfolds, balance.classes=balance.classes, max.after.balance.size=max.after.balance.size, class.sampling.factors = class.sampling.factors, nodesize=nodesize, h2o = data@h2o)
  
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
  mySum$class.sampling.factors = res$class_sampling_factors

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
  params$class.sampling.factors = res$class_sampling_factors

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
      result <- c(result, tmp)
    } else {
      class_names = res$'cmDomain'
      result$confusion = .build_cm(tail(res$'cms', 1)[[1]]$'_arr', class_names) 
    }
  }

  extra_json <- .fetchJSON(params$h2o, res$'_key')
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
h2o.SpeeDRF <- function(x, y, data, key="", classification=TRUE, nfolds=0, validation, holdout.fraction=0,
                        mtries=-1,
                        ntree=50, 
                        depth=20,
                        sample.rate=2/3,
                        oobee = TRUE,
                        importance = FALSE,
                        nbins=1024, 
                        seed=-1,
                        stat.type="ENTROPY",
                        balance.classes=FALSE,
                        verbose=FALSE
    ) {
  nbins <- max(nbins, 1024)
  args <- .verify_dataxy(data, x, y)
  if(!classification) stop("Use type = \"BigData\" for random forest regression.")
  if(!is.character(key)) stop("key must be of class character")
  if(nchar(key) > 0 && regexpr("^[a-zA-Z_][a-zA-Z0-9_.]*$", key)[1] == -1)
    stop("key must match the regular expression '^[a-zA-Z_][a-zA-Z0-9_.]*$'")
  if(!is.numeric(ntree)) stop('ntree must be a number')
  if( any(ntree < 1) ) stop('ntree must be >= 1')
  if(!is.numeric(depth)) stop('depth must be a number')
  if( any(depth < 1) ) stop('depth must be >= 1')
  if(!is.numeric(sample.rate)) stop('sample.rate must be a number')
  if( any(sample.rate < 0 || sample.rate > 1) ) stop('sample.rate must be between 0 and 1')
  if(!is.numeric(nbins)) stop('nbins must be a number')
  if( any(nbins < 1)) stop('nbins must be an integer >= 1')
  if(!is.numeric(seed)) stop("seed must be an integer")
  if(!(stat.type %in% c("ENTROPY", "GINI", "TWOING"))) stop(paste("stat.type must be either GINI or ENTROPY or TWOING. Input was: ", stat.type, sep = ""))
  if(!(is.logical(oobee))) stop(paste("oobee must be logical (TRUE or FALSE). Input was: ", oobee, " and is of type ", mode(oobee), sep = ""))
  #if(!(sampling_strategy %in% c("RANDOM", "STRATIFIED"))) stop(paste("sampling_strategy must be either RANDOM or STRATIFIED. Input was: ", sampling_strategy, sep = ""))

#  if(!is.logical(local_mode)) stop(paste("local_mode must be a logical value. Input was: ", local_mode, " class: ", class(local_mode), sep =""))
  if(!is.numeric(nfolds)) stop("nfolds must be numeric")
  if(nfolds == 1) stop("nfolds cannot be 1")
  if(!missing(validation) && class(validation) != "H2OParsedData")
    stop("validation must be an H2O parsed dataset")
  if(!is.numeric(holdout.fraction)) stop("holdout.fraction must be numeric")
  if(holdout.fraction > 0 && (!missing(validation) || nfolds>1 || oobee)) stop("holdout.fraction cannot be combined with validation, nfolds or oobee")
  if(!is.logical(verbose)) stop("verbose must be a logical value")

  if (missing(validation) && nfolds == 0 && oobee) {
    res <- .h2o.__remoteSend(data@h2o, .h2o.__PAGE_SpeeDRF, source=data@key, destination_key=key, response=args$y, ignored_cols=args$x_ignore, balance_classes = as.numeric(balance.classes), ntrees=ntree, max_depth=depth, mtries = mtries, importance=as.numeric(importance),
                                sample_rate=sample.rate, nbins=nbins, seed=seed, select_stat_type = stat.type, oobee=as.numeric(oobee), sampling_strategy="RANDOM", verbose = as.numeric(verbose))

  } else if(missing(validation) && nfolds >= 2 && oobee) {
        res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_SpeeDRF, source=data@key, destination_key=key, response=args$y, ignored_cols=args$x_ignore, ntrees=ntree, balance_classes = as.numeric(balance.classes), max_depth=depth, mtries = mtries, n_folds=nfolds, importance=as.numeric(importance),
                                sample_rate=sample.rate, nbins=nbins, seed=seed, select_stat_type=stat.type, oobee=as.numeric(oobee), sampling_strategy="RANDOM", verbose = as.numeric(verbose))

  } else if(missing(validation) && nfolds == 0) {
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_SpeeDRF, source=data@key, destination_key=key, response=args$y, holdout_fraction = as.numeric(holdout.fraction), ignored_cols=args$x_ignore, balance_classes = as.numeric(balance.classes), ntrees=ntree, max_depth=depth, mtries = mtries, importance=as.numeric(importance),
                            sample_rate=sample.rate, nbins=nbins, seed=seed, select_stat_type = stat.type, oobee=as.numeric(oobee), sampling_strategy="RANDOM", verbose = as.numeric(verbose))
  } else if(missing(validation) && nfolds >= 2) {
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_SpeeDRF, source=data@key, destination_key=key, response=args$y, ignored_cols=args$x_ignore, ntrees=ntree, balance_classes = as.numeric(balance.classes), max_depth=depth, mtries = mtries, n_folds=nfolds, importance=as.numeric(importance),
                            sample_rate=sample.rate, nbins=nbins, seed=seed, select_stat_type=stat.type, oobee=as.numeric(oobee), sampling_strategy="RANDOM", verbose = as.numeric(verbose))
  } else if(!missing(validation) && nfolds == 0) {
    res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_SpeeDRF, source=data@key, destination_key=key, response=args$y, ignored_cols=args$x_ignore, balance_classes = as.numeric(balance.classes), ntrees=ntree, max_depth=depth, mtries = mtries, validation=validation@key, importance=as.numeric(importance),
                            sample_rate=sample.rate, nbins=nbins, seed=seed, select_stat_type = stat.type, oobee=as.numeric(oobee), sampling_strategy="RANDOM", verbose = as.numeric(verbose))
  } else stop("Cannot set both validation and nfolds at the same time")
  params = list(x=args$x, y=args$y, type="fast", ntree=ntree, depth=depth, mtries=mtries, sample.rate=sample.rate, nbins=nbins, stat.type = stat.type, balance_classes = as.numeric(balance.classes),
                sampling_strategy="RANDOM", seed=seed, oobee=oobee, nfolds=nfolds, importance=importance, verbose = as.numeric(verbose), h2o = data@h2o)
  
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
  mySum$nbins = res$nbins
  
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


  extra_json <- .fetchJSON(params$h2o, res$'_key')
  result$priorDistribution <- extra_json$speedrf_model$"_priorClassDist"
  result$modelDistribution <- extra_json$speedrf_model$"_modelClassDist"
  result$params$seed <- params$seed
  if (params$seed == -1)
    result$params$seed <- extra_json$speedrf_model$parameters$seed
  
  return(result)
}

# ------------------------------- Prediction ---------------------------------------- #

h2o.predict <- function(object, newdata, ...) {
  if( missing(object) ) stop('Must specify object')
  if(!inherits(object, "H2OModel")) stop("object must be an H2O model")
  if( missing(newdata) ) newdata <- object@data
  if(class(newdata) != "H2OParsedData") stop('newdata must be a H2O dataset')
  
  if(class(object) %in% c("H2OCoxPHModel", "H2OGBMModel", "H2OKMeansModel", "H2ODRFModel", "H2ONBModel",
                          "H2ODeepLearningModel", "H2OSpeeDRFModel")) {
    # Set randomized prediction key
    key_prefix = switch(class(object), "H2OCoxPHModel" = "CoxPHPredict", "H2OGBMModel" = "GBMPredict",
                        "H2OKMeansModel" = "KMeansPredict", "H2ODRFModel" = "DRFPredict",
                        "H2OGLMModel" = "GLM2Predict", "H2ONBModel" = "NBPredict",
                        "H2ODeepLearningModel" = "DeepLearningPredict", "H2OSpeeDRFModel" = "SpeeDRFPredict")
    rand_pred_key = .h2o.__uniqID(key_prefix)
    res = .h2o.__remoteSend(object@data@h2o, .h2o.__PAGE_PREDICT2, model=object@key, data=newdata@key, prediction=rand_pred_key)
#    res = .h2o.__remoteSend(object@data@h2o, .h2o.__PAGE_INSPECT2, src_key=rand_pred_key)
    .h2o.exec2(rand_pred_key, h2o = object@data@h2o, rand_pred_key)
  } else if(class(object) == "H2OPCAModel") {
    # Predict with user imposed number of principle components
    .args <- list(...)
    numPC = .args$num_pc
    # Set randomized prediction key
    rand_pred_key = .h2o.__uniqID("PCAPredict")
    # Find the number of columns in new data that match columns used to build pca model, detects expanded cols
    if(is.null(numPC)) numPC = 1
# Taken out so that default numPC = 1 instead of # of principle components resulting from analysis     
#    {
#      match_cols <- function(colname) length(grep(pattern = colname , object@model$params$x))
#      numMatch = sum(sapply(colnames(newdata), match_cols))
#      numPC = min(numMatch, object@model$num_pc)
#    }
    res = .h2o.__remoteSend(object@data@h2o, .h2o.__PAGE_PCASCORE, source=newdata@key, model=object@key, destination_key=rand_pred_key, num_pc=numPC)
    .h2o.__waitOnJob(object@data@h2o, res$job_key)
    .h2o.exec2(rand_pred_key, h2o = object@data@h2o, rand_pred_key)
  } else if(class(object) == "H2OGLMModel"){
 # Set randomized prediction key
    key_prefix = "GLM2Predict"
    rand_pred_key = .h2o.__uniqID(key_prefix)    
    res = .h2o.__remoteSend(object@data@h2o, .h2o.__PAGE_GLMPREDICT2, model=object@key, data=newdata@key, lambda=object@model$lambda,prediction=rand_pred_key)
    res = .h2o.__remoteSend(object@data@h2o, .h2o.__PAGE_INSPECT2, src_key=rand_pred_key)
    .h2o.exec2(rand_pred_key, h2o = object@data@h2o, rand_pred_key)
  } else
    stop(paste("Prediction has not yet been implemented for", class(object)))
}


h2o.makeGLMModel <- function(model, beta) {
  if( missing(model) || class(model) != "H2OGLMModel") 
    stop('Must specify source glm model')      
   res = .h2o.__remoteSend(model@data@h2o, .h2o.__GLMMakeModel, model=model@key, names = paste(names(beta),sep=","), beta = as.vector(beta))   
   .h2o.get.glm(model@data@h2o, as.character(res$destination_key), FALSE)
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

h2o.mse <- function(data, reference) {
  if(!class(data) %in% c("H2OParsedData", "H2OParsedDataVA")) stop("data must be an H2O parsed dataset")
  if(!class(reference) %in% c("H2OParsedData", "H2OParsedDataVA")) stop("reference must be an H2O parsed dataset")
  if(ncol(data) != 1) stop("Must specify exactly one column for data")
  if(ncol(reference) != 1) stop("Must specify exactly one column for reference")
  
  res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_CONFUSION, actual = reference@key, vactual = 0, predict = data@key, vpredict = 0)
  return(res$mse)
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

h2o.gapStatistic <- function(data, cols = "", K = 10, B = 10, boot_frac = 0.1, max_iter = 50, seed = 0) {
  args <- .verify_datacols(data, cols)
  ignored_cols <- if (args$cols_ignore == "") "" else (match(args$cols_ignore, colnames(data)) - 1)
  if(!is.numeric(B) || B < 1) stop("B must be an integer greater than 0")
  if(!is.numeric(K) || K < 2) stop("K.max must be an integer greater than 1")
  if(!is.numeric(boot_frac) || boot_frac < 0 || boot_frac > 1) stop("boot_frac must be a number between 0 and 1")
  if(!is.numeric(seed)) stop("seed must be numeric")

  res <- .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GAPSTAT, source = data@key, ignored_cols = ignored_cols, b_max = B, k_max = K, bootstrap_fraction = boot_frac, seed = seed)
  .h2o.__waitOnJob(data@h2o, res$job_key)
  res2 <- .h2o.__remoteSend(data@h2o, .h2o.__PAGE_GAPSTATVIEW, '_modelKey' = res$destination_key)

  result <- list()
  result$log_within_ss     <- res2$gap_model$wks
  result$boot_within_ss    <- res2$gap_model$wkbs
  result$se_boot_within_ss <- res2$gap_model$sk
  result$gap_stats <- res2$gap_model$gap_stats
  result$k_opt     <- res2$gap_model$k_best
  result$params    <- list()
  result$params$K  <- K
  result$params$B  <- B
  result$params$boot_frac <- boot_frac
  new("H2OGapStatModel", data=data, key="", model = result)
}

h2o.performance <- function(data, reference, measure = "accuracy", thresholds, gains = TRUE, ...) {
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
  
  if (is.list(res$aucdata$thresholds)) {
    res$aucdata$thresholds <- as.numeric(unlist(res$aucdata$thresholds))
  }
  
  meas = as.numeric(res$aucdata[[measure]])
  result = .h2o.__getPerfResults(res$aucdata, criterion)
  roc = .get_roc(res$aucdata$confusion_matrices)
  gains_table <- NULL
  if (gains) {
    l <- list(...)
    percents <- FALSE
    groups <- 10
    if ("percents" %in% names(l)) percents <- l$percents
    if ("groups" %in% names(l)) groups <- l$groups
    gains_table <- h2o.gains(actual = reference, predicted = data, percents = percents, groups = groups)
  }
  new("H2OPerfModel", cutoffs = res$aucdata$thresholds, measure = meas, perf = measure, model = result, roc = roc, gains = gains_table)
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
  result$error <- 1 - result$accuracy
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
  .h2o.exec2(res$destination_key, h2o = data@h2o, res$destination_key)
}

h2o.deepfeatures <- function(data, model, key = "", layer = -1) {
  if(missing(data)) stop("Must specify data")
  if(class(data) != "H2OParsedData") stop("data must be an H2O parsed dataset")
  if(missing(model)) stop("Must specify model")
  if(class(model) != "H2ODeepLearningModel") stop("model must be an H2O deep learning model")
  if(!is.character(key)) stop("key must be of class character")
  if(!is.numeric(layer)) stop("layer must be of class numeric")

  if (layer != -1) layer = layer - 1; #index translation (R index is from 1..N, Java expects 0..N-1)
  res = .h2o.__remoteSend(data@h2o, .h2o.__PAGE_DEEPFEATURES, source = data@key, dl_model = model@key, destination_key = key, layer = layer)
  .h2o.__waitOnJob(data@h2o, res$job_key)
  .h2o.exec2(res$destination_key, h2o = data@h2o, res$destination_key)
}

# ------------------------------- Helper Functions ---------------------------------------- #
# Used to verify data, x, y and turn into the appropriate things
.verify_dataxy <- function(data, x, y) {
   .verify_dataxy_full(data, x, y, FALSE)
}
.verify_dataxy_full <- function(data, x, y, autoencoder) {
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

  if (!missing(autoencoder) && !autoencoder) if( y %in% x ) {
    # stop(paste(y, 'is both an explanatory and dependent variable'))
    warning("Response variable in explanatory variables")
    x <- setdiff(x,y)
  }

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
  if (algo == "DeepLearning" && !is.null(modelOrig$validationKey)) validation@key = modelOrig$validationKey

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

  x <- pred_errs_orig <- unlist(lapply(seq_along(myModelSum),  function(x) myModelSum[[x]]$prediction_error))
  y <- pred_errs <- sort(pred_errs_orig)
  result <- result[order(match(x,y))]
  myModelSum <- myModelSum[order(match(x,y))]

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
    my_params <- list(params$ntree, params$depth, params$sample.rate, params$nbins)
  
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
  cf_matrix = cbind(cf_matrix, round(cf_error, 5))
  
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
