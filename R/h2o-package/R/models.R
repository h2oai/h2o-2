    #'
    #' Retrieve Model Data
    #'
    #' After a model is constructed by H2O, R must create a view of the model. All views are backed by S4 objects that
    #' subclass the H2OModel object (see classes.R for class specifications).
    #'
    #' This file contains the set of model getters that fill out and return the appropriate S4 object.
    #'
    #'
    #' Maintenance strategy:
    #'
    #'   The getter code attempts to be as modular and concisse as possible. The overall strategy is to create, for each
    #'   model type, a list of parameters to be filled in by the retrieved json. There is a mapping from the json names
    #'   to the names used in the model (e.g. see .glm.json.map). This is used to perform succinct model data filling.
    #-----------------------------------------------------------------------------------------------------------------------


    #'
    #' Fetch the JSON for a given model key.
    #'
    #' Grabs all of the JSON and returns it as a named list. Do this by using the 2/Inspector.json page, which provides
    #' a redirect URL to the appropriate Model View page.
    .fetch.JSON <- function(h2o, key) {
      redirect_url <- .h2o.__remoteSend(h2o, .h2o.__PAGE_INSPECTOR, src_key = key)$response_info$redirect_url
      page <- strsplit(redirect_url, '\\?')[[1]][1]                         # returns a list of two items
      page <- paste(strsplit(page, '')[[1]][-1], sep = "", collapse = "")   # strip off the leading '/'
      key  <- strsplit(strsplit(redirect_url, '\\?')[[1]][2], '=')[[1]][2]  # split the second item into a list of two items
      if (grepl("GLMGrid", page)) .h2o.__remoteSend(client = h2o, page = page, grid_key = key) #glm grid page
      else .h2o.__remoteSend(client = h2o, page = page, '_modelKey' = key)
    }


    #'
    #' Fetch the parameters of the model.
    #'
    #' A helper function to retrieve the parameters of the model from the model view page.
    .get.model.params <-
    function(h2o, key) {
      json <- .fetchJSON(h2o, key)
      algo <- model.type <- names(json)[3]
      if (algo == "grid") return("") # no parameters if algo.type is "grid" --> GLMGRrid result
      params <- json[[model.type]]$parameters
      params$h2o <- h2o
      params
    }

    #'
    #' Helper to recursively replace leading '_' in list names.
    .repl<- function(l) { names(l) <- unlist(lapply(names(l), function(x) gsub("^_*", "", x))); l }

    #'
    #' Helper to filter out non-null results
    .filt <- function(l) names(Filter(is.null, l))

    #'
    #' The generic fill function
    .fill<-
    function(from, to) {
      print(from)
      print(.filt(to))
      print(names(from[.filt(to)]))
      stop("WAS to RIGHT?")
      to[.filt(to)] <- from[.filt(to)]
      to
    }

    #'
    #' Helper to recursively map json names to model names
    .names.mapper<-
    function(l, model.names) {
      a <- match(names(model.names), names(l))
      a <- a[!is.na(a)]
      names(l)[a] <- unlist(lapply(names(l)[a], function(n) model.names[[n]]))
      l
    }

    #'
    #' Helper to recursively operate on a list.
    .rlapply<-
    function(l, func, ...) {
      if (is.list(l)) {
        l <- func(l, ...)
        l <- lapply(l, .rlapply, func, ...)
      }
      l
    }

    #'
    #' Helper to recursively fill from a list.
    .rfillapply<-
    function(l, res, func, ...) {
      if (is.list(l)) {
        res <- func(l, res, ...)
        res <- lapply(l, .rfillapply, res, func, ...)
      }
      res
    }

    #'
    #' Preamble method for every model.
    #'
    #' Fetches all of the json and the parameters.
    .h2o.__model.preamble<-
    function(h2o, key, model.names = "") {
      params <- .get.model.params(h2o, key)
      json   <- .fetch.JSON(h2o, key)
      res <- list(json = json, params = params)
      res <- .rlapply(res, .repl)
      if ( length(model.names) > 1) res <- .rlapply(res, .names.mapper, model.names)
      res
    }

    #-----------------------------------------------------------------------------------------------------------------------
    #
    #       GLM Model Getter
    #
    #-----------------------------------------------------------------------------------------------------------------------

    #'
    #' Field names for GLM results
    .glm.result.fields <- c("coefficients","normalized_coefficients","rank","iter","lambda","deviance",
                            "null.deviance","df.residual","df.null","aic","train.err")
    .glm.binomial.result <- c(.glm.result.fields, "prior", "threshold", "best_threshold", "auc", "confusion")
    .glm.params.fields <- c("alpha","lambda","family","prior")

    .glm.json.map <- list(iteration = "iter", beta = "coefficients", norm_beta = "normalized_coefficients",
                          lambda_value = "lambda", null_deviance = "null.deviance", residual_deviance = "deviance",
                          avg_err = "train.err")

    #'
    #' Is the GLM family binomial?
    .isBinomial <- function(pre) pre$params$family == "binomial"

    #'
    #' Fill in a single GLM Result
    .h2o.__getGLMResults<-
    function(h2o, key, lambda_idx = 1) {
      pre    <- .h2o.__model.preamble(h2o, key, .glm.json.map)
      params <- pre$params
      submod <- pre$json$glm_model$submodels[[lambda_idx]]
      valid  <- if(is.null(submod$xvalidation)) submod$validation else submod$xvalidation

      # create an empty list of results that will be filled in below
      result <- sapply(if(.isBinomial(pre)) .glm.binomial.result else .glm.result.fields, function(x) {})

      # fill in all results
    #  result <- .rfillapply(pre, result, .fill)                  # TODO
      result[names(result)] <- submod[names(result)]              # fill in the results from submod
      result[.filt(result)] <- pre$json[.filt(result)]            # fill in the result from the json
      result[.filt(result)] <- pre$json$glm_model[.filt(result)]  # fill in the result from the model json
      result[.filt(result)] <- valid[.filt(result)]               # fill in the result from the validation
      result$df.residual    <- max(valid$nobs-result$rank,0)
      result$df.null        <- valid$nobs-1

      params$lambda_all <- sapply(pre$json$glm_model$submodels, function(x) { x$lambda_value })
      params$lambda_best <- params$lambda_all[[pre$json$glm_model$best_lambda_idx+1]]
      if (params$family == "tweedie")
        params$family <- .h2o.__getFamily(params$family, params$link, params$tweedie_variance_power, params$tweedie_link_power)
      else
        params$family <- .h2o.__getFamily(params$family, params$link)

      idxes <- submod$idxs + 1
      names(result$coefficients) <- pre$json$glm_model$coefficients_names[idxes]
      if(pre$params$standardize == "true" && !is.null(submod$norm_beta)) {
          names(result$normalized_coefficients) = names(result$coefficients)
      }
      result$params <- pre$params

      if(params$family$family == "binomial") {
        cm_ind <- trunc(100*result$best_threshold) + 1
        result$confusion <- .build_cm(valid$cms[[cm_ind]]$arr, c("false", "true"))
      }
      result
    }




    #-----------------------------------------------------------------------------------------------------------------------
    #
    #       GBM Model Getter
    #
    #-----------------------------------------------------------------------------------------------------------------------
