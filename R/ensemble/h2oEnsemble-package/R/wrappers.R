# Set of default wrappers to create a uniform interface for h2o supervised ML functions
# Wrappers for: h2o.glm, h2o.randomForest, h2o.gbm, h2o.deeplearning

# We want the function wrappers to all have the same arguments, 
# like "x, y, data, family", so that the ensemble code can be written more cleanly.


h2o.glm.wrapper <- function(x, y, data, key = "", family = "binomial", link, nfolds = 0, alpha = 0.5, nlambda = -1, 
                                    lambda.min.ratio = -1, lambda = 1e-5, epsilon = 1e-4, standardize = TRUE, 
                                    prior, variable_importances = FALSE, use_all_factor_levels = FALSE, 
                                    tweedie.p = ifelse(family == 'tweedie', 1.5, as.numeric(NA)), iter.max = 100, 
                                    higher_accuracy = FALSE, lambda_search = FALSE, return_all_lambda = FALSE, 
                                    max_predictors = -1, ...) {
  
  h2o.glm(x = x, y = y, data = data, key = key, family = family, link = link, nfolds = nfolds, 
          alpha = alpha, nlambda = nlambda, lambda.min.ratio = lambda.min.ratio, lambda = lambda, 
          epsilon = epsilon, standardize = standardize, prior = prior, 
          variable_importances = variable_importances, use_all_factor_levels = use_all_factor_levels, 
          tweedie.p = tweedie.p, iter.max = iter.max, higher_accuracy = higher_accuracy, 
          lambda_search = lambda_search, return_all_lambda = return_all_lambda, 
          max_predictors = max_predictors)
}


h2o.gbm.wrapper <- function(x, y, data, key = "", family = "binomial", n.trees = 10, interaction.depth = 5, 
                     n.minobsinnode = 10, shrinkage = 0.1, n.bins = 20, importance = FALSE, nfolds = 0, validation, 
                     balance.classes = FALSE, max.after.balance.size = 5, ...) {
  
  h2o.gbm(x = x, y = y, data = data, key = key, distribution = ifelse(family=="binomial", "multinomial", "gaussian"), 
          n.trees = n.trees, interaction.depth = interaction.depth, 
          n.minobsinnode = n.minobsinnode, shrinkage = shrinkage, n.bins = n.bins, 
          nfolds = nfolds, importance = importance, validation = validation, 
          balance.classes = balance.classes, max.after.balance.size = max.after.balance.size) 
}


h2o.randomForest.wrapper <- function(x, y, data, key = "", family = "binomial", ntree = 50, 
                                              depth = 20, mtries = -1, sample.rate = 2/3, nbins = 20, seed = -1, 
                                              importance = FALSE, nfolds = 0, validation, nodesize = 1, 
                                              balance.classes = FALSE, max.after.balance.size = 5, doGrpSplit = TRUE,
                                              verbose = FALSE, oobee = TRUE, stat.type = "ENTROPY", type = "fast", ...){
  
  h2o.randomForest(x = x, y = y, data = data, key = key, classification = ifelse(family=="binomial", TRUE, FALSE), 
                   ntree = ntree, depth = depth, sample.rate = sample.rate, nbins = nbins, seed = seed, 
                   importance = importance, nfolds = nfolds, validation = validation, nodesize = nodesize, 
                   balance.classes = balance.classes, max.after.balance.size = max.after.balance.size,
                   doGrpSplit = doGrpSplit, verbose = verbose, oobee = oobee, stat.type = stat.type, 
                   type = ifelse(family=="binomial", type, "BigData"))
}


h2o.deeplearning.wrapper <- function(x, y, data, key = "", family = "binomial", override_with_best_model,
                                nfolds = 0, validation, checkpoint = "", autoencoder, use_all_factor_levels, 
                                activation, hidden, epochs, train_samples_per_iteration, seed, adaptive_rate,
                                rho, epsilon, rate, rate_annealing, rate_decay, momentum_start,
                                momentum_ramp, momentum_stable, nesterov_accelerated_gradient,
                                input_dropout_ratio, hidden_dropout_ratios, l1, l2, max_w2,
                                initial_weight_distribution, initial_weight_scale, loss,
                                score_interval, score_training_samples, score_validation_samples,
                                score_duty_cycle, classification_stop, regression_stop, quiet_mode,
                                max_confusion_matrix_size, max_hit_ratio_k, balance_classes,
                                max_after_balance_size, score_validation_sampling, diagnostics,
                                variable_importances, fast_mode, ignore_const_cols, force_load_balance,
                                replicate_training_data, single_node_mode, shuffle_training_data,
                                sparse, col_major, max_categorical_features, reproducible, ...) {
  
  h2o.deeplearning(x = x, y = y, data = data, key = key, override_with_best_model = override_with_best_model,
                   classification = ifelse(family=="binomial", TRUE, FALSE), nfolds = nfolds,
                   validation = validation, checkpoint = checkpoint, autoencoder = autoencoder, 
                   use_all_factor_levels = use_all_factor_levels,
                   activation = activation, hidden = hidden, epochs = epochs, 
                   train_samples_per_iteration = train_samples_per_iteration, 
                   seed = seed, adaptive_rate = adaptive_rate, rho = rho, epsilon = epsilon, 
                   rate = rate, rate_annealing = rate_annealing, rate_decay = rate_decay, 
                   momentum_start = momentum_start, momentum_ramp = momentum_ramp, 
                   momentum_stable = momentum_stable, nesterov_accelerated_gradient = nesterov_accelerated_gradient,
                   input_dropout_ratio = input_dropout_ratio, hidden_dropout_ratios = hidden_dropout_ratios, 
                   l1 = l1, l2 = l2, max_w2 = max_w2, initial_weight_distribution = initial_weight_distribution, 
                   initial_weight_scale = initial_weight_scale, loss = loss,
                   score_interval = score_interval, score_training_samples = score_training_samples, 
                   score_validation_samples = score_validation_samples,
                   score_duty_cycle = score_duty_cycle, classification_stop = classification_stop, 
                   regression_stop = regression_stop, quiet_mode = quiet_mode,
                   max_confusion_matrix_size = max_confusion_matrix_size, max_hit_ratio_k = max_hit_ratio_k, 
                   balance_classes = balance_classes, max_after_balance_size = max_after_balance_size, 
                   score_validation_sampling = score_validation_sampling, diagnostics = diagnostics,
                   variable_importances = variable_importances, fast_mode = fast_mode, 
                   ignore_const_cols = ignore_const_cols, force_load_balance = force_load_balance,
                   replicate_training_data = replicate_training_data, single_node_mode = single_node_mode, 
                   shuffle_training_data = shuffle_training_data, sparse = sparse, col_major = col_major,
                   max_categorical_features = max_categorical_features, reproducible = reproducible)  
}

