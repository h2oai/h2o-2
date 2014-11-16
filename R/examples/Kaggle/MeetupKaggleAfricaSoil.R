## This code block is to re-install a particular version of H2O
# START
if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }
#install.packages("h2o", repos=(c("file:///Users/arno/h2o/target/R", getOption("repos"))))
install.packages("h2o", repos=(c("http://s3.amazonaws.com/h2o-release/h2o/master/1545/R", getOption("repos")))) #choose a build here
# END

library(h2o)
library(stringr)

## Connect to H2O server (On server(s), run 'java -jar h2o.jar -Xmx4G -port 43322 -name AfricaSoil' first)
#h2oServer <- h2o.init(ip="mr-0xd1", port = 43322)

## Launch H2O directly on localhost
h2oServer <- h2o.init(nthreads = -1) # allow the use of all cores - requires reproducible = F below though.

## Import data
path_train <- "/Users/arno/kaggle_africasoil/data/training.csv.gz"
path_test <- "/Users/arno/kaggle_africasoil/data/sorted_test.csv.gz"
train_hex <- h2o.importFile(h2oServer, path = path_train)
test_hex <- h2o.importFile(h2oServer, path = path_test)

## Group variables
vars <- colnames(train_hex)
spectra <- vars[seq(2,3579,by=20)] # "poor man's dimensionality reduction": take every N-th column of spectral data
extra <- vars[3580:3595]
targets <- vars[3596:3600]
predictors <- c(spectra, extra)

## Settings
ensemble_size <- 2
n_fold = 3
reproducible_mode = F # set to TRUE if you want reproducible results, e.g. for final Kaggle submission if you think you'll win :)  Note: will be slower
seed0 = 1337 # Only really matters for reproducible_mode = T

## Scoring helpers
MSEs <- matrix(0, nrow = 1, ncol = length(targets))
RMSEs <- matrix(0, nrow = 1, ncol = length(targets))
CMRMSE = 0

## Main loop over regression targets
for (resp in 1:length(targets)) {
  cat("\n\nNow training and cross-validating a DL model for", targets[resp], "...\n")
  
  ## Run grid search with n-fold cross-validation
  cvmodel <-
    h2o.deeplearning(x = predictors,
                     y = targets[resp],
                     data = train_hex,
                     nfolds = n_fold,
                     classification = F,
                     activation="RectifierWithDropout",
                     hidden = c(100,100),
                     hidden_dropout_ratios = c(0.0,0.0),
                     input_dropout_ratio = 0,
                     epochs = 100,
                     l1 = c(0,1e-5),
                     l2 = c(0,1e-5), 
                     rho = 0.99, 
                     epsilon = 1e-8, 
                     train_samples_per_iteration = -2,
                     reproducible = reproducible_mode,
                     seed = seed0 + resp
    )

  ## Collect cross-validation error

  cvmodel <- cvmodel@model[[1]] #Only if cvmodel is a grid search model

  MSE <- cvmodel@model$valid_sqr_error
  RMSE <- sqrt(MSE)
  CMRMSE <- CMRMSE + RMSE #column-mean-RMSE
  MSEs[resp] <- MSE
  RMSEs[resp] <- RMSE
  cat("\nCross-validated MSEs so far:", MSEs)
  cat("\nCross-validated RMSEs so far:", RMSEs)
  cat("\nCross-validated CMRMSE so far:", CMRMSE/resp)
    
  cat("\n\nTaking parameters from grid search winner for", targets[resp], "...\n")

  p <- cvmodel@model$params

  ## Build an ensemble model on full training data - should perform better than the CV model above
  for (n in 1:ensemble_size) {
    cat("\n\nBuilding ensemble model", n, "of", ensemble_size, "for", targets[resp], "...\n")
    model <-
      h2o.deeplearning(x = predictors,
                       y = targets[resp],
                       key = paste0(targets[resp], "_cv_ensemble_", n, "_of_", ensemble_size),
                       data = train_hex, 
                       classification = F,
                       activation = p$activation,
                       hidden = p$hidden,
                       hidden_dropout_ratios = p$hidden_dropout_ratios,
                       input_dropout_ratio = p$input_dropout_ratio,
                       epochs = p$epochs,
                       l1 = p$l1,
                       l2 = p$l2,
                       rho = p$rho,
                       epsilon = p$epsilon,
                       train_samples_per_iteration = p$train_samples_per_iteration,
                       reproducible = p$reproducible,
                       seed = p$seed + n
                       )
    
    ## Aggregate ensemble model predictions
    test_preds <- h2o.predict(model, test_hex)
    if (n == 1) {
      test_preds_blend <- test_preds
    } else {
      test_preds_blend <- cbind(test_preds_blend, test_preds[,1])
    }
  }
  
  ## Now create submission
  cat (paste0("\n Number of ensemble models: ", ncol(test_preds_blend)))
  ensemble_average <- matrix("ensemble_average", nrow = nrow(test_preds_blend), ncol = 1)
  ensemble_average <- rowMeans(as.data.frame(test_preds_blend)) # Simple ensemble average, consider blending/stacking
  ensemble_average <- as.data.frame(ensemble_average)
  
  colnames(ensemble_average)[1] <- targets[resp]
  if (resp == 1) {
    final_submission <- cbind(as.data.frame(test_hex[,1]), ensemble_average)
  } else {
    final_submission <- cbind(final_submission, ensemble_average)
  }
  print(head(final_submission))
  
  ## Remove no longer needed old models and temporaries from K-V store to keep memory footprint low
  ls_temp <- h2o.ls(h2oServer)
  for (n_ls in 1:nrow(ls_temp)) {
    if (str_detect(ls_temp[n_ls, 1], "DeepLearning")) {
      h2o.rm(h2oServer, keys = as.character(ls_temp[n_ls, 1]))
    } else if (str_detect(ls_temp[n_ls, 1], "Last.value")) {
      h2o.rm(h2oServer, keys = as.character(ls_temp[n_ls, 1]))
    }
  }
}
cat(paste0("\nOverall cross-validated CMRMSE = " , CMRMSE/length(targets)))

## Write final submission to CSV
write.csv(final_submission, file = "./submission.csv", quote = F, row.names=F)
