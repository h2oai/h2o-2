## This code block is to re-install a particular version of H2O
# START
#if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
#if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }
#install.packages("h2o", repos=(c("file:///Users/arno/h2o/target/R", getOption("repos"))))
#install.packages("h2o", repos=(c("http://s3.amazonaws.com/h2o-release/h2o/master/1545/R", getOption("repos")))) #choose a build here
# END
library(h2o)
library(stringr)

## Connect to H2O server (On server(s), run 'java -jar h2o.jar -Xmx4G -port 43322 -name AfricaSoil' first)
h2oServer <- h2o.init(ip="mr-0xd1", port = 53322)

## Launch H2O directly on localhost
#h2oServer <- h2o.init(nthreads = -1, max_mem_size = '128g') # allow the use of all cores - requires reproducible = F below though.

## Import data
path_train <- "/home/arno/kaggle_tradeshift/data/train.csv"
path_trainLabels <- "/home/arno/kaggle_tradeshift/data/trainLabels.csv"
path_test <- "/home/arno/kaggle_tradeshift/data/test.csv"
train_hex <- h2o.importFile(h2oServer, path = path_train)
trainLabels_hex <- h2o.importFile(h2oServer, path = path_trainLabels)
test_hex <- h2o.importFile(h2oServer, path = path_test)

## Group variables
vars <- colnames(train_hex)
ID <- vars[1]
labels <- colnames(trainLabels_hex)
predictors <- vars[c(-1,-4,-35,-62,-65,-92,-95)]
predictors
targets <- labels[-1]
#targets <- labels[c(2,3)]
targets

## Settings
ensemble_size <- 2
n_fold = 20
reproducible_mode = F # set to TRUE if you want reproducible results, e.g. for final Kaggle submission if you think you'll win :)  Note: will be slower
seed0 = 1337 # Only really matters for reproducible_mode = T
submit = T


## Scoring helpers
MSEs <- matrix(0, nrow = 1, ncol = length(targets))
LogLoss <- matrix(0, nrow = 1, ncol = length(targets))

errs = 0
cv_preds <- matrix(0, nrow = nrow(train_hex), ncol = 1)
holdout_valid_se <- matrix(0, nrow = 1, ncol = length(targets))
holdout_valid_mse <- matrix(0, nrow = 1, ncol = length(targets))
holdout_valid_logloss <- matrix(0, nrow = 1, ncol = length(targets))



## Main loop over regression targets
for (resp in 1:length(targets)) {
  trainWL <- h2o.exec(h2oServer,expr=cbind(train_hex, trainLabels_hex))
  splits <- h2o.splitFrame(trainWL, ratios = 1-1/n_fold, shuffle=T)
  train <- splits[[1]]
  valid <- splits[[2]]
  
  cat("\n\nNow training and cross-validating a DL model for", targets[resp], "...\n")
  
  train_resp <- train[,targets[resp]]
  valid_resp <- valid[,targets[resp]]
  
  # build final model blend with validated parameters, do early stopping based on validation error
  cvmodel <-
    h2o.deeplearning(x = predictors,
                     y = targets[resp],
                     data = train,
                     validation = valid,
                     classification = F,
                     activation="Tanh",
                     hidden = c(1),
                     epochs = 1,
                     l1 = c(0),
                     l2 = c(0), 
                     rho = 0.99, 
                     epsilon = 1e-8,
                     score_duty_cycle = 0.1,
                     score_interval = 1,
                     train_samples_per_iteration = 10000,
                     reproducible = reproducible_mode,
                     seed = seed0 + resp,
                     max_categorical_features = 1000000
    )
  
  #model <- cvmodel@model[[1]] #If cv model is a grid search model
  model <- cvmodel #If cvmodel is not a grid search model
  
  ## Use the model and store results
  #train_preds <- h2o.predict(model, train)
  valid_preds <- h2o.predict(model, valid)
  valid_preds <- ifelse(valid_preds > 1e-15, valid_preds, 1e-15)
  valid_preds <- ifelse(valid_preds < 1-1e-15, valid_preds, 1-1e-15)
  
  test_preds <- h2o.predict(model, test_hex)
  test_preds <- ifelse(test_preds > 1e-15, test_preds, 1e-15)
  test_preds <- ifelse(test_preds < 1-1e-15, test_preds, 1-1e-15)
  
  #print(head(test_preds))

  ## Compute MSE
  #msetrain <- h2o.exec(h2oServer,expr=mean((train_preds - train_resp)^2))
  sevalid <- h2o.exec(h2oServer,expr=(valid_preds - valid_resp)^2)
  msevalid <- h2o.exec(h2oServer,expr=mean(sevalid))
  holdout_valid_se[resp] <- holdout_valid_se[resp] + h2o.exec(h2oServer,expr=sum(sevalid))
  
  ## Compute LogLoss
  LL <- mean(-valid_resp*(log(valid_preds)-(1-valid_resp)*log(1-valid_preds)))
  holdout_valid_logloss[resp] <- holdout_valid_logloss[resp] + LL
  
  
  MSE <- msevalid
  MSEs[resp] <- MSE
  LogLoss[resp] <- LL
  cat("\nCross-validated MSEs so far:", MSEs)
  cat("\nCross-validated LogLoss so far:", LogLoss)
  
  if (submit) {
    cat("\n\nTaking parameters from grid search winner for", targets[resp], "...\n")
    #p <- cvmodel@sumtable[[1]]  #If cvmodel is a grid search model
    p <- cvmodel@model$params   #If cvmodel is not a grid search model
    
    ## Build an ensemble model on full training data - should perform better than the CV model above
    for (n in 1:ensemble_size) {
      cat("\n\nBuilding ensemble model", n, "of", ensemble_size, "for", targets[resp], "...\n")
      model <-
        h2o.deeplearning(x = predictors,
                         y = targets[resp],
                         key = paste0(targets[resp], "_cv_ensemble_", n, "_of_", ensemble_size),
                         data = trainWL, 
                         classification = F,
                         activation = p$activation,
                         hidden = p$hidden,
                         epochs = p$epochs,
                         l1 = p$l1,
                         l2 = p$l2,
                         rho = p$rho,
                         epsilon = p$epsilon,
                         train_samples_per_iteration = p$train_samples_per_iteration,
                         reproducible = p$reproducible,
                         seed = p$seed + n,
                         max_categorical_features = p$max_categorical_features
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
    
  }

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
cat("\nOverall cross-validated MSEs = " , MSEs)
cat("\nOverall cross-validated MSE = " , mean(MSEs))
cat("\nOverall cross-validated LogLosses = " , LogLoss)
cat("\nOverall cross-validated LogLoss = " , mean(LogLoss))

if (submit) {
  # print(head(final_submission))
  # f <- matrix(final_submission[,-1], nrow=(ncol(final_submission)-1)*nrow(final_submission), byrow = F)
  # head(f)
  
  sink("./submission.csv")
  print("id_label,pred")
  
  ## Reformat to Kaggle style
  for (row in 1:nrow(final_submission)) {
    for (resp in 1:length(targets)) {
      if (resp == 14) {
        print(paste0(final_submission[row,1],"_",targets[resp],",",0.0))
      } else {
        print(paste0(final_submission[row,1],"_",targets[resp],",",final_submission[row,1+resp]))
      }
    }
  }
  
  sink()
}
