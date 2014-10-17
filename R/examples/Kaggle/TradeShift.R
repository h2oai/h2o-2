sink("TradeShift.log", split = T)

## This code block is to re-install a particular version of H2O
# START
#if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
#if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }
#install.packages("h2o", repos=(c("http://s3.amazonaws.com/h2o-release/h2o/master/1548/R", getOption("repos")))) #choose a build here
# END

# Fetch the latest nightly build using Jo-fai Chow's package
#devtools::install_github("woobe/deepr")
#deepr::install_h2o()

library(h2o)
library(stringr)

## Connect to H2O server (On server(s), run 'java -jar h2o.jar -Xmx8G -port 53322 -name TradeShift' first)
h2oServer <- h2o.init(ip="mr-0xd1", port = 53322)

## Launch H2O directly on localhost
#h2oServer <- h2o.init(nthreads = -1, max_mem_size = '8g') # allow the use of all cores - requires reproducible = F below though.

## Import data
path_train <- "/home/arno/kaggle_tradeshift/data/train.csv"
path_trainLabels <- "/home/arno/kaggle_tradeshift/data/trainLabels.csv"
path_test <- "/home/arno/kaggle_tradeshift/data/test.csv"
path_submission <- "/home/arno/kaggle_tradeshift/data/sampleSubmission.csv"

train_hex <- h2o.importFile(h2oServer, path = path_train)
trainLabels_hex <- h2o.importFile(h2oServer, path = path_trainLabels)
test_hex <- h2o.importFile(h2oServer, path = path_test)

## Group variables
vars <- colnames(train_hex)
ID <- vars[1]
labels <- colnames(trainLabels_hex)
predictors <- vars[c(-1,-4,-35,-62,-65,-92,-95)] #remove ID and variables with too many factor levels
targets <- labels[-1] ## all targets
#targets <- labels[c(7,8,10,11,13,29,30,31,32,33)]  ## harder to predict targets for tuning of parameters

## Settings
validate = T #whether to compute CV error on train/validation split (or n-fold), potentially with grid search - not needed for submission
submit = T #whether to make ensemble prediction for submission

# These settings lead to a cross-validated log-loss of 0.008743972 with h2o.randomForest(ntree=20,depth=40,type="fast")
ensemble_size <- 5
seed0 = 1337

reproducible_mode = F # For DL only. Set to TRUE if you want reproducible results, e.g. for final Kaggle submission if you think you'll win :)  Note: will be slower

## Scoring helpers
MSEs <- matrix(0, nrow = 1, ncol = length(targets))
LogLoss <- matrix(0, nrow = 1, ncol = length(targets))

## Main loop over targets
for (resp in 1:length(targets)) {
  # always just predict class 0 for y_14 (is constant)
  if (resp == 14) {
    if (submit) {
      final_submission <- cbind(final_submission, as.data.frame(matrix(0, nrow = nrow(test_hex), ncol = 1)))
      colnames(final_submission)[resp] <- targets[resp]
    }
    next
  }
  
  # No need to do n-fold just yet - simple 95%/5% split is fine (enough data) and much faster
  trainWL <- h2o.exec(h2oServer,expr=cbind(train_hex, trainLabels_hex))
  
  if (validate) {
    splits <- h2o.splitFrame(trainWL, ratios = c(0.95), shuffle=F)
    train <- splits[[1]]
    valid <- splits[[2]]
    
    cat("\n\nNow training and cross-validating an H2O model for", targets[resp], "...\n")
    
    train_resp <- train[,targets[resp]]
    valid_resp <- valid[,targets[resp]]
    
    
    for (n in 1:ensemble_size) {
      cat("\n\nBuilding ensemble validation model", n, "of", ensemble_size, "for", targets[resp], "...\n")

      #   cvmodel <-
      #     h2o.deeplearning(x = predictors,
      #                      y = targets[resp],
      #                      data = train,
      #                      validation = valid,
      #                      classification = T,
      #                      activation="Tanh",
      #                      hidden = c(10,50,50),
      #                      epochs = 1,
      #                      l1 = c(0),
      #                      l2 = c(0),
      #                      rho = 0.99,
      #                      epsilon = 1e-8,
      #                      score_duty_cycle = 0.1,
      #                      score_interval = 1,
      #                      train_samples_per_iteration = 10000,
      #                      reproducible = reproducible_mode,
      #                      seed = seed0 + resp*ensemble_size + n,
      #                      max_categorical_features = 10000
      #     )

      cvmodel <-
        h2o.randomForest(x = predictors,
                         y = targets[resp],
                         data = train,
                         classification = T,
                         type = "fast",
                         ntree = 20,
                         depth = c(40),
                         seed = seed0 + resp*ensemble_size + n,
                         validation = valid
        )
      
      #model <- cvmodel@model[[1]] #If cv model is a grid search model
      model <- cvmodel #If cvmodel is not a grid search model
      
      # use probabilities - clamp validation predictions for LogLoss computation
      valid_preds <- h2o.predict(model, valid)[,3]
      
      # compute LogLoss for this ensemble member, on validation data, as a guidance to see the variance etc.
      vpc <- valid_preds
      vpc <- h2o.exec(h2oServer,expr=ifelse(vpc > 1e-15, vpc, 1e-15))
      vpc <- h2o.exec(h2oServer,expr=ifelse(vpc < 1-1e-15, vpc, 1-1e-15))
      myLL <- h2o.exec(h2oServer,expr=mean(-valid_resp*(log(vpc)-(1-valid_resp)*log(1-vpc))))
      cat("\nLogLoss of this ensemble member:", myLL)
      
      if (n == 1) {
        valid_preds_blend <- valid_preds
      } else {
        valid_preds_blend <- valid_preds_blend+valid_preds
      }
    }
    valid_preds <- valid_preds_blend/ensemble_size ##ensemble average of probabilities
    
    # clamp predictions for LogLoss computation
    valid_preds <- h2o.exec(h2oServer,expr=ifelse(valid_preds > 1e-15, valid_preds, 1e-15))
    valid_preds <- h2o.exec(h2oServer,expr=ifelse(valid_preds < 1-1e-15, valid_preds, 1-1e-15))
    
    test_preds <- h2o.predict(model, test_hex)[,3]
    
    ## Compute MSE
    #msetrain <- h2o.exec(h2oServer,expr=mean((train_preds - train_resp)^2))
    sevalid <- h2o.exec(h2oServer,expr=(valid_preds - valid_resp)^2)
    msevalid <- h2o.exec(h2oServer,expr=mean(sevalid))
    MSE <- msevalid
    MSEs[resp] <- MSE
    cat("\n\nCross-validated MSEs so far:", MSEs)
    cat("\nMean cross-validated MSE so far:", sum(MSEs)/resp)
    
    ## Compute LogLoss
    LL <- h2o.exec(h2oServer,expr=mean(-valid_resp*(log(valid_preds)-(1-valid_resp)*log(1-valid_preds))))
    LogLoss[resp] <- LL
    cat("\nCross-validated LogLosses so far:", LogLoss)
    cat("\nMean cross-validated LogLoss so far:", sum(LogLoss)/resp)
  }
  
  if (submit) {
    if (validate) {
      cat("\n\nTaking parameters from validation run (or grid search winner) for", targets[resp], "...\n")
      #p <- cvmodel@sumtable[[1]]  #If cvmodel is a grid search model
      p <- cvmodel@model$params   #If cvmodel is not a grid search model
    }
    else {
      p = list(ntree=50, depth=40) #For randomForest
    }
    ## Build an ensemble model on full training data - should perform better than the CV model above
    for (n in 1:ensemble_size) {
      cat("\n\nBuilding ensemble model", n, "of", ensemble_size, "for", targets[resp], "...\n")
      #       model <-
      #         h2o.deeplearning(x = predictors,
      #                          y = targets[resp],
      #                          key = paste0(targets[resp], "_cv_ensemble_", n, "_of_", ensemble_size),
      #                          data = trainWL,
      #                          classification = p$classification,
      #                          activation = p$activation,
      #                          hidden = p$hidden,
      #                          epochs = p$epochs,
      #                          l1 = p$l1,
      #                          l2 = p$l2,
      #                          rho = p$rho,
      #                          epsilon = p$epsilon,
      #                          train_samples_per_iteration = p$train_samples_per_iteration,
      #                          reproducible = p$reproducible,
      #                          seed = seed0 + resp*ensemble_size + n,
      #                          max_categorical_features = p$max_categorical_features
      #         )

      model <-
        h2o.randomForest(x = predictors,
                         y = targets[resp],
                         data = trainWL,
                         classification = T,
                         type = "fast",
                         ntree = p$ntree,
                         depth = p$depth,
                         seed = seed0 + resp*ensemble_size + n,
                         key = paste0(targets[resp], "_cv_ensemble_", n, "_of_", ensemble_size)
        )
      
      ## Aggregate ensemble model predictions (probabilities)
      test_preds <- h2o.predict(model, test_hex)[,3]
      
      if (n == 1) {
        test_preds_blend <- test_preds
      } else {
        test_preds_blend <- cbind(test_preds_blend, test_preds[,1])
      }
    }
    
    ## Now create submission
    cat("\n Number of ensemble models: ", ncol(test_preds_blend))
    ensemble_average <- matrix("ensemble_average", nrow = nrow(test_preds_blend), ncol = 1)
    ensemble_average <- rowMeans(as.data.frame(test_preds_blend)) # Simple ensemble average, consider blending/stacking
    ensemble_average <- as.data.frame(ensemble_average)
    
    
    colnames(ensemble_average)[1] <- targets[resp]
    if (resp == 1) {
      final_submission <- ensemble_average
    } else {
      final_submission <- cbind(final_submission, ensemble_average)
    }
    print(head(final_submission))
  }
  
  ## Remove no longer needed old models and temporaries from K-V store to keep memory footprint low
  ls_temp <- h2o.ls(h2oServer)
  for (n_ls in 1:nrow(ls_temp)) {
    if (str_detect(ls_temp[n_ls, 1], "DeepLearning") ||
          str_detect(ls_temp[n_ls, 1], "SpeeDRF") ||
          str_detect(ls_temp[n_ls, 1], "Last.value")) {
      h2o.rm(h2oServer, keys = as.character(ls_temp[n_ls, 1]))
    }
  }
}
if (validate) {
  cat("\nOverall cross-validated MSEs = " , MSEs)
  cat("\nOverall cross-validated MSE = " , mean(MSEs))
  cat("\nOverall cross-validated LogLosses = " , LogLoss)
  cat("\nOverall cross-validated LogLoss = " , mean(LogLoss))
  cat("\n")
}
if (submit) {
  print(summary(final_submission))
  submission <- read.csv(path_submission)
  fs <- as.matrix(final_submission)
  dim(fs) <- c(prod(dim(fs)),1) #reshape predictions into 1D
  summary(fs)
  submission[,2] <- fs #replace 0s with actual predictions
  write.csv(submission, file = "./submission.csv", quote = F, row.names = F)
}
sink()
