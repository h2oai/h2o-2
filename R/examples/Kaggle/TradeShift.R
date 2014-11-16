sink("TradeShift.log", split = T)

## This code block is to install a particular version of H2O
# START
if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }
install.packages("h2o", repos=(c("http://s3.amazonaws.com/h2o-release/h2o/master/1555/R", getOption("repos")))) #choose a build here
# END

# Fetch the latest nightly build using Jo-fai Chow's package
#devtools::install_github("woobe/deepr")
#deepr::install_h2o()

library(h2o)

library(stringr)

## Connect to H2O server (On server(s), run 'java -Xmx8g -ea -jar h2o.jar -port 53322 -name TradeShift' first)
## Go to http://server:53322/ to check Jobs/Data/Models etc.
#h2oServer <- h2o.init(ip="server", port = 53322)
h2o.shutdown(h2oServer)
## Launch H2O directly on localhost, go to http://localhost:54321/ to check Jobs/Data/Models etc.!
h2oServer <- h2o.init(nthreads = -1, max_mem_size = '8g')

## Import data
path_train <- "/Users/arno/kaggle_tradeshift/data/train.csv"
path_trainLabels <- "/Users/arno/kaggle_tradeshift/data/trainLabels.csv"
path_test <- "/Users/arno/kaggle_tradeshift/data/test.csv"
path_submission <- "/Users/arno/kaggle_tradeshift/data/sampleSubmission.csv"

train_hex <- h2o.importFile(h2oServer, path = path_train)
trainLabels_hex <- h2o.importFile(h2oServer, path = path_trainLabels)
test_hex <- h2o.importFile(h2oServer, path = path_test)

## Group variables
vars <- colnames(train_hex)
ID <- vars[1]
labels <- colnames(trainLabels_hex)
predictors <- vars[c(-1,-92)] #remove ID and one features with too many factors
targets <- labels[-1] #remove ID

## Settings (at least one of the following two settings has to be TRUE)
validate = T #whether to compute CV error on train/validation split (or n-fold), potentially with grid search
submitwithfulldata = T #whether to use full training dataset for submission (if FALSE, then the validation model(s) will make test set predictions)

ensemble_size <- 2 # more -> lower variance
seed0 = 1337
reproducible_mode = T # Set to TRUE if you want reproducible results, e.g. for final Kaggle submission if you think you'll win :)  Note: will be slower for DL

## Scoring helpers
tLogLoss <- matrix(0, nrow = 1, ncol = length(targets))
vLogLoss <- matrix(0, nrow = 1, ncol = length(targets))

## Attach the labels to the training data
trainWL <- h2o.exec(h2oServer,expr=cbind(train_hex, trainLabels_hex))
trainWL <- h2o.assign(trainWL, "trainWL")
h2o.rm(h2oServer, keys = c("train.hex","trainLabels.hex")) #no longer need these two individually
h2o.rm(h2oServer, grep(pattern = "Last.value", x = h2o.ls(h2oServer)$Key, value = TRUE))

## Impute missing values based on group-by on targets
for (i in predictors) {
  if (sum(is.na(trainWL[,i]))==0 || sum(is.na(trainWL[,i])) == nrow(trainWL)) next
  h2o.impute(trainWL,i,method='mean',targets)
}

# Split the training data into train/valid (95%/5%)
## Want to keep train large enough to make a good submission if submitwithfulldata = F
splits <- h2o.splitFrame(trainWL, ratios = 0.95, shuffle=!reproducible_mode)
train <- splits[[1]]
valid <- splits[[2]]

## Main loop over targets
for (resp in 1:length(targets)) {
  # always just predict class 0 for y_14 (is constant)
  if (resp == 14) {
    final_submission <- cbind(final_submission, as.data.frame(matrix(0, nrow = nrow(test_hex), ncol = 1)))
    colnames(final_submission)[resp] <- targets[resp]
    next
  }

  if (validate) {
    cat("\n\nNow training and validating an ensemble model for", targets[resp], "...\n")
    train_resp <- train[,targets[resp]]
    valid_resp <- valid[,targets[resp]]

    for (n in 1:ensemble_size) {
      cat("\n\nBuilding ensemble validation model", n, "of", ensemble_size, "for", targets[resp], "...\n")

      cvmodel <-
        h2o.randomForest(x = predictors,
                         y = targets[resp],
                         data = train,
                         validation = valid,
                         classification = T,
                         #type = "BigData", ntree = 50, depth = 30, mtries = 20, nbins = 50, #ensemble_size 1: training LL: 0.002863313 validation LL: 0.009463341 LB: 0.094373
                         #type = "BigData", ntree = 100, depth = 30, mtries = 30, nbins = 100, #ensemble_size 1: training LL: 0.002892511 validation LL: 0.008592581
                         type = "fast", ntree = c(10,20), depth = c(5,10), mtries = 10, nbins = 10, #demo for grid search
                         seed = seed0 + resp*ensemble_size + n
        )

      model <- cvmodel@model[[1]] #If cv model is a grid search model
      #model <- cvmodel #If cvmodel is not a grid search model
      
      # use probabilities - clamp validation predictions for LogLoss computation
      train_preds <- h2o.predict(model, train)[,3]
      valid_preds <- h2o.predict(model, valid)[,3]

      # compute LogLoss for this ensemble member, on training data
      tpc <- train_preds
      tpc <- h2o.exec(h2oServer,expr=ifelse(tpc > 1e-15, tpc, 1e-15))
      tpc <- h2o.exec(h2oServer,expr=ifelse(tpc < 1-1e-15, tpc, 1-1e-15))
      trainLL <- h2o.exec(h2oServer,expr=mean(-train_resp*log(tpc)-(1-train_resp)*log(1-tpc)))
      cat("\nLogLoss of this ensemble member on training data:", trainLL)

      # compute LogLoss for this ensemble member, on validation data
      vpc <- valid_preds
      vpc <- h2o.exec(h2oServer,expr=ifelse(vpc > 1e-15, vpc, 1e-15))
      vpc <- h2o.exec(h2oServer,expr=ifelse(vpc < 1-1e-15, vpc, 1-1e-15))
      validLL <- h2o.exec(h2oServer,expr=mean(-valid_resp*log(vpc)-(1-valid_resp)*log(1-vpc)))
      cat("\nLogLoss of this ensemble member on validation data:", validLL)

      if (!submitwithfulldata) {
        test_preds <- h2o.predict(model, test_hex)[,3]
      }
      if (n == 1) {
        valid_preds_ensemble <- valid_preds
        train_preds_ensemble <- train_preds
        if (!submitwithfulldata) {
          test_preds_ensemble <- test_preds
        }
      } else {
        valid_preds_ensemble <- valid_preds_ensemble + valid_preds
        train_preds_ensemble <- train_preds_ensemble + train_preds
        if (!submitwithfulldata) {
          test_preds_ensemble <- test_preds_ensemble + test_preds
        }
      }
    }
    train_preds <- train_preds_ensemble/ensemble_size ##ensemble average of probabilities
    valid_preds <- valid_preds_ensemble/ensemble_size ##ensemble average of probabilities
    if (!submitwithfulldata) {
      test_preds  <- test_preds_ensemble/ensemble_size
    }

    ## Compute LogLoss of ensemble
    train_preds <- h2o.exec(h2oServer,expr=ifelse(train_preds > 1e-15, train_preds, 1e-15))
    train_preds <- h2o.exec(h2oServer,expr=ifelse(train_preds < 1-1e-15, train_preds, 1-1e-15))
    tLL <- h2o.exec(h2oServer,expr=mean(-train_resp*log(train_preds)-(1-train_resp)*log(1-train_preds)))
    tLogLoss[resp] <- tLL
    cat("\nLogLosses of ensemble on training data so far:", tLogLoss)
    cat("\nMean LogLoss of ensemble on training data so far:", sum(tLogLoss)/resp)

    valid_preds <- h2o.exec(h2oServer,expr=ifelse(valid_preds > 1e-15, valid_preds, 1e-15))
    valid_preds <- h2o.exec(h2oServer,expr=ifelse(valid_preds < 1-1e-15, valid_preds, 1-1e-15))
    vLL <- h2o.exec(h2oServer,expr=mean(-valid_resp*log(valid_preds)-(1-valid_resp)*log(1-valid_preds)))
    vLogLoss[resp] <- vLL
    cat("\nLogLosses of ensemble on validation data so far:", vLogLoss)
    cat("\nMean LogLoss of ensemble on validation data so far:", sum(vLogLoss)/resp)

    if (!submitwithfulldata) {
      cat("\nMaking test set predictions with ensemble model on 95% of the data\n")
      ensemble_average <- as.data.frame(test_preds) #bring ensemble average to R
      colnames(ensemble_average)[1] <- targets[resp] #give it the right name
      if (resp == 1) {
        final_submission <- ensemble_average
      } else {
        final_submission <- cbind(final_submission, ensemble_average)
      }
      #print(head(final_submission))
    }
  }
  
  if (submitwithfulldata) {
    if (validate) {
      cat("\n\nTaking parameters from validation run (or grid search winner) for", targets[resp], "...\n")
      p <- cvmodel@model[[1]]@model$params #If cvmodel is a grid search model
      #p <- cvmodel@model$params   #If cvmodel is not a grid search model
    }
    else {
      p = list(classification = T, type = "BigData", ntree=50, depth=30, mtries=20, nbins=50) #LB: 0.0093360
    }
    ## Build an ensemble model on full training data - should perform better than the CV model above
    for (n in 1:ensemble_size) {
      cat("\n\nBuilding ensemble model", n, "of", ensemble_size, "for", targets[resp], "...\n")

      model <-
        h2o.randomForest(x = predictors,
                         y = targets[resp],
                         data = trainWL,
                         classification = T,
                         type = p$type,
                         ntree = p$ntree,
                         depth = p$depth,
                         mtries = p$mtries,
                         nbins = p$nbins,
                         seed = seed0 + resp*ensemble_size + n,
                         key = paste0(targets[resp], "_cv_ensemble_", n, "_of_", ensemble_size)
        )
      
      ## Aggregate ensemble model predictions (probabilities)
      test_preds <- h2o.predict(model, test_hex)[,3]
      
      if (n == 1) {
        test_preds_ensemble <- test_preds
      } else {
        test_preds_ensemble <- test_preds_ensemble + test_preds
      }
    }
    test_preds <- test_preds_ensemble/ensemble_size #simple ensemble average
    ensemble_average <- as.data.frame(test_preds) #bring ensemble average to R
    colnames(ensemble_average)[1] <- targets[resp] #give it the right name

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
    if (str_detect(ls_temp[n_ls, 1], "DRF") || str_detect(ls_temp[n_ls, 1], "Last.value")) {
      h2o.rm(h2oServer, keys = as.character(ls_temp[n_ls, 1]))
    }
  }
}
if (validate) {
  cat("\nOverall training LogLosses = " , tLogLoss)
  cat("\nOverall training LogLoss = " , mean(tLogLoss))
  cat("\nOverall validation LogLosses = " , vLogLoss)
  cat("\nOverall validation LogLoss = " , mean(vLogLoss))
  cat("\n")
}

print(summary(final_submission))
submission <- read.csv(path_submission)
#reshape predictions into 1D
fs <- t(as.matrix(final_submission))
dim(fs) <- c(prod(dim(fs)),1)
submission[,2] <- fs #replace 0s with actual predictions
write.csv(submission, file = "./submission.csv", quote = F, row.names = F)
sink()
