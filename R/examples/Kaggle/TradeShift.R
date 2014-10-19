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
predictors <- vars[c(-1,-4,-5,-35,-36,-62,-65,-66,-92,-95,-96)] #remove ID and variables with too many factor levels
targets <- labels[-1] ## all targets
#targets <- labels[c(7,8,10,11,13,29,30,31,32,33)]  ## harder to predict targets for tuning of parameters

## Settings (at least one of these has to be TRUE)
validate = T #whether to compute CV error on train/validation split (or n-fold), potentially with grid search
submitwithfulldata = F #whether to use full dataset for submission (if FALSE, then the validation model(s) will make test set predictions)

ensemble_size <- 1
seed0 = 1337

reproducible_mode = F # Set to TRUE if you want reproducible results, e.g. for final Kaggle submission if you think you'll win :)  Note: will be slower for DL

## Scoring helpers
LogLoss <- matrix(0, nrow = 1, ncol = length(targets))

## Split the training data into train/valid (95%/5%)
## Want to keep train large enough to make a good submission if submitwithfulldata = F
trainWL <- h2o.exec(h2oServer,expr=cbind(train_hex, trainLabels_hex))
splits <- h2o.splitFrame(trainWL, ratios = 0.95, shuffle=!reproducible_mode)
train <- splits[[1]]
valid <- splits[[2]]
train <- h2o.assign(train, "train")
valid <- h2o.assign(valid, "valid")

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
                         classification = T,
                         type = "fast",
                         ntree = 100,
                         depth = 20,
                         seed = seed0 + resp*ensemble_size + n
        )
      
      #model <- cvmodel@model[[1]] #If cv model is a grid search model
      model <- cvmodel #If cvmodel is not a grid search model
      
      # use probabilities - clamp validation predictions for LogLoss computation
      valid_preds <- h2o.predict(model, valid)[,3]

      # compute LogLoss for this ensemble member, on validation data, as a guidance to see the variance etc.
      vpc <- valid_preds
      vpc <- h2o.exec(h2oServer,expr=ifelse(vpc > 1e-15, vpc, 1e-15))
      vpc <- h2o.exec(h2oServer,expr=ifelse(vpc < 1-1e-15, vpc, 1-1e-15))
      myLL <- h2o.exec(h2oServer,expr=mean(-valid_resp*log(vpc)-(1-valid_resp)*log(1-vpc)))
      cat("\nLogLoss of this ensemble member on validation data:", myLL)

      #mean_resp <- as.data.frame(matrix(mean(train_resp), nrow = nrow(train_resp), ncol = 1))
      mean_resp <- mean(train_resp)
      meanLL <- h2o.exec(h2oServer,expr=mean(-valid_resp*log(mean_resp)-(1-valid_resp)*log(1-mean_resp)))
      cat("\nLogLoss of the mean response:", meanLL)

      if (!submitwithfulldata) {
        test_preds <- h2o.predict(model, test_hex)[,3]
        if (myLL > meanLL) {
          cat("\nReplacing the test set prediction with the mean response!")
          test_preds <- ifelse(test_preds[,1] > 1000, 0., mean_resp) #not sure how else to replace all elements with mean_resp..
        }
      }

      if (n == 1) {
        valid_preds_ensemble <- valid_preds
        if (!submitwithfulldata) {
          test_preds_ensemble <- test_preds
        }
      } else {
        valid_preds_ensemble <- valid_preds_ensemble + valid_preds
        if (!submitwithfulldata) {
          test_preds_ensemble <- test_preds_ensemble + test_preds
        }
      }
    }
    valid_preds <- valid_preds_ensemble/ensemble_size ##ensemble average of probabilities
    if (!submitwithfulldata) {
      test_preds  <- test_preds_ensemble/ensemble_size
    }

    ## Compute LogLoss of ensemble
    valid_preds <- h2o.exec(h2oServer,expr=ifelse(valid_preds > 1e-15, valid_preds, 1e-15))
    valid_preds <- h2o.exec(h2oServer,expr=ifelse(valid_preds < 1-1e-15, valid_preds, 1-1e-15))
    LL <- h2o.exec(h2oServer,expr=mean(-valid_resp*log(valid_preds)-(1-valid_resp)*log(1-valid_preds)))
    LogLoss[resp] <- LL
    cat("\nLogLosses of ensemble on validation data so far:", LogLoss)
    cat("\nMean LogLoss of ensemble on validation data so far:", sum(LogLoss)/resp)

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
      #p <- cvmodel@sumtable[[1]]  #If cvmodel is a grid search model
      p <- cvmodel@model$params   #If cvmodel is not a grid search model
    }
    else {
      p = list(ntree=100, depth=20) #For randomForest
    }
    ## Build an ensemble model on full training data - should perform better than the CV model above
    for (n in 1:ensemble_size) {
      cat("\n\nBuilding ensemble model", n, "of", ensemble_size, "for", targets[resp], "...\n")

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
    if (str_detect(ls_temp[n_ls, 1], "SpeeDRF") || str_detect(ls_temp[n_ls, 1], "Last.value")) {
      h2o.rm(h2oServer, keys = as.character(ls_temp[n_ls, 1]))
    }
  }
}
if (validate) {
  cat("\nOverall validation LogLosses = " , LogLoss)
  cat("\nOverall validation LogLoss = " , mean(LogLoss))
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
