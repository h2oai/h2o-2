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
predictors <- vars[c(-1,-92)] #remove ID and one features with too many factors
targets <- labels[-1] ## all targets
#targets <- labels[c(7,8,10,11,13,29,30,31,32,33)]  ## harder to predict targets for tuning of parameters

## Settings (at least one of the following two settings has to be TRUE)
validate = F #whether to compute CV error on train/validation split (or n-fold), potentially with grid search
submitwithfulldata = T #whether to use full training dataset for submission (if FALSE, then the validation model(s) will make test set predictions)

ensemble_size <- 2 # more -> lower variance
seed0 = 1337
reproducible_mode = T # Set to TRUE if you want reproducible results, e.g. for final Kaggle submission if you think you'll win :)  Note: will be slower for DL

## Scoring helpers
tLogLoss <- matrix(0, nrow = 1, ncol = length(targets))
vLogLoss <- matrix(0, nrow = 1, ncol = length(targets))

## Split the training data into train/valid (95%/5%)
## Want to keep train large enough to make a good submission if submitwithfulldata = F
trainWL <- h2o.exec(h2oServer,expr=cbind(train_hex, trainLabels_hex))
if (validate) {
  splits <- h2o.splitFrame(trainWL, ratios = 0.95, shuffle=!reproducible_mode)
  train <- splits[[1]]
  valid <- splits[[2]]
  train <- h2o.assign(train, "train")
  valid <- h2o.assign(valid, "valid")
}

## Main loop over targets
#for (resp in c(6,7,9,12,29,33)) { # FIXME: remove
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
                         type = "BigData", #this has better handling of categoricals than type = "fast", but is slower
                         ntree = c(50),
                         depth = c(30),
                         mtries = 20,
                         nbins = 50,
                         seed = seed0 + resp*ensemble_size + n
        )

      #y33 DRF
      #100trees, depth 20: validation ll 0.1305
      #50 trees, depth 30: training ll 0.0298 validation ll 0.0832
      #50 trees, depth 30, 20 mtries, 50 bins: training ll 0.0278 validation ll 0.0779
      #20 trees, depth 100: training ll 0.0232 validation ll 0.1140

      #overall
      #1 ensemble DRF ntree=100, depth=20
      #Overall validation LogLosses =  0.0036210883470459268897 0.00093248060944743443 0.0033672750311952927456 0.0021615067778732411849 0.00066611468647258254006 0.039699153689623581376 0.026062744200152396928 0.0038022229461941057724 0.047238971099232508755 0.011217721209381286557 0.00094352354247681296737 0.045193823452661284479 0.0052504059519060076663 0 0.0015075118782610749842 0.0031323080340788398909 0.00056565005960328878284 0.00049855167613287968299 0.00010742980631501478778 0.00038932701854535060665 0.0014864136358284606425 0.0011580518684932109442 0.00012617514326470962924 0.0065985565515910820505 0.00091284420340833911239 0.0037073679367141914968 0.0019028473236039631158 0.0024906651997033796807 0.030321520288551069566 0.0088540590503823502627 0.013449799883376137993 0.018525638668708464124 0.13056367073405611423
      #Overall validation LogLoss =  0.012619861227402436737

      # 1ensemble DRF 50 trees, depth 30, 20 mtries, 50 bins
      #Overall training LogLosses =  0.000705078 0.0001237611 0.000622789 0.0007093981 5.144581e-05 0.01030896 0.005183933 0.0007074265 0.01132459 0.002862326 0.0001907624 0.01342103 0.0009695712 0 0.000278939 0.0006330025 4.219361e-05 4.906694e-05 5.536219e-05 9.803132e-05 0.0004691322 0.0002654067 4.076982e-05 0.001107899 9.153072e-05 0.0006073599 0.0003994988 0.0004687263 0.006693895 0.0018135 0.003063326 0.003295198 0.02783544
      #Overall training LogLoss =  0.002863313
      #Overall validation LogLosses =  0.003536253 0.001190493 0.00187604 0.001776977 0.0006466543 0.02641678 0.02127753 0.007762937 0.03849423 0.01445234 0.001482586 0.03245364 0.005364658 0 0.001331533 0.002560752 0.000593061 0.0005078235 0.0001158875 0.0003086396 0.001246065 0.0008614129 0.0004870374 0.004126767 0.001185908 0.003610489 0.001881706 0.00291564 0.02438964 0.00748035 0.01031835 0.01369977 0.07793831
      #Overall validation LogLoss =  0.009463341 <-- best
      
      #model <- cvmodel@model[[1]] #If cv model is a grid search model
      model <- cvmodel #If cvmodel is not a grid search model
      
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
      #p <- cvmodel@sumtable[[1]]  #If cvmodel is a grid search model
      p <- cvmodel@model$params   #If cvmodel is not a grid search model
    }
    else {
      p = list(ntree=50, depth=30, mtries=20, nbins=50)
    }
    ## Build an ensemble model on full training data - should perform better than the CV model above
    for (n in 1:ensemble_size) {
      cat("\n\nBuilding ensemble model", n, "of", ensemble_size, "for", targets[resp], "...\n")

      model <-
        h2o.randomForest(x = predictors,
                         y = targets[resp],
                         data = trainWL,
                         classification = T,
                         type = "BigData",
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
