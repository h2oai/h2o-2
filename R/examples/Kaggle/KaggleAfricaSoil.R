if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }
install.packages("h2o", repos=(c("file:///Users/arno/h2o/target/R", getOption("repos"))))
#install.packages("h2o", repos=(c("file:///home/arno/h2o/target/R", getOption("repos"))))

suppressMessages(library(h2o))
localH2O <- h2o.init(ip="mr-0xd1", port = 53322)
#localH2O <- h2o.init(max_mem_size = '8g', beta=T)

#suppressMessages(if (!require(h2o)) install.packages("caret"))
#suppressMessages(library(caret))

# Import data
path_cloud <- "~/"
#path_train <- paste0(path_cloud, "h2o/smalldata/mnist/train.csv.gz")
#path_test <- paste0(path_cloud, "h2o/smalldata/mnist/test.csv.gz")
#path_output <- paste0(path_cloud, "/blending_mnist/outputs")
path_train <- paste0(path_cloud, "/kaggle_africasoil/data/training.csv.gz")
path_train1 <- paste0(path_cloud, "/kaggle_africasoil/data/train_shuf1.csv.gz")
path_train2 <- paste0(path_cloud, "/kaggle_africasoil/data/train_shuf2.csv.gz")
path_train3 <- paste0(path_cloud, "/kaggle_africasoil/data/train_shuf3.csv.gz")
path_train4 <- paste0(path_cloud, "/kaggle_africasoil/data/train_shuf4.csv.gz")
path_train5 <- paste0(path_cloud, "/kaggle_africasoil/data/train_shuf5.csv.gz")

path_test <- paste0(path_cloud, "/kaggle_africasoil/data/sorted_test.csv.gz")
path_output <- paste0(path_cloud, "/kaggle_africasoil/outputs")

train_hex <- h2o.uploadFile(localH2O, path = path_train)
train_hex1 <- h2o.uploadFile(localH2O, path = path_train1)
train_hex2 <- h2o.uploadFile(localH2O, path = path_train2)
train_hex3 <- h2o.uploadFile(localH2O, path = path_train3)
train_hex4 <- h2o.uploadFile(localH2O, path = path_train4)
train_hex5 <- h2o.uploadFile(localH2O, path = path_train5)

test_hex <- h2o.uploadFile(localH2O, path = path_test)

#problem with this is that splitFrame only shuffles intra-chunk, that's why we made 5 pre-shuffled training data sets
train_hex <- h2o.rebalance(train_hex, key = "train_rebalanced", chunks=32)
train_hex1 <- h2o.rebalance(train_hex1, key = "train_rebalanced1", chunks=32)
train_hex2 <- h2o.rebalance(train_hex2, key = "train_rebalanced2", chunks=32)
train_hex3 <- h2o.rebalance(train_hex3, key = "train_rebalanced3", chunks=32)
train_hex4 <- h2o.rebalance(train_hex4, key = "train_rebalanced4", chunks=32)
train_hex5 <- h2o.rebalance(train_hex5, key = "train_rebalanced5", chunks=32)


# group features
vars <- colnames(train_hex)

#predictors <- vars[1:784] 
#targets <- vars[785]

spectra_hi <- vars[seq(2,2500,by=10)] # cheap way of binning: just take every 10-th column
spectra_hi_all <- vars[seq(2,2500,by=1)]
spectra_hi_lots <- vars[seq(2,2500,by=5)]
spectra_omit <- vars[seq(2501,2670,by=10)] # cheap way of binning: just take every 10-th column
spectra_low <- vars[seq(2671,3579,by=10)] # cheap way of binning: just take every 10-th column
spectra_low_all <- vars[seq(2671,3579,by=1)]
spectra_low_lots <- vars[seq(2671,3579,by=5)]
extra <- vars[3580:3595]
predictors <- c(spectra_hi, spectra_low, extra)
allpredictors <- c(spectra_hi_all, spectra_low_all, extra)
lotspredictors <- c(spectra_hi_lots, spectra_low_lots, extra)
targets <- vars[3596:3600]

## Parameters for run
validation = F ## use cross-validation to determine best model parameters
grid = F ## do a grid search
submit = T ## whether to create a submission 
submission = 22 ## submission index
blend = T

## Settings
n_loop <- 1
n_fold <- 5 # must be <= 5!!
ensemble = (n_loop > 1) # only used if blend = F and submit = T

## Train a DL model
errs = 0

cv_preds <- matrix(0, nrow = nrow(train_hex), ncol = 1)
holdout_valid_se <- matrix(0, nrow = 1, ncol = length(targets))
holdout_valid_mse <- matrix(0, nrow = 1, ncol = length(targets))
for (resp in 1:length(targets)) {
  if (validation) {
    if (grid) {
      cat("\n\nNow running grid search for ", targets[resp], "...\n")
      
      # run grid search with n-fold cross-validation
      gridmodel <- 
        h2o.deeplearning(x = predictors,
                         y = targets[resp],
                         data = train_hex,
                         nfolds = n_fold,
                         classification = F,
                         score_training_samples = 0,
                         score_validation_samples = 0,
                         score_duty_cycle = 0.1,
                         score_interval = 5,
                         max_w2 = 10,
                         force_load_balance=T,
                         activation="RectifierWithDropout", hidden_dropout_ratios = c(0.1,0.1), 
                         hidden = c(100,100), epochs = 200, l1 = 0, l2 = 1e-5, rho = 0.95, epsilon = 1e-6, train_samples_per_iteration = 1000 
              )
      print(gridmodel)
      
      # print grid search results to file
      sink(paste0(path_output, "/submission_", submission, "_", targets[resp], "_grid"))
      print(gridmodel)
      sink()
      
      errs = errs + sqrt(gridmodel@sumtable[[1]]$prediction_error)
    } else {
      cat("\n\nTraining cv model for ", targets[resp], "...\n")
      # run one model with n-fold cross-validation
      cvmodel <- 
        h2o.deeplearning(x = predictors, y = targets[resp],
                         data = train_hex,
                         classification = F, 
                         nfolds = n_fold,
                         score_training_samples = 0,
                         score_validation_samples = 0,
                         score_duty_cycle = 1,
                         score_interval = 1e-1,
                         force_load_balance=T,
                         activation="Rectifier", hidden = c(100,100,100), epochs = 500, l1 = 1e-5, rho = 0.95, epsilon = 1e-6, train_samples_per_iteration = 10000
        )
      print(cvmodel)
      
      #       cvmodel <- 
      #         h2o.gbm(x = x = predictors,
      #                          y = targets[resp],
      #                          data = train_hex,
      #                          nfolds = 5,
      #                          distribution = "gaussian",
      #                          n.trees = 10
      #         )
      #       print(cvmodel)
      
      # print grid search results to file
      sink(paste0(path_output, "/submission_", submission, "_", targets[resp], "_cv"))
      print(cvmodel)
      sink()
      
      errs = errs + sqrt(cvmodel@model$valid_sqr_error)
    }
  }
  
  if (submit) {
    if (validation) {
      if (grid) {
        cat("\n\nTaking parameters from grid search winner for ", targets[resp], "...\n")
        p <- gridmodel@sumtable[[1]]
      } else {
        cat("\n\nTaking parameters from cv model for ", targets[resp], "...\n")
        p <- cvmodel@model$params
      }
      # print grid search results to file
      sink(paste0(path_output, "/submission_", submission, "_", targets[resp], "_final_params"))
      print(p)
      sink()
      
      if (blend) {
        # blending
        y <- as.matrix(train_hex[, targets[resp]])
        
        ## Loops
        for (n in 1:n_loop) {
          
          ## CV
          #set.seed(n)
          #rand_folds <- createFolds(y, k = n_fold)
          
          ## Main Loop
          for (nn in 1:n_fold) {
            
            ##
            cat("\n\nNow training loop", n, "/", n_loop, "model", nn, "/", n_fold, "for ", targets[resp], "...\n")
             
#             folds <- h2o.nFoldExtractor(train_hex, nfolds=n_fold, fold_to_extract=nn)
#             train <- folds[[1]]
#             valid <- folds[[2]]    
#             response_folds <- h2o.nFoldExtractor(train_hex[,targets[resp]], nfolds=n_fold, fold_to_extract=nn)
#             train_resp <- response_folds[[1]]
#             valid_resp <- response_folds[[2]]
            
            splits <- h2o.splitFrame(train_hex, ratios = 1.-1./n_fold, shuffle=T)
            train <- splits[[1]]
            valid <- splits[[2]]
            train_resp <- train[,targets[resp]]
            valid_resp <- valid[,targets[resp]]
            
            # build final model blend components with validated parameters
            model <- h2o.deeplearning(x = predictors, y = targets[resp], key = paste0(targets[resp], submission, "_blend_", n , "_", nn), 
                                      data = train,
                                      validation = valid,
                                      classification = F, 
                                      score_training_samples = 0,
                                      score_validation_samples = 0,
                                      score_duty_cycle = 1,
                                      score_interval = 1e-1,
                                      force_load_balance=T,
                                      activation = p$activation, input_dropout_ratio = p$input_dropout_ratio, hidden = p$hidden, epochs = p$epochs, l1 = p$l1, l2 = p$l2, max_w2 = p$max_w2, train_samples_per_iteration = p$train_samples_per_iteration)
            
            ## Use the model and store results
            yy_temp_train <- as.data.frame(h2o.predict(model, train))
            yy_temp_valid <- as.data.frame(h2o.predict(model, valid))
            yy_temp_test <- as.data.frame(h2o.predict(model, test_hex))
            
            ## Store
            if ((n == 1) & (nn == 1)) {
              yy_test_all <- matrix(yy_temp_test[, 1], ncol = 1)
            } else {
              yy_test_all <- cbind(yy_test_all, matrix(yy_temp_test[, 1], ncol = 1))
            }
            print(model)
            msetrain <- mean((yy_temp_train - as.data.frame(train_resp))^2)
            sevalid <- (yy_temp_valid - as.data.frame(valid_resp))^2
            msevalid <- mean(sevalid)
            holdout_valid_se[resp] <- holdout_valid_se[resp] + sum(sevalid)
            
            cat("\nMSE on training dataset for", targets[resp], ":", msetrain, "\n")
            cat("\nRMSE on training dataset for", targets[resp], ":", sqrt(msetrain), "\n")
            cat("\nMSE on holdout validation dataset for", targets[resp], ":", msevalid, "\n")
            cat("\nRMSE on holdout validation dataset for", targets[resp], ":", sqrt(msevalid), "\n")
            
            # print blending results to file
            sink(paste0(path_output, "/submission_", submission, "_", targets[resp], "_blend_loop", n, "_fold", nn))
            print(model)
            cat("\nMSE on training dataset for", targets[resp], ":", msetrain, "\n")
            cat("\nRMSE on training dataset for", targets[resp], ":", sqrt(msetrain), "\n")
            cat("\nMSE on holdout validation dataset for", targets[resp], ":", msevalid, "\n")
            cat("\nRMSE on holdout validation dataset for", targets[resp], ":", sqrt(msevalid), "\n")
            sink()
          } 
          
        } 
        holdout_valid_se[resp] <- holdout_valid_se[resp]/n_loop
        holdout_valid_mse[resp] <- holdout_valid_se[resp]/nrow(train_hex) ## total number of (cross-)validation rows = # training rows
        cat("\nOverall MSE on holdout validation dataset for", targets[resp], ":", holdout_valid_mse[resp], "\n")
      } 
      else {
        # no blending
        
        # build final model on full training data with validated parameters
        model <- h2o.deeplearning(x = predictors, y = targets[resp], key = paste0(targets[resp], submission, "_cv"), data = train_hex, classification = F, score_training_samples = 0,
                                  activation = p$activation, input_dropout_ratio = p$input_dropout_ratio, hidden = p$hidden, epochs = p$epochs, l1 = p$l1, l2 = p$l2, max_w2 = p$max_w2, train_samples_per_iteration = p$train_samples_per_iteration)
      }
    }
    else {
      if (blend) {
        # for blending scoring
#        blend_holdout_train_preds <- as.data.frame(matrix(0, nrow = 0, ncol = n_loop)) ## empty vectors to be filled with n-fold holdout predictions, one vector per n_loop
        
        ## Loops
        for (n in 1:n_loop) {
  
          ## Main Loop
          for (nn in 1:n_fold) {
            
            ##
            cat("\n\nNow training loop", n, "/", n_loop, "model", nn, "/", n_fold, "for ", targets[resp], "...\n")
            
            #             folds <- h2o.nFoldExtractor(train_hex, nfolds=n_fold, fold_to_extract=nn)
            #             train <- folds[[1]]
            #             valid <- folds[[2]]
            #             response_folds <- h2o.nFoldExtractor(train_hex[,targets[resp]], nfolds=n_fold, fold_to_extract=nn)
            #             train_resp <- response_folds[[1]]
            #             valid_resp <- response_folds[[2]]
            
            if (nn==1) train_data <- train_hex1
            if (nn==2) train_data <- train_hex2
            if (nn==3) train_data <- train_hex3
            if (nn==4) train_data <- train_hex4
            if (nn==5) train_data <- train_hex5
            
            splits <- h2o.splitFrame(train_data, ratios = 1.-1./n_fold, shuffle=T)
            train <- splits[[1]]
            valid <- splits[[2]]
            train_resp <- train[,targets[resp]]
            valid_resp <- valid[,targets[resp]]
            
            # build final model blend components with hardcoded parameters
            if (resp == 4) #SOC 0.06
              model <- h2o.deeplearning(x = predictors, y = targets[resp], key = paste0(targets[resp], submission, "_blend_", n , "_", nn), 
                                        data = train,
                                        validation = valid,
                                        classification = F, 
                                        score_training_samples = 0,
                                        score_validation_samples = 0,
                                        score_duty_cycle = 1,
                                        score_interval = 0.1,
                                        force_load_balance=F,
                                        override_with_best_model=T,
                                        activation="Rectifier", hidden = c(100,100,100), epochs = 100, l1 = 1e-5, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 5000)                           
              
            ## Use the model and store results
            yy_temp_train <- as.data.frame(h2o.predict(model, train))
            yy_temp_valid <- as.data.frame(h2o.predict(model, valid))
            yy_temp_test <- as.data.frame(h2o.predict(model, test_hex))
            
            ## For blending, stitch together the predictions for the validation holdout, after n_folds models are done, this will be the predictions on the full training data and can be scored
            ## For each n_loop, there's a new vector in this frame, so we can blend it together with GLM
            #blend_holdout_train_preds[,n] <- rbind(blend_holdout_train_preds[,n], yy_temp_valid)
            #blend_holdout_train_preds

            ## Store
            if ((n == 1) & (nn == 1)) {
              yy_test_all <- matrix(yy_temp_test[, 1], ncol = 1)
            } else {
              yy_test_all <- cbind(yy_test_all, matrix(yy_temp_test[, 1], ncol = 1))
            }
            print(head(yy_test_all))
            msetrain <- mean((yy_temp_train - as.data.frame(train_resp))^2)
            sevalid <- (yy_temp_valid - as.data.frame(valid_resp))^2
            msevalid <- mean(sevalid)
            holdout_valid_se[resp] <- holdout_valid_se[resp] + sum(sevalid)
            
            # print blending results to file
            sink(paste0(path_output, "/submission_", submission, "_", targets[resp], "_blend_loop", n, "_fold", nn))
            print(model)
            cat("\nMSE on training dataset for", targets[resp], ":", msetrain, "\n")
            cat("\nRMSE on training dataset for", targets[resp], ":", sqrt(msetrain), "\n")
            cat("\nMSE on holdout validation dataset for", targets[resp], ":", msevalid, "\n")
            cat("\nRMSE on holdout validation dataset for", targets[resp], ":", sqrt(msevalid), "\n")
            sink()
            
            print(model)
            cat("\nMSE on training dataset for", targets[resp], ":", msetrain, "\n")
            cat("\nRMSE on training dataset for", targets[resp], ":", sqrt(msetrain), "\n")
            cat("\nMSE on holdout validation dataset for", targets[resp], ":", msevalid, "\n")
            cat("\nRMSE on holdout validation dataset for", targets[resp], ":", sqrt(msevalid), "\n")   
          } 
        }
        holdout_valid_se[resp] <- holdout_valid_se[resp]/n_loop
        holdout_valid_mse[resp] <- holdout_valid_se[resp]/nrow(train_hex) ## total number of (cross-)validation rows = # training rows
        cat("\nOverall", n_fold, "-fold cross-validated MSE on training dataset for", targets[resp], ":", holdout_valid_mse[resp], "\n")
        
        #cat("\nStitched together (cross-check, used for blending):", mean((blend_holdout_train_preds - y)^2), "\n")
        
      } else {
        
        if (!ensemble) {
          # build final model on full training data with hardcoded parameters
          model <- h2o.deeplearning(x = predictors, y = targets[resp], key = paste0(targets[resp], submission, "_hardcoded"), data = train_hex, classification = F, score_training_samples = 0, force_load_balance=T,
                                    #activation="Rectifier", hidden = c(300,300,300), epochs = 1000, l1 = 1e-5, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 100000 #submission 1 - 0.42727
                                    #activation="RectifierWithDropout",input_dropout_ratio = 0.2, hidden = c(500,500,500), epochs = 500, l1 = 1e-5, l2 = 1e-5, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 50000 #submission 2 - 0.684
                                    #activation="Rectifier", hidden = c(500,500,500), epochs = 1000, l1 = 1e-5, l2 = 1e-5, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 10000 #submission 3 - 0.45212
                                    #activation="Rectifier", hidden = c(300,300,300), epochs = 2000, l1 = 0, l2 = 1e-5, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 100000 #submission 4 - 0.44199
                                    #activation="Rectifier", hidden = c(300,300,300), epochs = 500, l1 = 1e-5, l2=0, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 100000 #submission 5 - 0.47247
                                    #activation="Rectifier", hidden = c(300,300,300), epochs = 2000, l1 = 1e-5, l2=0, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 100000 #submission 6 - 0.45016
                                    #activation="Rectifier", hidden = c(500,500,500), epochs = 3000, l1 = 1e-5, l2=0, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 100000 #submission 12 0.54
                                    activation="Rectifier", hidden = c(300,300,300), epochs = 1000, l1 = 1e-5, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 10000 #submission 15 0.482                  
          )
        } else {
          for (n in 1:n_loop) {
            #ensemble model on full trainin data without holdout validation
            model <- h2o.deeplearning(x = predictors, y = targets[resp], key = paste0(targets[resp], submission, "_blend_", n , "_", nn), 
                                      data = train,
                                      validation = valid,
                                      classification = F, 
                                      score_training_samples = 0,
                                      score_validation_samples = 0,
                                      score_duty_cycle = 1,
                                      score_interval = 0.1,
                                      force_load_balance=F,
                                      override_with_best_model=T,
                                      activation="Rectifier", hidden = c(100,100,100), epochs = 100, l1 = 1e-5, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 5000)                           
            
            yy_temp_test <- as.data.frame(h2o.predict(model, test_hex))
            if (n == 1) {
              yy_test_all <- matrix(yy_temp_test[, 1], ncol = 1)
            } else {
              yy_test_all <- cbind(yy_test_all, matrix(yy_temp_test[, 1], ncol = 1))
            }
          }
        }
      }
    }
    
    ## Now create submission
    print(model)
    if (!blend) {
      sink(paste0(path_output, "/submission_", submission, "_", targets[resp], "_score"))
      print(model)
      sink()
    } else if (!ensemble) {
      cat("\nOverall", n_fold, "-fold cross-validated MSE on training dataset:", holdout_valid_mse, "\n")
      cat("\nOverall", n_fold, "-fold cross-validated RMSE on training dataset:", sqrt(holdout_valid_mse), "\n")
      cat("\nOverall", n_fold, "-fold cross-validated CMRMSE on training dataset:", mean(sqrt(holdout_valid_mse)), "\n")
      sink(paste0(path_output, "/submission_", submission, "_", targets[resp], "_score"))
      cat("\nOverall", n_fold, "-fold cross-validated MSE on training dataset:", holdout_valid_mse, "\n")
      cat("\nOverall", n_fold, "-fold cross-validated RMSE on training dataset:", sqrt(holdout_valid_mse), "\n")
      cat("\nOverall", n_fold, "-fold cross-validated CMRMSE on training dataset:", mean(sqrt(holdout_valid_mse)), "\n")
      sink()
    }
    if (validation) {
      errs = errs/length(targets)
      sink(paste0(path_output, "/submission_", submission, "_", targets[resp], "_errs"))
      print(paste0("Validation CMRMSE = " , errs))
      sink()
    }
    
    ## Make predictions
    if (blend | ensemble) {
      if (blend) cat("\nBlending results\n") else cat("\nEnsemble average\n")
      yy_test_avg <- matrix("test_target", nrow = nrow(yy_test_all), ncol = 1)
      yy_test_avg <- rowMeans(yy_test_all) ### TODO: Use GLM to find best blending factors based on yy_fulltrain_all?
      pred <- as.data.frame(yy_test_avg)
    } else {
      cat("\nWARNING: NOT blending results\n")
      pred <- as.data.frame(h2o.predict(model, test_hex))
    }
    colnames(pred)[1] <- targets[resp]
    if (resp == 1) {
      preds <- cbind(as.data.frame(test_hex[,1]), pred)
    } else {
      preds <- cbind(preds, pred)
    }
    print(head(preds))
    ## Save the model as well
    #h2o.saveModel(object = model, dir = path_output, name = paste0("submission_", submission, targets[resp]), force = T)
  }
}

if (submit) {
  ## Write final submission
  write.csv(preds, file = paste0(path_output, "/submission_", submission, ".csv"), quote = F, row.names=F)
}

### MNIST ONLY
#print(paste0("Actual RMSE of ensemble prediction on test: ", sqrt(mean((preds[2] - as.data.frame(test_hex[,targets[resp]]))^2))))

## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## System and Session Info
## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

print(sessionInfo())
print(Sys.info())

#1/5*(sqrt(0.089)+sqrt(0.79)+sqrt(0.179)+sqrt(0.1109)+sqrt(0.1437)) # 0.4644653 submission 1 (CV-values using 3 folds)
1/5*(sqrt(0.094)+sqrt(0.90)+sqrt(0.174)+sqrt(0.108)+sqrt(0.142)) # 0.47 submission 1 repro 3 fold #2
1/5*(sqrt(0.0553)+sqrt(0.898)+sqrt(0.1686)+sqrt(0.1035)+sqrt(0.1215)) # 0.452, but scored 0.50223 on their test set holdout!! submission 14 (5 ensemble cv models)
1/5*(sqrt(0.0758)+sqrt(0.6847)+sqrt(0.1553)+sqrt(0.0874)+sqrt(0.118)) #submission 16 should be 0.4273, but scored 0.48375 (10-fold cv models blend)
1/5*(sqrt(0.0600)+?) #submission 17 (50-fold cv models blend)
#Overall 10 -fold cross-validated MSE on training dataset: 0.05225971 0.6752378 0.1674132 0.07708522 0.2251007 #submission 18 should be 0.4423, but scored 0.620!!!! Wasn't blending
#Overall 5 -fold cross-validated MSE on training dataset: 0.07033987 0.6485939 0.1662352 0.09940603 0.1279225 #submission 19, should be 0.43, but scored 0.47 Wasn't blending
#Overall 10 -fold cross-validated MSE on training dataset: 0.06074805 0.6604369 0.1668773 0.08833406 0.1117856 #submission 20, should be 0.419, but scored 0.447 Early stopping on 10% validation was noisy
#Overall 3 -fold cross-validated MSE on training dataset: 0.06258908 1.091812 0.1782126 0.09278491 0.1400748 #submission 21, should be 0.479 
#Overall 5 -fold cross-validated MSE on training dataset: 0.101359 0.77772 0.1644998 0.06177728 0.1256438 # submission 22, should be 0.4417, scored 0.43439! Finally working logic after shuffling properly!

#GOAL: 1/5*(sqrt(0.06)+sqrt(0.64)+sqrt(0.15)+sqrt(0.07)+sqrt(0.09))
#BEST: 