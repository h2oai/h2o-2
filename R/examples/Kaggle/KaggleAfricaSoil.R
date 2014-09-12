#if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
#if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }
#install.packages("h2o", repos=(c("file:///Users/arno/h2o/target/R", getOption("repos"))))
#install.packages("h2o", repos=(c("file:///home/arno/h2o/target/R", getOption("repos"))))

suppressMessages(library(h2o))
#localH2O <- h2o.init(ip="192.168.1.185", port = 53322)
localH2O <- h2o.init(max_mem_size = '80g', beta=T)

suppressMessages(if (!require(h2o)) install.packages("caret"))
suppressMessages(library(caret))

# Import data
path_cloud <- "~/"
path_train <- paste0(path_cloud, "/kaggle_africasoil/data/training_shuf.csv.gz")
path_test <- paste0(path_cloud, "/kaggle_africasoil/data/sorted_test.csv.gz")
path_output <- paste0(path_cloud, "/kaggle_africasoil/outputs")

train_hex <- h2o.uploadFile(localH2O, path = path_train)
test_hex <- h2o.uploadFile(localH2O, path = path_test)


# if rebalanced, use this:
#train_hex <- h2o.getFrame(localH2O, key = "train_rebalanced")

#train_splits <- h2o.splitFrame(train_hex, 0.75, shuffle = T)
#train <- train_splits[[1]]
#valid <- train_splits[[2]]

# group features
vars <- colnames(train_hex)
spectra_hi <- vars[2:2500] 
spectra_omit <- vars[2501:2670] 
spectra_low <- vars[2671:3579] 
extra <- vars[3580:3595]
targets <- vars[3596:3600]



## Parameters for run
validation = F ## use cross-validation to determine best model parameters
grid = F ## do a grid search
submit = T ## whether to create a model on the full training data for submission 
submission = 12 ## submission index
blend = F

## Settings
n_loop <- 2
n_fold <- 10

## Train a DL model
errs = 0
#resp = 2 
mse <- matrix(0, nrow = 1, ncol = length(targets))
for (resp in 1:length(targets)) {
  if (validation) {
    if (grid) {
      cat("\n\nNow running grid search for ", targets[resp], "...\n")
      
      # run grid search with n-fold cross-validation
      gridmodel <- 
        h2o.deeplearning(x = c(spectra_hi, spectra_low, extra),
                         y = targets[resp],
                         data = train_hex,
                         nfolds = 3,
                         classification = F,
                         score_training_samples = 0,
                         score_validation_samples = 0,
                         score_duty_cycle = 0,
                         max_w2 = 10, 
                         #activation=c("RectifierWithDropout"), input_dropout_ratio = c(0.2), hidden_dropout_ratios = list(c(0.5,0.5,0.5)), hidden = list(c(300,300,300), c(500,500,500)), epochs = c(100), l1 = c(0,1e-5), l2 = c(0,1e-5), train_samples_per_iteration = 10000
                         activation=c("Rectifier"), hidden = c(100,100,100), epochs = c(100), l1 = c(0,1e-5), l2 = c(0,1e-5), train_samples_per_iteration = 10000
                         
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
        h2o.deeplearning(x = c(spectra_hi, spectra_low, extra),
                         y = targets[resp],
                         data = train_hex,
                         nfolds = 5,
                         classification = F,
                         score_training_samples = 0,
                         score_validation_samples = 0,
                         score_duty_cycle = 0,
                         max_w2 = 10, 
                         activation=c("Rectifier"), input_dropout_ratio=0.0, hidden = c(300,300,300), epochs = c(1000), l1 = 1e-5, l2 = 0, train_samples_per_iteration = 100000
        )
      print(cvmodel)
      
      #       cvmodel <- 
      #         h2o.gbm(x = c(spectra_hi, spectra_low, extra),
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
      cat("\n\nTaking validated model to make predictions for ", targets[resp], "...\n")
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
      
      ## TODO put blend logic to no-validation path as well
      if (blend) {
        # blending
        y <- as.matrix(train_hex[, vars[3595+resp]])
        
        ## Loops
        for (n in 1:n_loop) {
          
          ## CV
          set.seed(n)
          rand_folds <- createFolds(y, k = n_fold)
          
          ## Main Loop
          for (nn in 1:n_fold) {
            
            ##
            cat("\n\nNow training loop", n, "/", n_loop, "model", nn, "/", n_fold, "for ", targets[resp], "...\n")
            
            ## Split
            row_train <- as.integer(unlist(rand_folds[-nn]))
            row_valid <- as.integer(unlist(rand_folds[nn]))
            
            # build final model blend components with validated parameters
            model <- h2o.deeplearning(x = c(spectra_hi, spectra_low, extra), y = targets[resp], key = paste0(targets[resp], submission, "_blend_", n , "_", nn), 
                                      data = train_hex[row_train,],
                                      validation = train_hex[row_valid,],
                                      classification = F, 
                                      score_training_samples = 0,
                                      score_validation_samples = 0,
                                      score_duty_cycle = 1,
                                      activation = p$activation, input_dropout_ratio = p$input_dropout_ratio, hidden = p$hidden, epochs = p$epochs, l1 = p$l1, l2 = p$l2, max_w2 = p$max_w2, train_samples_per_iteration = p$train_samples_per_iteration)
            
            ## Use the model and store results
            yy_temp_train <- as.data.frame(h2o.predict(model, train_hex))
            yy_temp_test <- as.data.frame(h2o.predict(model, test_hex))
            
            ## Store
            if ((n == 1) & (nn == 1)) {
              yy_train_all <- matrix(yy_temp_train[, 1], ncol = 1)
              yy_test_all <- matrix(yy_temp_test[, 1], ncol = 1)
            } else {
              yy_train_all <- cbind(yy_train_all, matrix(yy_temp_train[, 1], ncol = 1))
              yy_test_all <- cbind(yy_test_all, matrix(yy_temp_test[, 1], ncol = 1))
            }
            print(model)
            
            # print blending results to file
            sink(paste0(path_output, "/submission_", submission, "_", targets[resp], "_blend_loop", n, "_fold", nn))
            print(model)
            cat("\nMSE on whole training dataset:", mean((yy_temp_train - y)^2), "\n")
            sink()
            
            cat("\nMSE on whole training dataset:", mean((yy_temp_train - y)^2), "\n")
            if (ncol(yy_train_all) >= 2) {
              yy_train_avg <- matrix("train_target", nrow = nrow(yy_train_all), ncol = 1)
              yy_train_avg <- rowMeans(yy_train_all)
              cat("\nMSE of the Ensemble: ", mse[resp] <- mean((yy_train_avg - y)^2), "\n")
              # show status
              cat("\nMSE of the Ensemble: ", mse, "\n")
            }
          } 
        } 
      } 
      else {
        # no blending
        
        # build final model on full training data with validated parameters
        model <- h2o.deeplearning(x = c(spectra_hi, spectra_low, extra), y = targets[resp], key = paste0(targets[resp], submission, "_cv"), data = train_hex, classification = F, score_training_samples = 0,
                                  activation = p$activation, input_dropout_ratio = p$input_dropout_ratio, hidden = p$hidden, epochs = p$epochs, l1 = p$l1, l2 = p$l2, max_w2 = p$max_w2, train_samples_per_iteration = p$train_samples_per_iteration)
      }
    }
    else {
      if (blend) {
        # blending
        y <- as.matrix(train_hex[, vars[3595+resp]])
        
        ## Loops
        for (n in 1:n_loop) {
          
          ## CV
          set.seed(n)
          rand_folds <- createFolds(y, k = n_fold)
          
          ## Main Loop
          for (nn in 1:n_fold) {
            
            ##
            cat("\n\nNow training loop", n, "/", n_loop, "model", nn, "/", n_fold, "...\n")
            
            ## Split
            row_train <- as.integer(unlist(rand_folds[-nn]))
            row_valid <- as.integer(unlist(rand_folds[nn]))
            
            # build final model blend components with hardcoded parameters
            if (resp == 1) # Ca
              model <- h2o.deeplearning(x = c(spectra_hi, spectra_low, extra), y = targets[resp], key = paste0(targets[resp], submission, "_blend_", n , "_", nn), 
                                        data = train_hex[row_train,],
                                        validation = train_hex[row_valid,],
                                        classification = F, 
                                        score_training_samples = 0,
                                        score_validation_samples = 0,
                                        score_duty_cycle = 1,
                                        activation="Rectifier", hidden = c(300,300,300), epochs = 500, l1 = 0, l2 = 1e-5, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 10000)
            if (resp == 2) # P
              model <- h2o.deeplearning(x = c(spectra_hi, spectra_low, extra), y = targets[resp], key = paste0(targets[resp], submission, "_blend_", n , "_", nn), 
                                        data = train_hex[row_train,],
                                        validation = train_hex[row_valid,],
                                        classification = F, 
                                        score_training_samples = 0,
                                        score_validation_samples = 0,
                                        score_duty_cycle = 1,
                                        activation="Rectifier", hidden = c(300,300,300), epochs = 500, l1 = 0, l2 = 1e-5, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 100000)
            if (resp == 3) #pH
              model <- h2o.deeplearning(x = c(spectra_hi, spectra_low, extra), y = targets[resp], key = paste0(targets[resp], submission, "_blend_", n , "_", nn), 
                                        data = train_hex[row_train,],
                                        validation = train_hex[row_valid,],
                                        classification = F, 
                                        score_training_samples = 0,
                                        score_validation_samples = 0,
                                        score_duty_cycle = 1,
                                        activation="Rectifier", hidden = c(300,300,300), epochs = 500, l1 = 1e-5, l2 = 1e-5, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 100000)
            if (resp == 4) #SOC
              model <- h2o.deeplearning(x = c(spectra_hi, spectra_low, extra), y = targets[resp], key = paste0(targets[resp], submission, "_blend_", n , "_", nn), 
                                        data = train_hex[row_train,],
                                        validation = train_hex[row_valid,],
                                        classification = F, 
                                        score_training_samples = 0,
                                        score_validation_samples = 0,
                                        score_duty_cycle = 1,
                                        activation="Rectifier", hidden = c(300,300,300), epochs = 500, l1 = 1e-5, l2 = 1e-5, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 100000)
            if (resp == 5) #Sand
              model <- h2o.deeplearning(x = c(spectra_hi, spectra_low, extra), y = targets[resp], key = paste0(targets[resp], submission, "_blend_", n , "_", nn), 
                                        data = train_hex[row_train,],
                                        validation = train_hex[row_valid,],
                                        classification = F, 
                                        score_training_samples = 0,
                                        score_validation_samples = 0,
                                        score_duty_cycle = 1,
                                        activation="Rectifier", hidden = c(300,300,300), epochs = 500, l1 = 1e-5, l2 = 0, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 100000)
                                    
            ## Use the model and store results
            yy_temp_train <- as.data.frame(h2o.predict(model, train_hex))
            yy_temp_test <- as.data.frame(h2o.predict(model, test_hex))
            
            ## Store
            if ((n == 1) & (nn == 1)) {
              yy_train_all <- matrix(yy_temp_train[, 1], ncol = 1)
              yy_test_all <- matrix(yy_temp_test[, 1], ncol = 1)
            } else {
              yy_train_all <- cbind(yy_train_all, matrix(yy_temp_train[, 1], ncol = 1))
              yy_test_all <- cbind(yy_test_all, matrix(yy_temp_test[, 1], ncol = 1))
            }
            print(model)
            
            # print blending results to file
            sink(paste0(path_output, "/submission_", submission, "_", targets[resp], "_blend_loop", n, "_fold", nn))
            print(model)
            cat("\nMSE on whole training dataset:", mean((yy_temp_train - y)^2), "\n")
            sink()
            
            cat("\nMSE on whole training dataset:", mean((yy_temp_train - y)^2), "\n")
            if (ncol(yy_train_all) >= 2) {
              yy_train_avg <- matrix("train_target", nrow = nrow(yy_train_all), ncol = 1)
              yy_train_avg <- rowMeans(yy_train_all)
              cat("\nMSE of the Ensemble: ", mse[resp] <- mean((yy_train_avg - y)^2), "\n")
              # show status
              cat("\nMSE of the Ensemble: ", mse, "\n")
            }
          } 
        }
      } else {
        # build final model on full training data with hardcoded parameters
        model <- h2o.deeplearning(x = c(spectra_hi, spectra_low, extra), y = targets[resp], key = paste0(targets[resp], submission, "_hardcoded"), data = train_hex, classification = F, score_training_samples = 0,
                                  #activation="Rectifier", hidden = c(300,300,300), epochs = 1000, l1 = 1e-5, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 100000 #submission 1 - 0.42727
                                  #activation="RectifierWithDropout",input_dropout_ratio = 0.2, hidden = c(500,500,500), epochs = 500, l1 = 1e-5, l2 = 1e-5, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 50000 #submission 2 - 0.684
                                  #activation="Rectifier", hidden = c(500,500,500), epochs = 1000, l1 = 1e-5, l2 = 1e-5, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 10000 #submission 3 - 0.45212
                                  #activation="Rectifier", hidden = c(300,300,300), epochs = 2000, l1 = 0, l2 = 1e-5, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 100000 #submission 4 - 0.44199
                                  #activation="Rectifier", hidden = c(300,300,300), epochs = 500, l1 = 1e-5, l2=0, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 100000 #submission 5 - 0.47247
                                  #activation="Rectifier", hidden = c(300,300,300), epochs = 2000, l1 = 1e-5, l2=0, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 100000 #submission 6 - 0.45016
                                  activation="Rectifier", hidden = c(500,500,500), epochs = 3000, l1 = 1e-5, l2=0, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 100000 #submission 12
        )
      }
    }
    
    ## Now create submission
    
    print(model)
    # print grid search results to file
    sink(paste0(path_output, "/submission_", submission, "_", targets[resp], "_score"))
    if (!blend) {
      print(model)
    } else {
      cat("\nMSE of the Ensemble: ", mse[resp], "\n")
      cat("\nRMSE of the Ensemble: ", sqrt(mse[resp]), "\n")
    }
    sink()
    
    ## Make predictions
    if (blend & validation) {
      cat("\nMSE of the Ensemble: ", mse[resp], "\n")
      cat("\nRMSE of the Ensemble: ", sqrt(mse[resp]), "\n")
      yy_test_avg <- matrix("test_target", nrow = nrow(yy_test_all), ncol = 1)
      yy_test_avg <- rowMeans(yy_test_all)
      pred <- as.data.frame(yy_test_avg)
    } else {
      pred <- as.data.frame(h2o.predict(model, test_hex))
    }
    colnames(pred)[1] <- targets[resp]
    if (resp == 1) {
      preds <- cbind(as.data.frame(test_hex[,1]), pred)
    } else {
      preds <- cbind(preds, pred)
    }
    ## Save the model as well
    #h2o.saveModel(object = model, dir = path_output, name = paste0("submission_", submission, targets[resp]), force = T)
  }
}


if (validation) {
  if (blend) {
    cat("\nMSE of the Ensemble: ", mse, "\n")
    cat("\nRMSE of the Ensemble: ", sqrt(mse), "\n")
    cat("\nCMRMSE of the Ensemble: ", mean(sqrt(mse)), "\n")
    sink(paste0(path_output, "/submission_", submission, ".score"))
    cat("\nMSE of the Ensemble: ", mse, "\n")
    cat("\nRMSE of the Ensemble: ", sqrt(mse), "\n")
    cat("\nCMRMSE of the Ensemble: ", mean(sqrt(mse)), "\n")
    sink()
  }
  errs = errs / length(targets)
  print(errs)
  write.csv(errs, file = paste0(path_output, "/submission_", submission, ".mcrmse"), row.names=F)
}
if (submit) {
  ## Write final submission
  write.csv(preds, file = paste0(path_output, "/submission_", submission, ".csv"), quote = F, row.names=F)
}  

## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## System and Session Info
## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

print(sessionInfo())
print(Sys.info())

#1/5*(sqrt(0.089)+sqrt(0.79)+sqrt(0.179)+sqrt(0.1109)+sqrt(0.1437)) # 0.4644653 submission 1 (CV-values using 3 folds)
