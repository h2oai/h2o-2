if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }
install.packages("h2o", repos=(c("file:///Users/arno/h2o/target/R", getOption("repos"))))
#install.packages("h2o", repos=(c("file:///home/arno/h2o/target/R", getOption("repos"))))

suppressMessages(library(h2o))
localH2O <- h2o.init(ip="mr-0xd1", port = 43322)
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

spectra_hi <- vars[seq(2,2500,by=5)] # cheap way of binning: just take every 5-th column
spectra_hi_all <- vars[seq(2,2500,by=1)]
spectra_hi_lots <- vars[seq(2,2500,by=5)]
spectra_omit <- vars[seq(2501,2670,by=1)] # cheap way of binning: just take every 10-th column
spectra_low <- vars[seq(2671,3579,by=5)] # cheap way of binning: just take every 10-th column
spectra_low_all <- vars[seq(2671,3579,by=1)]
spectra_low_lots <- vars[seq(2671,3579,by=5)]
extra <- vars[3580:3595]

predictors <- c(spectra_hi, spectra_low, extra)

allpredictors <- c(spectra_hi_all, spectra_omit, spectra_low_all, extra)
lotspredictors <- c(spectra_hi_lots, spectra_omit, spectra_low_lots, extra)
targets <- vars[3596:3600]


transform = c(F,T,F,F,F)

# Transform data
offsets <- vector("numeric", length=length(targets))

for (resp in 1:length(targets)) {
  if (transform[resp]) {
    var <- data.matrix(as.data.frame(train_hex[,targets[resp]]))
    offsets[resp] <- min(var) - 0.01 
    plot(log(var-offsets[resp]))
    print(offsets)
   
    train_hex[,targets[resp]]  <- log(train_hex[,targets[resp]]-offsets[resp])
    train_hex1[,targets[resp]] <- log(train_hex1[,targets[resp]]-offsets[resp])
    train_hex2[,targets[resp]] <- log(train_hex2[,targets[resp]]-offsets[resp])
    train_hex3[,targets[resp]] <- log(train_hex3[,targets[resp]]-offsets[resp])
    train_hex4[,targets[resp]] <- log(train_hex4[,targets[resp]]-offsets[resp])
    train_hex5[,targets[resp]] <- log(train_hex5[,targets[resp]]-offsets[resp])
  }
}

## Parameters for run
validation = F ## use cross-validation to determine best model parameters
grid = F ## do a grid search
submit = T ## whether to create a submission 
submission = 35 ## submission index
blend = T

## Settings
n_loop <- 5
n_fold <- 10
ensemble = (n_loop > 1) # only used if blend = F and submit = T

## Train a DL model
errs = 0

cv_preds <- matrix(0, nrow = nrow(train_hex), ncol = 1)
holdout_valid_se <- matrix(0, nrow = 1, ncol = length(targets))
holdout_valid_mse <- matrix(0, nrow = 1, ncol = length(targets))
for (resp in 2:2) { #length(targets)) {
  
  ## Clear H2O Cluster
  
  library(stringr)
  
  ls_temp <- h2o.ls(localH2O)
  for (n_ls in 1:nrow(ls_temp)) {
    if (str_detect(ls_temp[n_ls, 1], "DeepLearning")) {
      h2o.rm(localH2O, keys = as.character(ls_temp[n_ls, 1]))
    } else if (str_detect(ls_temp[n_ls, 1], "GLM")) {
      h2o.rm(localH2O, keys = as.character(ls_temp[n_ls, 1]))
    } else if (str_detect(ls_temp[n_ls, 1], "GBM")) {
      h2o.rm(localH2O, keys = as.character(ls_temp[n_ls, 1]))
    } else if (str_detect(ls_temp[n_ls, 1], "Last.value")) {
      h2o.rm(localH2O, keys = as.character(ls_temp[n_ls, 1]))
    }
  }
  
  
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
                         force_load_balance=F,
                         single_node_mode=F,
                         activation="Rectifier", hidden = c(300,300,300), epochs=1000, l1 = 1e-5, l2 = 0, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 50000
  #                       activation="RectifierWithDropout", hidden_dropout_ratios = c(0.1,0.1), 
  #                       hidden = c(100,100), epochs = 200, l1 = 0, l2 = 1e-5, rho = 0.95, epsilon = 1e-6, train_samples_per_iteration = 1000
                          #activation="Rectifier", hidden = c(20,20,20), epochs = 200, l1 = 0, l2 = 0, rho = 0.99, epsilon = 1e-8, max_w2 = 1, train_samples_per_iteration = 1000 
                       #  activation="Rectifier", hidden = c(800,800,800),epochs = 200, l1 = 1e-5, l2 = 0, rho = 0.95, epsilon = 1e-6, train_samples_per_iteration = 1000
                         #activation="Rectifier", hidden = list(c(200,200), c(50,50,50)), epochs = 200, l1 = c(0,1e-5), l2 =c(0,1e-5), rho = c(0.95,0.99), epsilon = c(1e-6,1e-8), max_w2 = 1, train_samples_per_iteration = 1000
        )
      print(gridmodel)
      
      # print grid search results to file
      sink(paste0(path_output, "/submission_", submission, "_", targets[resp], "_grid"))
      print(gridmodel)
      sink()
      
      errs = errs + sqrt(gridmodel@sumtable[[1]]$prediction_error)
    } # grid
    else {
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
                         force_load_balance=F,
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
    } #no grid
  } #validation
  
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
        ## Blending - Training with validation and early stopping
        
        ## Loops
        for (n in 1:n_loop) {
          
          ## CV

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
            
            #train_data <- train_hex
            if (nn%%5==1) train_data <- train_hex1
            if (nn%%5==2) train_data <- train_hex2
            if (nn%%5==3) train_data <- train_hex3
            if (nn%%5==4) train_data <- train_hex4
            if (nn%%5==0) train_data <- train_hex5
            
            if (nn<=5) {
              splits <- h2o.splitFrame(train_data, ratios = 1.-1./n_fold, shuffle=T)
              train <- splits[[1]]
              valid <- splits[[2]]
            } else {
              splits <- h2o.splitFrame(train_data, ratios = 1./n_fold, shuffle=T)
              train <- splits[[2]]
              valid <- splits[[1]]
            }
            train_resp <- train[,targets[resp]]
            valid_resp <- valid[,targets[resp]]
            
            # build final model blend components with validated parameters, do early stopping based on validation error
            model <- h2o.deeplearning(x = predictors, y = targets[resp], key = paste0(targets[resp], submission, "_blend_", n , "_", nn), 
                                      data = train,
                                      validation = valid,
                                      classification = F, 
                                      score_training_samples = 0,
                                      score_validation_samples = 0,
                                      score_duty_cycle = 1,
                                      score_interval = 1e-1,
                                      force_load_balance=F,
                                      activation = p$activation, input_dropout_ratio = p$input_dropout_ratio, hidden = p$hidden, epochs = p$epochs, l1 = p$l1, l2 = p$l2, max_w2 = p$max_w2, train_samples_per_iteration = p$train_samples_per_iteration)
            
            ## Use the model and store results
            train_preds <- h2o.predict(model, train)
            valid_preds <- h2o.predict(model, valid)
            test_preds <- h2o.predict(model, test_hex)
            
            # transform back
            if (transform[resp]) {
              train_preds = offsets[resp] + exp(train_preds)
              valid_preds = offsets[resp] + exp(valid_preds)
              test_preds = offsets[resp] + exp(test_preds)

              train_resp = offsets[resp] + exp(train_resp)
              valid_resp = offsets[resp] + exp(valid_resp)
            }
            
            ## Store
            if ((n == 1) & (nn == 1)) {
              test_preds_blend <- test_preds
            } else {
              test_preds_blend <- cbind(test_preds_blend, test_preds[, 1])
            }
            
            print(head(test_preds_blend))
            print(var(test_preds_blend))
            msetrain <- h2o.exec(localH2O,expr=mean((train_preds - train_resp)^2))
            sevalid <- h2o.exec(localH2O,expr=(valid_preds - valid_resp)^2)
            msevalid <- h2o.exec(localH2O,expr=mean(sevalid))
            holdout_valid_se[resp] <- holdout_valid_se[resp] + h2o.exec(localH2O,expr=sum(sevalid))
            
            print(model)
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
          } # nn nfolds
        } # n loops
        holdout_valid_se[resp] <- holdout_valid_se[resp]/n_loop
        holdout_valid_mse[resp] <- holdout_valid_se[resp]/nrow(train_hex) ## total number of (cross-)validation rows = # training rows
        cat("\nOverall MSE on holdout validation dataset for", targets[resp], ":", holdout_valid_mse[resp], "\n")
      } # blend
      else {
        if (!ensemble) {
          # no blending

          # build one single model on full training data with validated parameters
          model <- h2o.deeplearning(x = predictors, y = targets[resp], key = paste0(targets[resp], submission, "_cv"), data = train_hex, classification = F, score_training_samples = 0, force_load_balancing = F,
                                    activation = p$activation, input_dropout_ratio = p$input_dropout_ratio, hidden = p$hidden, epochs = p$epochs, l1 = p$l1, l2 = p$l2, max_w2 = p$max_w2, train_samples_per_iteration = p$train_samples_per_iteration)
          test_preds <- as.data.frame(h2o.predict(model, test_hex))
          test_preds_blend <- matrix(test_preds[, 1], ncol = 1)

        } #no ensemble
        else {
          for (n in 1:n_loop) {
            #ensemble model on full training data without holdout validation
            model <- h2o.deeplearning(x = predictors, y = targets[resp], key = paste0(targets[resp], submission, "_cv_ensemble_", n, "_of_", n_loop), data = train_hex, classification = F, score_training_samples = 0, force_load_balancing = F,
                                      activation = p$activation, input_dropout_ratio = p$input_dropout_ratio, hidden = p$hidden, epochs = p$epochs, l1 = p$l1, l2 = p$l2, max_w2 = p$max_w2, train_samples_per_iteration = p$train_samples_per_iteration)

            test_preds <- as.data.frame(h2o.predict(model, test_hex))

            # transform back
            if (transform[resp]) {
              test_preds = offsets[resp] + exp(test_preds)
            }

            if (n == 1) {
              test_preds_blend <- matrix(test_preds[, 1], ncol = 1)
            } else {
              test_preds_blend <- cbind(test_preds_blend, matrix(test_preds[, 1], ncol = 1))
            }
          }
        } #ensemble
      } #no blend
    } #validation
    else { #no validation
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
            
            if (nn%%5==1) train_data <- train_hex1
            if (nn%%5==2) train_data <- train_hex2
            if (nn%%5==3) train_data <- train_hex3
            if (nn%%5==4) train_data <- train_hex4
            if (nn%%5==0) train_data <- train_hex5

            if (nn<=5) {
              splits <- h2o.splitFrame(train_data, ratios = 1.-1./n_fold, shuffle=T)
              train <- splits[[1]]
              valid <- splits[[2]]
            } else {
              splits <- h2o.splitFrame(train_data, ratios = 1./n_fold, shuffle=T)
              train <- splits[[2]]
              valid <- splits[[1]]
            }
            train_resp <- train[,targets[resp]]
            valid_resp <- valid[,targets[resp]]

            # build final model blend components with hardcoded parameters, early stopping based on validation error
            if (resp == 1) #Ca
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
                                        activation="Rectifier", hidden = c(300,300), epochs = c(100), l1 = c(0), l2 = c(1e-6), rho = c(0.90), epsilon = c(1e-8) #0.08584288 5x10-fold
                                        #activation="Rectifier", hidden = c(300,300,300), epochs = 1000, l1 = 1e-5, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 100000
                                        #activation="Rectifier", hidden = c(800,800,800), epochs = 50, l1 = 1e-5, l2 = 0, rho = 0.95, epsilon = 1e-6, train_samples_per_iteration = 1000  #0.10
                                        #activation="Rectifier", hidden = c(100,100,100), epochs = 100, l1 = 1e-5, l2 = 0, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 5000 #0.10
                                        #activation="Rectifier", hidden = c(800,800,800),epochs = 200, l1 = 1e-5, l2 = 0, rho = 0.95, epsilon = 1e-6, train_samples_per_iteration = 1000     #0.08
                                        #activation="Rectifier", hidden = c(20,20,20), epochs = 200, l1 = 0, l2 = 0, rho = 0.99, epsilon = 1e-8, max_w2 = 1, train_samples_per_iteration = 1000  #0.10 nice   
                                      )
            else if (resp == 2) #P
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
                                        activation="Rectifier", 
                                        hidden = c(300,300), epochs = c(100), l1 = c(1e-4), l2 = c(0), rho = c(0.95), epsilon = c(1e-8) #0.8234492 5x10-fold
                                        #hidden = c(300,300,300,300), epochs = 1000, l1 = 1e-5, l2 = 0, rho = 0.95, epsilon = 1e-6, train_samples_per_iteration = 5000
                                        )
            
            else if (resp == 3) #pH
              model <- h2o.glm(x = predictors, y = targets[resp], data=train, nfolds=10, family="gaussian", lambda_search=F) #0.109 10-fold
           
#               model <- h2o.deeplearning(x = predictors, y = targets[resp], key = paste0(targets[resp], submission, "_blend_", n , "_", nn),
#                                         data = train,
#                                         validation = valid,
#                                         classification = F,
#                                         score_training_samples = 0,
#                                         score_validation_samples = 0,
#                                         score_duty_cycle = 1,
#                                         score_interval = 0.1,
#                                         force_load_balance=F,
#                                         override_with_best_model=T,
#                                         activation="Rectifier",
#                                         hidden = c(100,100,100), epochs = 100, l1 = 0, l2 = 1e-5, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 5000
#                                         #hidden = c(300,300), epochs = c(100), l1 = c(0), l2 = c(1e-6), rho = c(0.95), epsilon = c(1e-8) #0.25 5x10-fold
#                                         #hidden = c(300,300,300), epochs = 50, l1 = 1e-5, l2 = 0, rho = 0.95, epsilon = 1e-6, train_samples_per_iteration = 1000
#                                         )
            else if (resp == 4) #SOC
              #model <- h2o.glm(x = predictors, y = targets[resp], data=train, nfolds=10, family="gaussian", lambda_search=F) #0.128
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
                                        activation="Rectifier",
                                        hidden = c(300,300), epochs = c(100), l1 = c(1e-6), l2 = c(0), rho = c(0.90), epsilon = c(1e-6) #0.09721899 5x10-fold
                                        #hidden = c(300,300,300), epochs = 50, l1 = 1e-5, l2 = 0, rho = 0.95, epsilon = 1e-6, train_samples_per_iteration = 1000
                                        )
             else if (resp == 5) #Sand
#               model <- h2o.glm(x = predictors, y = targets[resp], data=train, nfolds=10, family="gaussian", lambda_search=F) # 0.116

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
                                        activation="Rectifier",
                                        #hidden = c(300,300,300), epochs = 50, l1 = 1e-5, l2 = 0, rho = 0.95, epsilon = 1e-6, train_samples_per_iteration = 1000
                                        hidden = c(300,300), epochs = c(100), l1 = c(1e-4), l2 = c(1e-4), rho = c(0.90), epsilon = c(1e-8) #0.1120842 5x10-fold
                                        )

            ## Use the model and store results
            train_preds <- h2o.predict(model, train)
            valid_preds <- h2o.predict(model, valid)
            test_preds <- h2o.predict(model, test_hex)
            
            # transform back
            if (transform[resp]) {
              train_preds = offsets[resp] + exp(train_preds)
              valid_preds = offsets[resp] + exp(valid_preds)
              test_preds = offsets[resp] + exp(test_preds)

              train_resp = offsets[resp] + exp(train_resp)
              valid_resp = offsets[resp] + exp(valid_resp)
            }
            ## For blending, stitch together the predictions for the validation holdout, after n_folds models are done, this will be the predictions on the full training data and can be scored
            ## For each n_loop, there's a new vector in this frame, so we can blend it together with GLM
            #blend_holdout_train_preds[,n] <- rbind(blend_holdout_train_preds[,n], valid_preds)
            #blend_holdout_train_preds
            
            ## Store
            if ((n == 1) & (nn == 1)) {
              test_preds_blend <- test_preds
            } else {
              test_preds_blend <- cbind(test_preds_blend, test_preds[, 1])
            }
            print(head(test_preds_blend))
            print(var(test_preds_blend))
            msetrain <- h2o.exec(localH2O,expr=mean((train_preds - train_resp)^2))
            sevalid <- h2o.exec(localH2O,expr=(valid_preds - valid_resp)^2)
            msevalid <- h2o.exec(localH2O,expr=mean(sevalid))
            holdout_valid_se[resp] <- holdout_valid_se[resp] + h2o.exec(localH2O,expr=sum(sevalid))
            
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
          } # nn fold
        } # n loop
        holdout_valid_se[resp] <- holdout_valid_se[resp]/n_loop
        holdout_valid_mse[resp] <- holdout_valid_se[resp]/nrow(train_hex) ## total number of (cross-)validation rows = # training rows
        cat("\nOverall", n_fold, "-fold cross-validated MSE on training dataset for", targets[resp], ":", holdout_valid_mse[resp], "\n")
        
        #cat("\nStitched together (cross-check, used for blending):", mean((blend_holdout_train_preds - y)^2), "\n")
        
      } #blend
      else { # no blend
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
          test_preds <- h2o.predict(model, test_hex)

          # transform back
          if (transform[resp]) {
            test_preds = offsets[resp] + exp(test_preds)
          }
          
          test_preds_blend <- test_preds
        } # no ensemble
        else {
          for (n in 1:n_loop) {
            #ensemble model on full training data without holdout validation
            if (resp == 1 | resp == 5 | resp == 4)
            model <- h2o.deeplearning(x = predictors, y = targets[resp], key = paste0(targets[resp], submission, "_ensemble", n, "_of_", n_loop),
                                      data = train_hex,
                                      classification = F,
                                      score_training_samples = 0,
                                      score_validation_samples = 0,
                                      score_duty_cycle = 0,
                                      force_load_balance=F,
                                      override_with_best_model=T,
                                      activation="Rectifier", hidden = c(100,100,100), epochs = 1000, l1 = 1e-5, l2 = 0, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 5000)
            else
              model <- h2o.deeplearning(x = predictors, y = targets[resp], key = paste0(targets[resp], submission, "_ensemble", n, "_of_", n_loop),
                                        data = train_hex,
                                        classification = F,
                                        score_training_samples = 0,
                                        score_validation_samples = 0,
                                        score_duty_cycle = 0,
                                        force_load_balance=F,
                                        override_with_best_model=T,
                                        activation="Rectifier", hidden = c(100,100,100), epochs = 1000, l1 = 0, l2 = 1e-5, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 5000)


            test_preds <- h2o.predict(model, test_hex)

            # transform back
            if (transform[resp]) {
              test_preds = offsets[resp] + exp(test_preds)
            }

            if (n == 1) {
              test_preds_blend = test_preds
            } else {
              test_preds_blend <- cbind(test_preds_blend, test_preds[,1])
            }
          }
        } #ensemble
      } #no blend
    } #no validation
    
    ## Now create submission
    print(model)
    if (!blend) {
      sink(paste0(path_output, "/submission_", submission, "_", targets[resp], "_score"))
      print(model)
      sink()
    } else {
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
    
    ## Make predictions (bring them to R)
    if (blend) cat("\nBlending results\n") 
    else if (ensemble) cat("\nEnsemble average\n")
    
    cat (paste0("\n Number of models: ",ncol(test_preds_blend)))
    yy_test_avg <- matrix("test_target", nrow = nrow(test_preds_blend), ncol = 1)
    yy_test_avg <- rowMeans(as.data.frame(test_preds_blend)) ### TODO: Use GLM to find best blending factors based on yy_fulltrain_all?
    pred <- as.data.frame(yy_test_avg)
    
    colnames(pred)[1] <- targets[resp]
    if (resp == 1) {
      preds <- cbind(as.data.frame(test_hex[,1]), pred)
    } else {
      preds <- cbind(preds, pred)
    }
    print(head(preds))
    ## Save the model as well
    #h2o.saveModel(object = model, dir = path_output, name = paste0("submission_", submission, targets[resp]), force = T)
    if (submit) {
      ## Write partial submission
      write.csv(preds, file = paste0(path_output, "/submission_partial_", submission, ".csv"), quote = F, row.names=F)
    }
  } #submit
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
#Overall 5 -fold cross-validated MSE on training dataset: 0.1000461 0.7913372 0.161784 0.06879418 0.1249119 # submission 23, should be 0.4447, scored 0.43 as 10-ensemble (same as submission 22 on holdout blend) -> consistent!
#Overall 5 -fold cross-validated MSE on training dataset: 0.1021683 0.7653759 0.1610202 0.09344637 0.1232382 #submission 24, should be 0.45 (5-blend), scored 0.433 on 50-ensemble
#Overall 5 -fold cross-validated MSE on training dataset: 0.0982809 0.7687449 0.1532805 0.06295989 0.1241085 #again 24, should be 0.437 (5-blend)
#Overall 5 -fold cross-validated MSE on training dataset: 0.106878 0.9372732 0.1225303 0.0867318 0.1176422 #GLM 10-fold CV no lambda search baseline: 0.4565
#Overall 5 -fold cross-validated MSE on training dataset: 0.09116863 1.079429 0.1673652 0.07868158 0.1491798 #submission 26, same as 24, rotated train shuffles, cv score 0.4833481
#Overall 5 -fold cross-validated MSE on training dataset: 0.07162816 1.150291 0.1680577 0.08794933 0.1489059 #submission 27, same as 26, rotated one more, cv score 0.486
#Overall 5 -fold cross-validated MSE on training dataset: 0.1125352 0.8863004 0.1437713 0.07582662 0.132544 #submission 28, with log-transform for P, cv score 0.459
#Overall 5 -fold cross-validated MSE on training dataset: 0.08975365 0.1575689 0.1751598 0.07799052 0.1228687 # submission 31, same as submission 20, but with 5x 5-fold cv, shuffled datasets, cv score: 0.3489707, scored 0.58!!
#Overall 5 -fold cross-validated MSE on training dataset: 0.09185992 0.8122152 0.1776434 0.06928164 0.1204961 # submission 31 again, had log-transformed P mixed up, cv score 0.4472
#Try: log-transform for P.  Goes from 0.812 to 0.893 -> Didn't help!
#Try: #submission 1+32: Mix of submission 1 (all but P) and submission 32: 1000 epochs for P instead of 50 and run on 10 nodes, MSE for P goes from 0.812 to 0.76924 -> helps a little
#Overall 5x10-fold cross-validated MSE on training dataset: 0.08584288 0.8234492 0.2522184 0.09721899 0.1120842 #submission 33, every 5-th point, and take winning parameters from grid search for 300,300 with 100 epochs, 10-fold cv CMRMSE: 0.4698467 LB: 0.44366
#Overall 5x10-fold cross-validated MSE on training dataset: 0.08584288 0.8234492 0.1090233 0.09721899 0.1120842 #submission 34, same as 33, but use glm for pH cv score: 0.4354 RMSE: 0.2929896 0.9074410 0.3301868 0.3117996 0.3347898 LB: 0.45977
# pH using GLM: 0.1090233
#Overall 5x10-fold cross-validated MSE on training dataset: 0.08584288 0.8167192 0.1090233 0.09721899 0.1120842 #submission 35, same as 34, but use log for P cv score: 0.4346 RMSE: 0.2929896 0.9037252 0.3301868 0.3117996 0.3347898 LB: 0.47375


#GOAL: 1/5*(sqrt(0.06)+sqrt(0.64)+sqrt(0.15)+sqrt(0.07)+sqrt(0.09))
#CURRENT 0.42: 1/5*(sqrt(0.10)+sqrt(0.77)+sqrt(0.122)+sqrt(0.063)+sqrt(0.117))
# 
# install.packages("e1071")
# library(e1071)
# 
# train <- read.csv("./training.csv",header=TRUE,stringsAsFactors=FALSE)
# test <- read.csv("./sorted_test.csv",header=TRUE,stringsAsFactors=FALSE)
# 
# submission <- test[,1]
# 
# labels <- train[,c("Ca","P","pH","SOC","Sand")]
# 
# train <- train[,2:3579]
# test <- test[,2:3579]
# 
# svms <- lapply(1:ncol(labels),
#                function(i)
#                {
#                  svm(train,labels[,i],cost=10000,scale=FALSE)
#                })
# 
# predictions <- sapply(svms,predict,newdata=test)
# 
# colnames(predictions) <- c("Ca","P","pH","SOC","Sand")
# submission <- cbind(PIDN=submission,predictions)
# 
# write.csv(submission,"beating_benchmark.csv",row.names=FALSE,quote=FALSE)