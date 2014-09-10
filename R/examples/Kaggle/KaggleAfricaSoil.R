if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }
install.packages("h2o", repos=(c("file:///Users/arno/h2o/target/R", getOption("repos"))))

suppressMessages(library(h2o))
#localH2O <- h2o.init(ip="192.168.1.181", port = 53322)
localH2O <- h2o.init(max_mem_size = '5g') # using 5GB

# Import data
path_cloud <- "/Users/arno/"
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
validation = T
submit = F
submission = 4

## Train a DL model
errs = 0
for (resp in 1:length(targets)) {
  if (validation) {
    # run grid search with n-fold cross-validation
    gridmodel <- h2o.deeplearning(x = c(spectra_hi, spectra_low, extra),
                                  y = targets[resp],
                                  key = targets[resp],
                                  data = train_hex,
                                  nfolds = 5,
                                  classification = F,
                                  #activation=c("RectifierWithDropout"), input_dropout_ratio = c(0.2), hidden_dropout_ratios = list(c(0.5,0.5,0.5)), hidden = list(c(300,300,300), c(500,500,500)), epochs = c(100), l1 = c(0,1e-5), l2 = c(0,1e-5), max_w2 = 10, train_samples_per_iteration = 10000,
                                  #activation=c("Rectifier"), input_dropout_ratio = c(0), hidden_dropout_ratios = c(0,0,0), hidden = c(300,300,300), epochs = c(100,200), l1 = 1e-5, l2 = 0, max_w2 = 10, train_samples_per_iteration = 100000, # repro submission 1 as reference for CV score
                                  activation=c("Rectifier"), input_dropout_ratio = c(0), hidden_dropout_ratios = c(0,0,0), hidden = c(50,50), epochs = c(10), l1 = c(0,1e-5), l2 = c(0,1e-5), max_w2 = 10, train_samples_per_iteration = 10000,
                                  score_training_samples = 0,
                                  score_validation_samples = 0
    )
    print(gridmodel)
    
    # for non-grid model only:
    errs = errs + gridmodel@model$valid_sqr_error
  }
  
  if (submit) {
    if (validation) {
      # pick parameters from the best cross-validated model
      p <- gridmodel@sumtable[[1]]
      # build final model on full training data with best parameters
      model <- h2o.deeplearning(x = c(spectra_hi, spectra_low, extra), y = targets[resp], key = targets[resp], data = train_hex, classification = F, score_training_samples = 0,
                               activation = p$activation, input_dropout_ratio = p$input_dropout_ratio, hidden = p$hidden, epochs = p$epochs, l1 = p$l1, l2 = p$l2, max_w2 = p$max_w2, train_samples_per_iteration = p$train_samples_per_iteration)
    } else {
      # build final model on full training data with given parameters
      model <- h2o.deeplearning(x = c(spectra_hi, spectra_low, extra), y = targets[resp], key = targets[resp], data = train_hex, classification = F, score_training_samples = 0,
                                #activation="Rectifier", hidden = c(300,300,300), epochs = 1000, l1 = 1e-5, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 100000 #submission 1 - 0.42727
                                #activation="RectifierWithDropout",input_dropout_ratio = 0.2, hidden = c(500,500,500), epochs = 500, l1 = 1e-5, l2 = 1e-5, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 50000 #submission 2 - 0.684
                                activation="Rectifier", hidden = c(500,500,500), epochs = 1000, l1 = 1e-5, l2 = 1e-5, rho = 0.99, epsilon = 1e-8, max_w2 = 10, train_samples_per_iteration = 10000 #submission 3 - 0.45212
      )
    }
    print(model)
    
    #   model <- h2o.gbm(x = c(spectra_hi, spectra_low, extra),
    #                             y = targets[resp],
    #                             key = targets[resp],
    #                             data = train_hex,
    #                             distribution = "gaussian",
    #                             n.trees = 100,
    #                             shrinkage = 0.01
    #                             )
    
    ## Make predictions
    pred <- as.data.frame(h2o.predict(model, test_hex))
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

