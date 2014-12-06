sink("CTR.log", split = T)

## This code block is to install a particular version of H2O
# START
#if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
#if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }
#install.packages("h2o", repos=(c("http://s3.amazonaws.com/h2o-release/h2o/master/1597/R", getOption("repos")))) #choose a build here
# END

# Fetch the latest nightly build using Jo-fai Chow's package
#devtools::install_github("woobe/deepr")
#deepr::install_h2o()

library(h2o)
library(stringr)

## Connect to H2O server (On server(s), run 'java -Xmx64G -jar h2o.jar -port 53322 -name CTR -data_max_factor_levels 100000000' first)
## Go to http://server:53322/ to check Jobs/Data/Models etc.
h2oServer <- h2o.init(ip="mr-0xd1", port = 53322)
#h2oServer <- h2o.init(nthreads=-1, max_mem_size='64g')

## Helper function for feature engineering
h2o.addNewFeatures <- function(frame, timecol, intcols, factorcols, key) {
  cat("\nFeature engineering for time column.")
  hour <- frame[,timecol] %% 100
  colnames(hour) <- "hour"
  day <- ((frame[,timecol] - hour) %% 10000)/100
  colnames(day) <- "day"
  dow <- day %% 7
  colnames(dow) <- "dayofweek"
  frame <- cbind(frame[,-match(timecol,colnames(frame))], day, dow, hour, as.factor(day), as.factor(dow), as.factor(hour))
  frame <- h2o.assign(frame, key)
  h2o.rm(h2oServer, grep(pattern = "Last.value", x = h2o.ls(h2oServer)$Key, value = TRUE))
  
  cat("\nFeature engineering for integer columns.")
  newfactors <- c()
  for (int in intcols) {
    # turn integers into factors, keep top 100 levels
    trim_integer_levels <- h2o.interaction(as.factor(frame[,int]), factors = 1, pairwise = FALSE, max_factors = 100, min_occurrence = 1)
    newfactors <- c(newfactors, colnames(trim_integer_levels))
    frame <- cbind(frame, trim_integer_levels)
    frame <- h2o.assign(frame, key)
    h2o.rm(h2oServer, grep(pattern = "Last.value", x = h2o.ls(h2oServer)$Key, value = TRUE))
  }
  
  cat("\nFeature engineering for factor columns.")
  # create pair-wise interaction between factors, keep top 100 levels
  factor_interactions <- h2o.interaction(frame, factors = c(newfactors,factorcols), pairwise = TRUE, max_factors = 100, min_occurrence = 2)
  frame <- cbind(frame, factor_interactions)
  
  # Store frame under designated key
  frame <- h2o.assign(frame, key)
  
  h2o.rm(h2oServer, grep(pattern = "Last.value", x = h2o.ls(h2oServer)$Key, value = TRUE))
  frame
}

h2o.logLoss <- function(preds, resp) {
  tpc <- preds
  tpc <- h2o.exec(h2oServer,expr=ifelse(tpc > 1e-15, tpc, 1e-15))
  tpc <- h2o.exec(h2oServer,expr=ifelse(tpc < 1-1e-15, tpc, 1-1e-15))
  LL <- h2o.exec(h2oServer,expr=mean(-resp*log(tpc)-(1-resp)*log(1-tpc)))
  h2o.rm(h2oServer, grep(pattern = "Last.value", x = h2o.ls(h2oServer)$Key, value = TRUE))
  LL
}



### START
myseed=5816985749037550201
path <- "/home/arno/h2o-kaggle/ctr/"

path_submission <- paste0(path,"./data/sampleSubmission.csv")
path_train <- paste0(path,"./data/train")
path_test <- paste0(path,"./data/test")

cat("\nReading data.")
train_hex <- h2o.importFile(h2oServer, path = path_train)
test_hex <- h2o.importFile(h2oServer, path = path_test)

## Feature engineering

## all columns
#intcols <- c("C1","banner_pos","site_category","device_type","device_conn_type","C14","C15","C16","C17","C18","C19","C20","C21")
#factorcols <- c("site_id","site_domain","site_category","app_id","app_domain","app_category","device_id","device_ip","device_model")

intcols <- c("C1","banner_pos","site_category","device_type","device_conn_type","C14","C15","C16","C17","C18","C19","C20","C21")
factorcols <- c()

cat("\nAdding features")
train_hex <- h2o.addNewFeatures(train_hex, "hour", intcols, factorcols, "train.hex")
test_hex <- h2o.addNewFeatures(test_hex, "hour", intcols, factorcols, "test.hex")

## Split into train/validation based on training days (first 9 days: train, last day: test)
cat("\nSplitting into train/validation")
train <- h2o.assign(train_hex[train_hex$day<30,], 'train')
valid <- h2o.assign(train_hex[train_hex$day==30,], 'valid')
h2o.rm(h2oServer, grep(pattern = "Last.value", x = h2o.ls(h2oServer)$Key, value = TRUE))

cat("\nTraining H2O model on training/validation splits days <30/30")
## Note: This could be grid search models, after which you would obtain the best model with model <- cvmodel@model[[1]]
#cvmodel <- h2o.randomForest(data=train, validation=valid, x=c(3:ncol(train)), y=2,
#                           type="BigData", ntree=50, depth=20, seed=myseed)
cvmodel <- h2o.gbm(data=train, validation=valid, x=c(3:ncol(train)), y=2,
                   distribution="bernoulli", n.tree=100, interaction.depth=10)
#cvmodel <- h2o.deeplearning(data=train, validation=valid, x=c(3:ncol(train)), y=2,
#                            hidden=c(50,50), max_categorical_features=100000, train_samples_per_iteration=10000, score_validation_samples=10000)

train_resp <- train[,2] #actual label
train_preds <- h2o.predict(cvmodel, train)[,3] #[,3] is probability for class 1
cat("\nLogLoss on training data:", h2o.logLoss(train_preds, train_resp))

valid_resp <- valid[,2]
valid_preds <- h2o.predict(cvmodel, valid)[,3]
cat("\nLogLoss on validation data:", h2o.logLoss(valid_preds, valid_resp))

usefullmodel = T #Set to TRUE for higher accuracy 
if (usefullmodel) {
  cat("\nTraining H2O model on all the training data.")
#  fullmodel <- h2o.randomForest(data=train_hex, x=c(3:ncol(train)), y=2,
#                                type=cvmodel@model$params$type,
#                                ntree=cvmodel@model$params$ntree,
#                                depth=cvmodel@model$params$depth,
#                                seed=cvmodel@model$params$seed)
  fullmodel <- h2o.gbm(data=train_hex, x=c(3:ncol(train)), y=2,
                       distribution=cvmodel@model$params$distribution,n.tree=cvmodel@model$params$n.tree,
                       interaction.depth=cvmodel@model$params$interaction.depth)
#  fullmodel <- h2o.deeplearning(data=train_hex, x=c(3:ncol(train)), y=2,
#                                hidden=cvmodel@model$params$hidden,
#                                max_categorical_features=cvmodel@model$params$max_categorical_features,
#                                train_samples_per_iteration=cvmodel@model$params$train_samples_per_iteration,
#                                score_validation_samples=cvmodel@model$params$score_validation_samples)
  
  cat("\nMaking predictions on test data with model trained on full data.")
  pred <- h2o.predict(fullmodel, test_hex)[,3]
} else {
  cat("\nMaking predictions on test data with model trained on train/validation splits.")
  pred <- h2o.predict(cvmodel, test_hex)[,3]
}

submission <- read.csv(path_submission, colClasses = c("character"))
submission[,2] <- as.data.frame(pred)
colnames(submission) <- c("id", "click")
cat("\nWriting predictions on test data.")
write.csv(as.data.frame(submission), file = paste(path,"./submission.csv", sep = ''), quote = F, row.names = F)
sink()
