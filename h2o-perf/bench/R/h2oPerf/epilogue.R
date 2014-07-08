##                                                      ##
#   This epilogue file contains "public" and "private"   #
#   R methods. The public methods are responsible for    #
#   handling the emission of the various results from    #
#   each phase (e.g. Parse is a phase).                  #
#                                                        # 
#   "Private" function declarations begin with a '.'     #
##                                                      ##
.NaN = -1
end_time     <<- round(System$currentTimeMillis())[[1]]
#correct_pass <<- 1
time_pass    <<- 1
passed       <<- ifelse(correct_pass && time_pass, 1, 0)

#"Private" Methods
.dataSources<-
function() {
  train_data_url  <<- trainData
  test_data_url   <<- testData
  train_data_name <<- basename(trainData)
  test_data_name  <<- tryCatch(basename(testData), error = function(e) {return(-1)})
}

.dataSources()

.isNone<-
function(arg) {
  if (!is.character(arg)) {
    return(FALSE)
  }
  return(arg == "None")
}

.coda<-
function(p, r) {
  print(paste(p, " RESULTS JSON:", sep = ""))
  .to.json(r)
}

.to.json<-
function(r) {
  cat(toJSON(r), "\n")
}

.saveRData<-
function() {
  save(data_center, response, predict_type, trainData, testData, model, h, confusion_matrix, IP, PORT, file = ".RData")
  q("no")
}

.emitResults<-
function() {
  .emitPhaseResults()
  cat("\n\n")
  if(phase == "parse") {
    correct_pass <<- 1
    .emitParseResults()
    cat("\n\n")
  }
  if(phase == "model") {
    .emitModelResults()
    cat("\n\n")
  }
  if(phase == "predict") {
    .emitPredictResults()
    cat("\n\n")
  }
}

.emitPredictType<-
function() {
  cat("\n\n")
  .calcPredictType()
}

.emitPhaseResults<-
function() {
  r <- list(phase_result = 
                list(phase_name = phase,
                     start_epoch_ms = start_time,
                     end_epoch_ms = end_time,
                     timing_passed = time_pass,
                     correctness_passed = correct_pass,
                     passed = passed))
  .coda("PHASE", r)
}

.emitParseResults<-
function() {
  r <- list(parse_result = 
                list(train_dataset_name = train_data_name,
                     test_dataset_name = test_data_name,
                     dataset_source = data_source,
                     num_train_rows = num_train_rows,
                     num_explan_cols = num_explan_cols,
                     train_dataset_url = train_data_url,
                     datacenter = data_center,
                     test_dataset_url = test_data_url))
  .coda("PARSE", r)
}

.emitKMeansResults<-
function() {
  r <- list(kmeans_result = 
                list(k = kmeans_k,
                     withinss = ifelse(kmeans_withinss == "NaN", .NaN, kmeans_withinss )))
  .coda("KMEANS", r)
}

.emitModelResults<-
function() {
  r <- list(model_result = 
                list(model_json = model.json))
  .coda("MODEL", r)
  .emitKMeansResults()
}

.emitRegressionResults<-
function() {
  r <- list(regression_result = 
                list(aic = ifelse(aic == "NaN", .NaN, aic),
                     null_deviance = ifelse(null_dev == "NaN", .NaN, null_dev),
                     residual_deviance = ifelse(res_dec == "NaN", .NaN, res_dev)))
  .coda("REGRESSION", r)
}

.emitCM<-
function() {
  r <- list(cm_json = 
                list(levels_json = levels.json,
                     cm_json = cm.json,
                     representation = .representation))
  .coda("CM", r)
}

.emitBinomResults<-
function() {
  print("DEBUG")
  print("bin results here....")
  print(confusion_matrix)
  r <- list(binomial_result = 
                list(auc = ifelse(auc == "NaN", .Nan, auc),
                     preccision = ifelse(precision[[1]] == "NaN", .NaN, precision[[1]]),
                     recall = ifelse(recall[[1]] == "NaN", .NaN, recall[[1]]),
                     error_rate = ifelse(error_rate[[1]] == "NaN", .NaN, error_rate[[1]]),
                     errs = multinom_errs,
                     minority_error_rate = ifelse(minority_error_rate[[1]] == "NaN", .NaN, minority_error_rate[[1]])))
  .coda("BINOMIAL", r)
}

.confusionMatrix<-
function() {
  if(.isNone(confusion_matrix))
    return(list())

  num_classes <- dim(confusion_matrix)[1]
  l = list()
  for ( i in 1:(num_classes-1)) {
    level <- i
    level_actual_count <- sum(confusion_matrix[level,-num_classes])
    level_predicted_correctly_count <- diag(confusion_matrix)[level]
    level_error_rate = confusion_matrix[level,num_classes]
    l[[length(l) + 1]] <- list(level = level,
                               level_actual_count = level_actual_count,
                               level_predicted_correctly_count = level_predicted_correctly_count,
                               level_error_rate = ifelse(level_error_rate[[1]] == "NaN", .NaN, level_error_rate[[1]]))
  }
  return(l)
}

.emitMultinomResults<-
function() {
  r <- list(multinomial_result = .confusionMatrix()) #, errs = multinom_errs)
  .coda("MULTINOMIAL", r)
}

.emitPredictResults<-
function() {
  if (predict_type == "regression") {
    .emitRegressionResults()
  }
  cat("\n\n")
  if (predict_type %in% c("binomial", "multinomial")) {
    .emitCM()
  }
  cat("\n\n")
  if (predict_type == "binomial") {
    .emitBinomResults()
  }
  cat("\n\n")
  if (predict_type == "multinomial") {
    .emitMultinomResults()
  }
  cat("\n\n")
}
.calcPredictType()
.emitResults()
.saveRData()

