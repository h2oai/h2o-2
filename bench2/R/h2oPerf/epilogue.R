##                                                      ##
#   This epilogue file contains "public" and "private"   #
#   R methods. The public methods are responsible for    #
#   handling the emission of the various results from    #
#   each phase (e.g. Parse is a phase).                  #
#                                                        # 
#   "Private" function declarations begin with a '.'     #
##                                                      ##
end_time <<- round(System$currentTimeMillis())[[1]]
correct_pass <<- 1
time_pass <<- 1
passed <<- ifelse(correct_pass && time_pass, 1, 0)


#"Private" Methods
.dataSources<-
function() {
  train_data_url <<- trainData
  test_data_url  <<- testData
  data_name      <<- basename(trainData)
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
  save(response, predict_type, trainData, testData, model, h, confusion_matrix, IP, PORT, file = ".RData")
  q("no")
}

.emitResults<-
function() {
  .emitPhaseResults()
  cat("\n\n")
  if(phase == "parse") {
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
                list(dataset_name = data_name,
                     dataset_source = data_source,
                     train_dataset_url = train_data_url,
                     test_dataset_url = test_data_url))
  .coda("PARSE", r)
}

.emitKMeansResults<-
function() {
  r <- list(kmeans_result = 
                list(k = kmeans_k,
                     withinss = kmeans_withinss ))
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
                list(aic = aic,
                     null_deviance = null_dev,
                     residual_deviance = res_dev))
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
  r <- list(binomial_result = 
                list(auc = auc,
                     precision_ = precision[[1]],
                     recall = recall[[1]],
                     error_rate = error_rate[[1]],
                     minority_error_rate = minority_error_rate[[1]]))
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
                               level_error_rate = level_error_rate)
  }
  return(l)
}

.emitMultinomResults<-
function() {
  r <- list(multinomial_result = .confusionMatrix())
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

