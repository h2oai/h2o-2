##
# Testing glm modeling performance with wide Arcene dataset with and without strong rules. 
# Test for JIRA PUB-853 
# 'Early termination in glm resulting in underfitting'
##


setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')


test <- function(conn) {
    print("Reading in Arcene training data for binomial modeling.")
        arcene.train = h2o.uploadFile(conn, locate("smalldata/arcene/arcene_train.data"), key="arcene.train", header=FALSE)
        arcene.label = h2o.uploadFile(conn, locate("smalldata/arcene/arcene_train_labels.labels"), key="arcene.label", header=FALSE)
        arcene.train.label = h2o.assign(data=ifelse(arcene.label==1,1,0), key="arcene.train.label")
        arcene.train.full = h2o.assign(data=(cbind(arcene.train,arcene.train.label)),key="arcene.train.full")
    print("Head of arcene training data: ")
        head(arcene.train.full)
    print("Dimension of arcene training data: ")
        dim(arcene.train.full)

    print("Reading in Arcene validation data.")
        arcene.valid = h2o.uploadFile(conn, locate("smalldata/arcene/arcene_valid.data"), key="arcene.valid", header=FALSE)
        arcene.label = h2o.uploadFile(conn, locate("smalldata/arcene/arcene_valid_labels.labels"), key="arcene.label", header=FALSE)
        arcene.valid.label = h2o.assign(data=ifelse(arcene.label==1,1,0), key="arcene.valid.label")
        arcene.valid.full = h2o.assign(data=(cbind(arcene.valid,arcene.valid.label)),key="arcene.valid.full")
    print("Head of arcene validation data: ")
        head(arcene.valid.full)
    print("Dimension of arcene validation data: ")
        dim(arcene.valid.full)
  
    print("Run model on 3000 columns of Arcene with strong rules off.")
        time.noSR.3000 <- system.time(model.noSR.3000 <- h2o.glm(x=c(1:3000), y="arcene.train.label", data=arcene.train.full, family="binomial", lambda_search=FALSE, alpha=1, nfolds=0, use_all_factor_levels=1, higher_accuracy=TRUE))
        print(time.noSR.3000)
  
    print("Run model on 3250 columns of Arcene with strong rules off.")
        time.noSR.3250 <- system.time(model.noSR.3250 <- h2o.glm(x=c(1:3250), y="arcene.train.label", data=arcene.train.full, family="binomial", lambda_search=FALSE, alpha=1, nfolds=0, use_all_factor_levels=1, higher_accuracy=TRUE))
        print(time.noSR.3250)

    #print("Check that modeling with additional columns takes more time to compute without strong rules, ie doesn't quit too early.")
        # looks at total elapsed time
        #stopifnot(time.noSR.3000[3] <= time.noSR.3250[3])

    print("Test models on validation set.")
        predict.noSR.3000 <- h2o.predict(model.noSR.3000, arcene.valid.full)
        predict.noSR.3250 <- h2o.predict(model.noSR.3250, arcene.valid.full)

    print("Check performance of predictions.")
        perf.noSR.3000 <- h2o.performance(predict.noSR.3000$"1", arcene.valid.full$"arcene.valid.label")
        auc.noSR.3000 <- perf.noSR.3000@model$auc
        print(auc.noSR.3000)
        perf.noSR.3250 <- h2o.performance(predict.noSR.3250$"1", arcene.valid.full$"arcene.valid.label")
        auc.noSR.3250 <- perf.noSR.3250@model$auc
        print(auc.noSR.3250)
        
    print("Check that prediction AUC better than guessing (0.5).")
        stopifnot(auc.noSR.3000 > 0.5)
        stopifnot(auc.noSR.3250 > 0.5)

  testEnd()
}

doTest("Testing glm modeling performance with wide Arcene dataset with and without strong rules", test)