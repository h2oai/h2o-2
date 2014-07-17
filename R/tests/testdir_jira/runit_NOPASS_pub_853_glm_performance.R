##
# Testing glm modeling performance with Arcene and Gisette datasets with and without strong rules. 
##


setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')


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

    print("Check that modeling with additional columns takes more time to compute without strong rules, ie doesn't quit too early.")
        # looks at total elapsed time
        stopifnot(time.noSR.3000[3] <= time.noSR.3250[3])

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

    print("Reading in Gisette data for gaussian modeling.")
        gisette.train = h2o.uploadFile(conn, locate("smalldata/gisette/Gisette_train_data.csv.gzip"), key="gisette.train", header=FALSE)
        gisette.train.label = h2o.uploadFile(conn, locate("smalldata/gisette/Gisette_train_labels.csv.gzip"), key="gisette.train.label", header=FALSE)
        gisette.train.full = h2o.assign(data=(cbind(gisette.train,gisette.train.label)),key="gisette.train.full")
    print("Head of Gisette data: ")
        head(gisette.train.full)
    print("Dimension of Gisette data: ")
        dim(gisette.train.full)

    print("Reading in Gisette validation data.")
        gisette.valid = h2o.uploadFile(conn, locate("smalldata/gisette/Gisette_valid_data.csv.gzip"), key="gisette.valid", header=FALSE)
        gisette.label = h2o.uploadFile(conn, locate("smalldata/gisette/Gisette_valid_labels.csv.gzip"), key="gisette.label", header=FALSE)
        gisette.valid.label = h2o.assign(data=ifelse(gisette.label==1,1,0), key="gisette.valid.label")
        gisette.valid.full = h2o.assign(data=(cbind(gisette.valid,gisette.valid.label)),key="gisette.valid.full")
    print("Head of gisette validation data: ")
        head(gisette.valid.full)
    print("Dimension of gisette validation data: ")
        dim(gisette.valid.full)
  
    print("Run model on 4500 columns of Gisette with strong rules on.")
        time.SR.4500 <- system.time(model.SR.4500 <- h2o.glm(x=c(1:4500), y="gisette.train.label", data=gisette.train.full, family="gaussian", lambda_search=TRUE, alpha=1, use_all_factor_levels=1, nfolds=0, higher_accuracy=TRUE))
        time.SR.4500

    print("Run model on all 5000 columns of Gisette with strong rules on.")
        time.SR.5000 <- system.time(model.SR.5000 <- h2o.glm(x=c(1:5000), y="gisette.train.label", data=gisette.train.full, family="gaussian", lambda_search=TRUE, alpha=1, use_all_factor_levels=1, nfolds=0, higher_accuracy=TRUE))
        time.SR.5000

    print("Test models on validation set.")
        predict.SR.4500 <- h2o.predict(model.SR.4500, gisette.valid.full)
        predict.SR.3250 <- h2o.predict(model.SR.3250, gisette.valid.full)

    print("Check performance of predictions.")
        perf.SR.4500 <- h2o.performance(predict.SR.4500$"1", gisette.valid.full$"gisette.valid.label")
        auc.SR.4500 <- perf.SR.4500@model$auc
        print(auc.SR.4500)
        perf.SR.5000 <- h2o.performance(predict.SR.5000$"1", gisette.valid.full$"gisette.valid.label")
        auc.SR.5000 <- perf.SR.5000@model$auc
        print(auc.SR.5000)
        
    print("Check that prediction AUC better than guessing (0.5).")
        stopifnot(auc.noSR.4500 > 0.5)
        stopifnot(auc.noSR.5000 > 0.5)

  
  testEnd()
}

doTest("Testing glm modeling performance with Arcene and Gisette with and without strong rules", test)