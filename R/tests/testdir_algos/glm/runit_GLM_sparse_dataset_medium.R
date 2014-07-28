##
# Testing glm modeling performance with sparse Gisette dataset with and without strong rules. 
# Test for JIRA PUB-853 
# 'Early termination in glm resulting in underfitting'
##


setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')


test <- function(conn) {
    print("Reading in Gisette data for gaussian modeling.")
        gisette.train = h2o.uploadFile(conn, locate("smalldata/gisette/Gisette_train_data.csv.gzip"), key="gisette.train", header=FALSE)
        gisette.train.label = h2o.uploadFile(conn, locate("smalldata/gisette/Gisette_train_labels.csv.gzip"), key="gisette.train.label", header=FALSE)
        gisette.train.full = h2o.assign(data=(cbind(gisette.train,gisette.train.label)),key="gisette.train.full")
    print("Head of Gisette data: ")
        head(gisette.train.full)
    print("Dimension of Gisette data: ")
        print(dim(gisette.train.full))

    print("Reading in Gisette validation data.")
        gisette.valid = h2o.uploadFile(conn, locate("smalldata/gisette/Gisette_valid_data.csv.gzip"), key="gisette.valid", header=FALSE)
        gisette.label = h2o.uploadFile(conn, locate("smalldata/gisette/Gisette_valid_labels.csv.gzip"), key="gisette.label", header=FALSE)
        gisette.valid.label = h2o.assign(data=ifelse(gisette.label==1,1,0), key="gisette.valid.label")
        gisette.valid.full = h2o.assign(data=(cbind(gisette.valid,gisette.valid.label)),key="gisette.valid.full")
    print("Head of gisette validation data: ")
        head(gisette.valid.full)
    print("Dimension of gisette validation data: ")
        print(dim(gisette.valid.full))
  
    print("Run model on 4500 columns of Gisette with strong rules on.")
        time.SR.4500 <- system.time(model.SR.4500 <- h2o.glm(x=c(1:4500), y="gisette.train.label", data=gisette.train.full, family="gaussian", lambda_search=TRUE, alpha=1, use_all_factor_levels=1, nfolds=0, higher_accuracy=TRUE))
        print(time.SR.4500)

    print("Run model on all 5000 columns of Gisette with strong rules on.")
        time.SR.5000 <- system.time(model.SR.5000 <- h2o.glm(x=c(1:5000), y="gisette.train.label", data=gisette.train.full, family="gaussian", lambda_search=TRUE, alpha=1, use_all_factor_levels=1, nfolds=0, higher_accuracy=TRUE))
        print(time.SR.5000)

    print("Test models on validation set.")
        predict.SR.4500 <- h2o.predict(model.SR.4500, gisette.valid.full)
        predict.SR.5000 <- h2o.predict(model.SR.5000, gisette.valid.full)

    print("Check performance of predictions.")
        perf.SR.4500 <- h2o.performance(predict.SR.4500$"predict", gisette.valid.full$"gisette.valid.label")
        perf.SR.5000 <- h2o.performance(predict.SR.5000$"predict", gisette.valid.full$"gisette.valid.label")
  
  testEnd()
}

doTest("Testing glm modeling performance with Arcene and Gisette with and without strong rules", test)