##
# Testing glm consistency on 1 chunk dataset with and without shuffling rows.
##


setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

test <- function(conn) {
    print("Reading in Arcene training data for binomial modeling.")
        arcene.train = h2o.uploadFile(conn, locate("smalldata/arcene/arcene_train.data"), key="arcene.train", header=FALSE)
        arcene.label = h2o.uploadFile(conn, locate("smalldata/arcene/arcene_train_labels.labels"), key="arcene.label", header=FALSE)
        arcene.train.label = h2o.assign(data=ifelse(arcene.label==1,1,0), key="arcene.train.label")
        arcene.train.full = h2o.assign(data=(cbind(arcene.train,arcene.train.label)),key="arcene.train.full")
    print("Dimension of arcene training data: ")
        print(dim(arcene.train.full))
    
    print("Shuffle rows of dataset.")
		arcene.train.full2 = h2o.assign(arcene.train.full[sample(nrow(arcene.train.full),replace=F),],"arcene.train.full2")
	print("Dimension of shuffled arcene training data: ")
        print(dim(arcene.train.full))
    
    print("Create model on original Arcene dataset.")
    	h2o.model <- h2o.glm(x=c(1:10000), y="arcene.train.label", data=arcene.train.full, family="binomial", lambda_search=TRUE, alpha=0.5, nfolds=0, use_all_factor_levels=1)

    print("Create second model on original Arcene dataset.")
    	h2o.model2 <- h2o.glm(x=c(1:10000), y="arcene.train.label", data=arcene.train.full, family="binomial", lambda_search=TRUE, alpha=0.5, nfolds=0, use_all_factor_levels=1)

    print("Create model on shuffled Arcene dataset.")
    	h2o.model.s <- h2o.glm(x=c(1:10000), y="arcene.train.label", data=arcene.train.full2, family="binomial", lambda_search=TRUE, alpha=0.5, nfolds=0, use_all_factor_levels=1)

    print("Assert that number of predictors remaining and their respective coefficients are equal.")
    	print("Comparing 2 models from original dataset")
    		stopifnot(h2o.model@model$coefficients == h2o.model2@model$coefficients)
    	print("Comparing models from original and shuffled dataset")
    		stopifnot(h2o.model@model$coefficients == h2o.model.s@model$coefficients)

    testEnd()
}

doTest("Testing glm consistency on 1 chunk dataset with and without shuffling rows.", test)
