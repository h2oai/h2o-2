#----------------------------------------------------------------------
# Purpose:  This test exercises the DeepLearning model downloaded as java code
#           for the iris data set.
#
# Notes:    Assumes unix environment.
#           curl, javac, java must be installed.
#           java must be at least 1.6.
#----------------------------------------------------------------------

options(echo=FALSE)
TEST_ROOT_DIR <- ".."
setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source(paste(TEST_ROOT_DIR, "findNSourceUtils.R", sep="/"))


#----------------------------------------------------------------------
# Parameters for the test.
#----------------------------------------------------------------------

train <- locate("smalldata/iris/iris_train.csv")
test <- locate("smalldata/iris/iris_test.csv")
x = c("sepal_len","sepal_wid","petal_len","petal_wid");
y = "species"
classification = T


#----------------------------------------------------------------------
# Run the tests
#----------------------------------------------------------------------

balance_classes = T
source('../Utils/shared_javapredict_DL.R')

balance_classes = F
source('../Utils/shared_javapredict_DL.R')

classification = F
x = c("sepal_len","sepal_wid","petal_len")
y = c("petal_wid")
source('../Utils/shared_javapredict_DL.R')
