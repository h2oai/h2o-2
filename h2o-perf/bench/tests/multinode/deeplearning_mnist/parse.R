source("../../../R/h2oPerf/prologue.R")

data_source <<- "smalldata"

#trainData   <<-  "/Users/spencer/master/h2o/smalldata/mnist/train.csv.gz"
#trainData    <<- "/home/0xdiag/datasets/mnist/mnist_training.csv.gz"
trainData <<- "/home/0xdiag/datasets/mnist/train.csv.gz"
response <<- "C785"

num_train_rows  <<- 50690
num_explan_cols <<- 784

upload.FV("parsed.hex", trainData)

#testData    <<- "/Users/spencer/master/h2o/smalldata/mnist/test.csv.gz"
#testData     <<- "/home/0xdiag/datasets/mnist/mnist_testing.csv.gz"
testData     <<- "/home/0xdiag/datasets/mnist/test.csv.gz"
upload.FV("test.hex", testData)

source("../../../R/h2oPerf/epilogue.R")
