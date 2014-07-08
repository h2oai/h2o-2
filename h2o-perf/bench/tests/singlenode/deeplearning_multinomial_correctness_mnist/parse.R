source("../../../R/h2oPerf/prologue.R")

data_source <<- "home-0xdiag-datasets"

trainData   <<-  "/home/0xdiag/datasets/mnist/mnist_training.csv.gz"
num_train_rows <<- 49749
num_explan_cols <<- 784 
response <<- "C1"
upload("parsed.hex", trainData)

testData    <<- "/home/0xdiag/datasets/mnist/mnist_testing.csv.gz"

upload("test.hex", testData)

source("../../../R/h2oPerf/epilogue.R")
