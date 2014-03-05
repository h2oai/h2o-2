source("../../R/h2oPerf/prologue.R")

data_source <<- "home-0xdiag-datasets"

trainData   <<-  "/home/0xdiag/datasets/mnist/mnist8m/mnist8m-train-1.csv"
num_train_cols <<- 7000000
num_explan_cols <<- 784
response <<- "C1"
upload.VA("parsed.hex", trainData)

testData    <<-  "/home/0xdiag/datasets/mnist/mnist8m/mnist8m-test-1.csv"
upload.VA("test.hex", testData)

source("../../R/h2oPerf/epilogue.R")
