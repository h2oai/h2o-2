source("../../../R/h2oPerf/prologue.R")

data_source <<- "home-0xdiag-datasets"

trainData   <<-  "../../../../../smalldata/mnist/train.csv.gz"
num_train_rows <<- 50000
num_explan_cols <<- 784 
response <<- "C785"
upload.FV("parsed.hex", trainData)

testData    <<-  "../../../../../smalldata/mnist/test.csv.gz"

upload.FV("test.hex", testData)

source("../../../R/h2oPerf/epilogue.R")
