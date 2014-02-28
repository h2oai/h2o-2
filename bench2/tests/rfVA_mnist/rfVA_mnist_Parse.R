source("../../R/h2oPerf/prologue.R")

data_source <<- "smalldata"
trainData   <<- "~/master/h2o/smalldata/mnist/train.csv.gz"
response <<- "C785"
upload.VA("parsed.hex", trainData)

testData    <<- "~/master/h2o/smalldata/mnist/test.csv.gz"
upload.VA("test.hex", testData)

source("../../R/h2oPerf/epilogue.R")
