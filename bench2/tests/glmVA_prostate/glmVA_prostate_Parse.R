source("../../R/h2oPerf/prologue.R")
data_source <<- "smalldata"
trainData   <<- "~/master/h2o/smalldata/logreg/prostate.csv"
response <<- "CAPSULE"
upload.VA("parsed.hex", trainData)

testData    <<- trainData
upload.VA("test.hex", testData)

source("../../R/h2oPerf/epilogue.R")
