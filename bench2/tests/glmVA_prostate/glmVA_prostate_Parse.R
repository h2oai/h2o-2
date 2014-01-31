source("../../R/h2oPerf/prologue.R")

data_source <<- "smalldata"
trainData   <<- "~/master/h2o/smalldata/logreg/prostate.csv"
response <<- "CAPSULE"
upload.VA()

testData    <<- trainData
upload.VA()

source("../../R/h2oPerf/epilogue.R")
