source("../../R/h2oPerf/prologue.R")

data_source <<- "home-0xdiag-datasets"

trainData   <<-  "/home/0xdiag/datasets/covtype200x.data"
response <<- "C55"

num_train_rows  <<- 116202400
num_explan_cols <<- 54

upload.VA("parsed.hex", trainData)

testData    <<-  trainData
upload.VA("test.hex", testData)

source("../../R/h2oPerf/epilogue.R")
