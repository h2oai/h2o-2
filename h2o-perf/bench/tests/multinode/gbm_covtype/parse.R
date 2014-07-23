source("../../../R/h2oPerf/prologue.R")

data_source <<- "home-0xdiag-datasets"

trainData   <<-  "/home/0xdiag/datasets/standard/covtype20x.data"
response <<- "C55"

num_train_rows  <<- 11620240
num_explan_cols <<- 54

upload("parsed.hex", trainData)

testData    <<-  trainData
upload("test.hex", testData)

source("../../../R/h2oPerf/epilogue.R")
