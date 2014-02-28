source("../../R/h2oPerf/prologue.R")

data_source <<- "home-0xdiag-datasets"

trainData   <<-  "/home/0xdiag/datasets/standard/covtype200x.data"
response <<- "C55"
upload.FV("parsed.hex", trainData)

testData    <<-  trainData
upload.FV("test.hex", testData)

source("../../R/h2oPerf/epilogue.R")
