source("../../R/h2oPerf/prologue.R")
data_source <<- "home-0xdiag-datasets"
trainData   <<- "/home/0xdiag/datasets/airlines/airlines_all.csv"
response    <<- "IsDepDelayed"
import.VA("parsed.hex", trainData)

testData    <<- trainData
import.VA("test.hex", testData)

source("../../R/h2oPerf/epilogue.R")
