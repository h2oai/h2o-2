source("../../R/h2oPerf/prologue.R")
data_source <<- "home-0xdiag-datasets"
trainData   <<- trainData   <<- "/home/0xdiag/datasets/airlines/airlines_all.csv"
upload.VA("parsed.hex", trainData)
source("../../R/h2oPerf/epilogue.R")
