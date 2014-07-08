source("../../../R/h2oPerf/prologue.R")
data_source <<- "home-0xdiag-datasets"
trainData   <<- trainData   <<- "/home/0xdiag/datasets/airlines/airlines_all.csv"
num_train_rows <<- 116695260
num_explan_cols <<- 31
upload("parsed.hex", trainData)
source("../../../R/h2oPerf/epilogue.R")
