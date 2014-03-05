source("../../R/h2oPerf/prologue.R")
data_source <<- "home-0xdiag-datasets"
trainData   <<- "/home/0xdiag/datasets/airlines/airlines_all.csv"
num_train_rows <<- 123534970
num_explan_cols <<- 31
upload.VA("parsed.hex", trainData)
source("../../R/h2oPerf/epilogue.R")
