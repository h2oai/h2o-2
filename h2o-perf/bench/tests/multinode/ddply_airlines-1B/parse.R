source("../../../R/h2oPerf/prologue.R")
data_source <<- "home-0xdiag-datasets"
trainData   <<- "/home/0xdiag/datasets/airlines/airlines1B"
import.FV("parsed.hex", trainData)
num_train_rows <<- 1021368222
num_explan_cols <<- 12
source("../../../R/h2oPerf/epilogue.R")
