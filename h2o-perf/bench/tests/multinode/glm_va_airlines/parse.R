source("../../R/h2oPerf/prologue.R")
data_source <<- "home-0xdiag-datasets"
trainData   <<- "/home/0xdiag/datasets/standard/allyears.csv"
num_train_rows <<- 123534970
num_explan_cols <<- 31
response    <<- "IsDepDelayed"
import.VA("parsed.hex", trainData)

testData    <<- trainData
import.VA("test.hex", testData)

source("../../R/h2oPerf/epilogue.R")
