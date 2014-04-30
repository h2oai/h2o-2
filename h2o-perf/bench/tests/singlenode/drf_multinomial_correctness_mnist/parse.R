source("../../../R/h2oPerf/prologue.R")

data_source <<- "home-0xdiag-datasets"

trainData   <<-  "../../../../../smalldata/mnist/train.csv.gz"
num_train_rows <<- 60000
num_explan_cols <<- 784
response <<- "C785"
import.FV("parsed.hex", trainData)

testData    <<-  "../../../../../smalldata/mnist/test.csv.gz"

import.FV("test.hex", testData)

source("../../../R/h2oPerf/epilogue.R")
