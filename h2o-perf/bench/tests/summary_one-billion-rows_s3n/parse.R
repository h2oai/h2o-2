source("../../R/h2oPerf/prologue.R")
data_source <<- "s3://h2o-bench/AirlinesClean2"
trainData   <<- "s3n://h2o-bench/AirlinesClean2"
hdfs.VA("parsed.hex","s3n://h2o-bench/AirlinesClean2")
num_train_rows <<- 1021368222
num_explan_cols <<- 12
source("../../R/h2oPerf/epilogue.R")
