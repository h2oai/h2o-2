source("../../R/h2oPerf/prologue.R")
data_source <<- "smalldata"
trainData   <<- "~/master/h2o/smalldata/airlines/allyears2k_headers.zip"
upload.VA("parsed.hex")
source("../../R/h2oPerf/epilogue.R")
