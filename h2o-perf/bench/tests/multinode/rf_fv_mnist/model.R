source("../../../R/h2oPerf/prologue.R")
runRF(x = 1:784, y = 785, depth = 50, ntree = 10)
source("../../../R/h2oPerf/epilogue.R")
