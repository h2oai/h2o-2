source("../../../R/h2oPerf/prologue.R")
runGLM(x = 1:11, y = 12, family = "binomial", nfolds = 0)
source("../../../R/h2oPerf/epilogue.R")
