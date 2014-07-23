source("../../../R/h2oPerf/prologue.R")
runGLM(x = 1:11, y = 12, family = "binomial", nfolds = 0)
#correct_pass <<- abs(res$auc - 0.6379) < 0.03
source("../../../R/h2oPerf/epilogue.R")
