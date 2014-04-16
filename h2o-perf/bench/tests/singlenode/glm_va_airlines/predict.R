source("../../../R/h2oPerf/prologue.R")
runGLMScore.VA()
correct_pass <<- abs(res$auc - 0.6379) < 0.03
source("../../../R/h2oPerf/epilogue.R")
