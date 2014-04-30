source("../../../R/h2oPerf/prologue.R")
expected_results <- c(0.053, 0.033, 0.188, 0.136, 0.134, 0.285, 0.152, 0.176, 0.143, 0.173)
runGBMScore(expected_results=expected_results, type="cm")
source("../../../R/h2oPerf/epilogue.R")
