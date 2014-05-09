source("../../../R/h2oPerf/prologue.R")
expected_results <- c(0.008, 0.01, 0.022, 0.036, 0.025, 0.018, 0.022, 0.021, 0.036, 0.031)
runDLScore(expected_results=expected_results, type="cm")
source("../../../R/h2oPerf/epilogue.R")
