source("../../../R/h2oPerf/prologue.R")
expected_results <- c(0.009, 0.011, 0.037, 0.044, 0.034, 0.04, 0.023, 0.041, 0.04, 0.048)
runRFScore(expected_results=expected_results, type="cm")
source("../../../R/h2oPerf/epilogue.R")
