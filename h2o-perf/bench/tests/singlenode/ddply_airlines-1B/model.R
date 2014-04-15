source("../../../R/h2oPerf/prologue.R")
data <- new("H2OParsedData", h2o = h, key = "parsed.hex", logic = TRUE)
h2o.ddply(data, .("C2", "C7", "C9", "C10", "C12"),  nrow)
source("../../../R/h2oPerf/epilogue.R")
