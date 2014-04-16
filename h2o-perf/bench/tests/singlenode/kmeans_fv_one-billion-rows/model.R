source("../../../R/h2oPerf/prologue.R")
runKMeans.FV(centers = 6, cols = c('C1','C2','C3','C4','C5','C6','C8','C11'), iter.max = 100, normalize = FALSE)
correct_pass <<- 1
source("../../../R/h2oPerf/epilogue.R")
