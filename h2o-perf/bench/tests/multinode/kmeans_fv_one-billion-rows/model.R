source("../../../R/h2oPerf/prologue.R")
runKMeans(centers = 6, cols = c('C1','C2','C3','C4','C5','C6','C8','C11'), iter.max = 100, normalize = FALSE)
source("../../../R/h2oPerf/epilogue.R")
