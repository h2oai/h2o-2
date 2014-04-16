source("../../../R/h2oPerf/prologue.R")
runKMeans.VA(centers = 6, cols = c('C1','C2','C3','C4','C5','C6','C8','C11'), iter.max = 100, normalize = FALSE)
correct_pass <<- as.numeric(abs(log(model@model$tot.withinss) - log(185462248582411)) < 5.0)
source("../../../R/h2oPerf/epilogue.R")
