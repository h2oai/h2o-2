source("../../../R/h2oPerf/prologue.R")
runKMeans.VA(centers = 6, cols = c('Year','Month','DayofMonth','DayOfWeek','CRSDepTime','CRSArrTime','Distance'), iter.max = 20, normalize = FALSE)

correct_pass <<- as.numeric(abs(log(model@model$tot.withinss) - log(24095826077700)) < 5.0)
source("../../../R/h2oPerf/epilogue.R")
