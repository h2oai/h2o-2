source("../../../R/h2oPerf/prologue.R")
runKMeans.VA(centers = 6, cols = c('Year','Month','DayofMonth','DayOfWeek','CRSDepTime','CRSArrTime','Distance'), iter.max = 20, normalize = FALSE)
source("../../../R/h2oPerf/epilogue.R")
