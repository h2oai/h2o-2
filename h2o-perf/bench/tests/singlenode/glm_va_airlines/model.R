source("../../../R/h2oPerf/prologue.R")
x <- c("Year","Month","DayofMonth","DayOfWeek","CRSDepTime","CRSArrTime","UniqueCarrier","CRSElapsedTime","Origin","Dest","Distance")
runGLM.VA(x = x, y = "IsDepDelayed", "binomial")
source("../../../R/h2oPerf/epilogue.R")
