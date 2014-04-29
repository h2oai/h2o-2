source("../../R/h2oPerf/prologue.R")
#runGLM.VA(x = c("Year", "Month", 
#                "DayofMonth", "DayOfWeek", 
#                "CRSDepTime","CRSArrTime", 
#                "UniqueCarrier", "CRSElapsedTime", 
#                "Origin", "Dest", "Distance"), 
#          y = "IsDepDelayed", 
#          family = "binomial",
#          nfolds = 0)

runGLM.FV(x = 1:11, y = 12, family = "binomial", nfolds = 0)
source("../../R/h2oPerf/epilogue.R")
