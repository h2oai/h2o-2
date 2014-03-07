source("../../R/h2oPerf/prologue.R")
runGLM.FV(x = c("Year", "Month", 
                "DayofMonth", "DayOfWeek", 
                "CRSDepTime","CRSArrTime", 
                "UniqueCarrier", "CRSElapsedTime", 
                "Origin", "Dest", "Distance"), 
          y = "IsDepDelayed", 
          family = "binomial",
          nfolds = 0)
source("../../R/h2oPerf/epilogue.R")
