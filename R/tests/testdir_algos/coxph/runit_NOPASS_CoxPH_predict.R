setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

test.CoxPH.predict <- function(conn) {
  Log.info("Import PBC from surivival package into H2O...")
  pbc.hex <- as.h2o(conn, pbc, key = "pbc.hex")
  Log.info("Create response and predictor features...")
  pbc.hex$statusOf2  <- pbc.hex$status == 2
  pbc.hex$logBili    <- log(pbc.hex$bili)
  pbc.hex$logProtime <- log(pbc.hex$protime)
  pbc.hex$logAlbumin <- log(pbc.hex$albumin)
  Log.info("Build Cox PH model with features including both numeric and factor types...")
  pbcmodel <- h2o.coxph(x = c("age", "edema", "logBili", "logProtime", "logAlbumin", "sex"),
                        y = c("time", "statusOf2"), data = pbc.hex)
  
  Log.info("Predict on the Cox PH model...")
  pred <- h2o.predict(object = pbcmodel, newdata = pbc.hex)
  #### Missing comparison against cox model in R. Input after AIOO error fixed for h2o.predict on Cox model.
  testEnd()
}

doTest("Cox PH Model Test: Categorical Column", test.CoxPH.predict)
