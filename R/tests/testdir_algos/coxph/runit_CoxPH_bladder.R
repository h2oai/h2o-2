setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

test.CoxPH.bladder <- function(conn) {
  Log.info("H2O Cox PH Model of bladder Data Set\n")
  bladder.h2o <- as.h2o(conn, bladder, key = "bladder.h2o")
  bladder.coxph.h2o <-
    h2o.coxph(x = "number", y = c("stop", "event"), data = bladder.h2o,
              key = "bladmod.h2o")
  bladder.coxph <-
    coxph(Surv(stop, event) ~ number, data = bladder, ties = "breslow")
  checkCoxPHModel(bladder.coxph.h2o, bladder.coxph)

  testEnd()
}

doTest("Cox PH Model Test: Bladder", test.CoxPH.bladder)
