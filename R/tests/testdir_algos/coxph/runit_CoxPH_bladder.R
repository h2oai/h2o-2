setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

test.CoxPH.bladder <- function(conn) {
  bladder.h2o <- as.h2o(conn, bladder, key = "bladder.h2o")

  Log.info("H2O Cox PH Model of bladder Data Set using Efron's approximation\n")
  bladder.coxph.h2o <-
    h2o.coxph(x = "number", y = c("stop", "event"), data = bladder.h2o,
              key = "bladmod.h2o")
  bladder.coxph <- coxph(Surv(stop, event) ~ number, data = bladder)
  checkCoxPHModel(bladder.coxph.h2o, bladder.coxph, tolerance = 1e-7)

  Log.info("H2O Cox PH Model of bladder Data Set using Efron's approximation; iter.max = 1\n")
  bladder.coxph.h2o <-
      h2o.coxph(x = "number", y = c("stop", "event"), data = bladder.h2o,
                key = "bladmod.h2o", control = h2o.coxph.control(iter.max = 1))
  bladder.coxph <- coxph(Surv(stop, event) ~ number, data = bladder,
                         control = coxph.control(iter.max = 1))
  checkCoxPHModel(bladder.coxph.h2o, bladder.coxph, tolerance = 1e-7)

  Log.info("H2O Cox PH Model of bladder Data Set using Breslow's approximation\n")
  bladder.coxph.h2o <-
      h2o.coxph(x = "number", y = c("stop", "event"), data = bladder.h2o,
          key = "bladmod.h2o", ties = "breslow")
  bladder.coxph <-
      coxph(Surv(stop, event) ~ number, data = bladder, ties = "breslow")
  checkCoxPHModel(bladder.coxph.h2o, bladder.coxph)

  Log.info("H2O Cox PH Model of bladder Data Set using Breslow's approximation; iter.max = 1\n")
  bladder.coxph.h2o <-
    h2o.coxph(x = "number", y = c("stop", "event"), data = bladder.h2o,
              key = "bladmod.h2o", ties = "breslow",
              control = h2o.coxph.control(iter.max = 1))
  bladder.coxph <-
    coxph(Surv(stop, event) ~ number, data = bladder, ties = "breslow",
          control = coxph.control(iter.max = 1))
  checkCoxPHModel(bladder.coxph.h2o, bladder.coxph)

  testEnd()
}

doTest("Cox PH Model Test: Bladder", test.CoxPH.bladder)
