setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

test.CoxPH.args <- function(conn) {
  bladder.h2o <- as.h2o(conn, bladder, key = "bladder.h2o")

  Log.info("H2O Cox PH x Argument\n")
  checkException(h2o.coxph(x = "foo",              y = c("stop", "event"), data = bladder.h2o))
  checkException(h2o.coxph(x = c("number", "foo"), y = c("stop", "event"), data = bladder.h2o))

  Log.info("H2O Cox PH y Argument\n")
  checkException(h2o.coxph(x = "foo",    y = c("foo", "event"), data = bladder.h2o))
  checkException(h2o.coxph(x = "number", y = c("stop", "foo"),  data = bladder.h2o))

  Log.info("H2O Cox PH data Argument\n")
  checkException(h2o.coxph(x = "number", y = c("stop", "event"), data = bladder))

  Log.info("H2O Cox PH lre Control Argument\n")
  checkException(h2o.coxph(x = "number", y = c("stop", "event"), data = bladder.h2o, lre = -1))
  checkException(h2o.coxph(x = "number", y = c("stop", "event"), data = bladder.h2o, lre = NULL))
  checkException(h2o.coxph(x = "number", y = c("stop", "event"), data = bladder.h2o, lre = NA_real_))

  Log.info("H2O Cox PH iter.max Control Argument\n")
  checkException(h2o.coxph(x = "number", y = c("stop", "event"), data = bladder.h2o, iter.max = -1))
  checkException(h2o.coxph(x = "number", y = c("stop", "event"), data = bladder.h2o, iter.max = NULL))
  checkException(h2o.coxph(x = "number", y = c("stop", "event"), data = bladder.h2o, iter.max = NA_integer_))

  testEnd()
}

doTest("Cox PH Model Test: Function Arguments", test.CoxPH.args)
