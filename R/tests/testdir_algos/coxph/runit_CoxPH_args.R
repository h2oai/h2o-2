setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

test.CoxPH.args <- function(conn) {
  bladder.h2o <- as.h2o(conn, bladder, key = "bladder.h2o")
  bladder0.h2o <- as.h2o(conn, bladder[bladder$event == 0, ], key = "bladder0.h2o")

  Log.info("H2O Cox PH x Argument\n")
  checkException(h2o.coxph(x = "foo",              y = c("stop", "event"), data = bladder.h2o), silent = TRUE)
  checkException(h2o.coxph(x = c("number", "foo"), y = c("stop", "event"), data = bladder.h2o), silent = TRUE)
  checkException(h2o.coxph(x = "number", y = c("stop", "event"), data = bladder0.h2o), silent = TRUE)

  Log.info("H2O Cox PH y Argument\n")
  checkException(h2o.coxph(x = "foo",    y = c("foo", "event"), data = bladder.h2o), silent = TRUE)
  checkException(h2o.coxph(x = "number", y = c("stop", "foo"),  data = bladder.h2o), silent = TRUE)

  Log.info("H2O Cox PH data Argument\n")
  checkException(h2o.coxph(x = "number", y = c("stop", "event"), data = bladder), silent = TRUE)

  Log.info("H2O Cox PH init Control Argument\n")
  checkException(h2o.coxph(x = "number", y = c("stop", "event"), data = bladder.h2o, init = NULL), silent = TRUE)
  checkException(h2o.coxph(x = "number", y = c("stop", "event"), data = bladder.h2o, init = NA_real_), silent = TRUE)

  Log.info("H2O Cox PH lre Control Argument\n")
  checkException(h2o.coxph(x = "number", y = c("stop", "event"), data = bladder.h2o, lre = -1), silent = TRUE)
  checkException(h2o.coxph(x = "number", y = c("stop", "event"), data = bladder.h2o, lre = NULL), silent = TRUE)
  checkException(h2o.coxph(x = "number", y = c("stop", "event"), data = bladder.h2o, lre = NA_real_), silent = TRUE)

  Log.info("H2O Cox PH iter.max Control Argument\n")
  checkException(h2o.coxph(x = "number", y = c("stop", "event"), data = bladder.h2o, iter.max = -1), silent = TRUE)
  checkException(h2o.coxph(x = "number", y = c("stop", "event"), data = bladder.h2o, iter.max = NULL), silent = TRUE)
  checkException(h2o.coxph(x = "number", y = c("stop", "event"), data = bladder.h2o, iter.max = NA_integer_), silent = TRUE)

  testEnd()
}

doTest("Cox PH Model Test: Function Arguments", test.CoxPH.args)
