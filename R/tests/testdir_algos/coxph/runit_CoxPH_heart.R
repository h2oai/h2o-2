setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

test.CoxPH.heart <- function(conn) {
  heart$start <- as.integer(heart$start)
  heart$stop  <- as.integer(heart$stop)
  heart.h2o <- as.h2o(conn, heart, key = "heart.h2o")

  Log.info("H2O Cox PH Model of heart Data Set using Efron's Approximation; 1 predictor\n")
  heart.coxph.h2o <-
    h2o.coxph(x = "age", y = c("start", "stop", "event"), data = heart.h2o,
              key = "heartmod.h2o")
  heart.coxph <- coxph(Surv(start, stop, event) ~ age, data = heart)
  checkCoxPHModel(heart.coxph.h2o, heart.coxph)

  Log.info("H2O Cox PH Model of heart Data Set using Efron's Approximation; 3 predictors\n")
  heart.coxph.h2o <-
    h2o.coxph(x = c("age", "year", "surgery"), y = c("start", "stop", "event"), data = heart.h2o,
              key = "heartmod.h2o")
  heart.coxph <- coxph(Surv(start, stop, event) ~ age + year + surgery, data = heart)
  checkCoxPHModel(heart.coxph.h2o, heart.coxph)

  Log.info("H2O Cox PH Model of heart Data Set using Efron's Approximation; init = 0.05\n")
  heart.coxph.h2o <-
    h2o.coxph(x = "age", y = c("start", "stop", "event"), data = heart.h2o,
              key = "heartmod.h2o", init = 0.05)
  heart.coxph <- coxph(Surv(start, stop, event) ~ age, data = heart, init = 0.05)
  checkCoxPHModel(heart.coxph.h2o, heart.coxph)

  Log.info("H2O Cox PH Model of heart Data Set using Efron's Approximation; iter.max = 1\n")
  heart.coxph.h2o <-
    h2o.coxph(x = "age", y = c("start", "stop", "event"), data = heart.h2o,
              key = "heartmod.h2o", control = h2o.coxph.control(iter.max = 1))
  heart.coxph <- coxph(Surv(start, stop, event) ~ age, data = heart,
                       control = coxph.control(iter.max = 1))
  checkCoxPHModel(heart.coxph.h2o, heart.coxph)

  Log.info("H2O Cox PH Model of heart Data Set using Breslow's Approximation\n")
  heart.coxph.h2o <-
    h2o.coxph(x = "age", y = c("start", "stop", "event"), data = heart.h2o,
              key = "heartmod.h2o", ties = "breslow")
  heart.coxph <-
    coxph(Surv(start, stop, event) ~ age, data = heart, ties = "breslow")
  checkCoxPHModel(heart.coxph.h2o, heart.coxph)

  Log.info("H2O Cox PH Model of heart Data Set using Breslow's Approximation; 3 predictors\n")
  heart.coxph.h2o <-
    h2o.coxph(x = c("age", "year", "surgery"), y = c("start", "stop", "event"), data = heart.h2o,
              key = "heartmod.h2o", ties = "breslow")
  heart.coxph <-
    coxph(Surv(start, stop, event) ~ age + year + surgery, data = heart, ties = "breslow")
  checkCoxPHModel(heart.coxph.h2o, heart.coxph)

  Log.info("H2O Cox PH Model of heart Data Set using Breslow's Approximation; init = 0.05\n")
  heart.coxph.h2o <-
    h2o.coxph(x = "age", y = c("start", "stop", "event"), data = heart.h2o,
              key = "heartmod.h2o", ties = "breslow", init = 0.05)
  heart.coxph <-
    coxph(Surv(start, stop, event) ~ age, data = heart, ties = "breslow",
          init = 0.05)
  checkCoxPHModel(heart.coxph.h2o, heart.coxph)

  Log.info("H2O Cox PH Model of heart Data Set using Breslow's Approximation; iter.max = 1\n")
  heart.coxph.h2o <-
    h2o.coxph(x = "age", y = c("start", "stop", "event"), data = heart.h2o,
              key = "heartmod.h2o", ties = "breslow",
              control = h2o.coxph.control(iter.max = 1))
  heart.coxph <-
    coxph(Surv(start, stop, event) ~ age, data = heart, ties = "breslow",
          control = coxph.control(iter.max = 1))
  checkCoxPHModel(heart.coxph.h2o, heart.coxph)

  testEnd()
}

doTest("Cox PH Model Test: Heart", test.CoxPH.heart)
