checkCoxPHModel <- function(myCoxPH.h2o, myCoxPH.r, tolerance = 1e-8, ...) {
  require(RUnit, quietly = TRUE)
  cat("H2O Cox Proportional Hazards Model\n")
  print(myCoxPH.h2o)
  cat("\nsurvival Package Cox Proportional Hazards Model\n")
  print(myCoxPH.r)
  checkEquals(myCoxPH.r$coefficients, myCoxPH.h2o@model$coefficients,
              tolerance = tolerance)
  checkEquals(myCoxPH.r$var,          myCoxPH.h2o@model$var,
              tolerance = tolerance)
  checkEquals(myCoxPH.r$loglik,       myCoxPH.h2o@model$loglik,
              tolerance = tolerance)
  checkEquals(myCoxPH.r$score,        myCoxPH.h2o@model$score,
              tolerance = tolerance)
  checkTrue  (                        myCoxPH.h2o@model$iter >= 1L)
  checkEquals(myCoxPH.r$means,        myCoxPH.h2o@model$means,
              tolerance = tolerance)
  checkEquals(myCoxPH.r$method,       myCoxPH.h2o@model$method)
  checkEquals(myCoxPH.r$n,            myCoxPH.h2o@model$n)
  checkEquals(myCoxPH.r$nevent,       myCoxPH.h2o@model$nevent)
  checkEquals(myCoxPH.r$wald.test,    myCoxPH.h2o@model$wald.test,
              tolerance = sqrt(tolerance))

  summaryCoxPH.h2o <- summary(myCoxPH.h2o)
  summaryCoxPH.r   <- summary(myCoxPH.r)
  cat("\nH2O Cox Proportional Hazards Model Summary\n")
  print(summaryCoxPH.h2o)
  cat("\nsurvival Package Cox Proportional Hazards Model Summary\n")
  print(summaryCoxPH.r)
  checkEquals(summaryCoxPH.r$n,            summaryCoxPH.h2o@summary$n)
  checkEquals(summaryCoxPH.r$loglik,       summaryCoxPH.h2o@summary$loglik,
              tolerance = tolerance)
  checkEquals(summaryCoxPH.r$nevent,       summaryCoxPH.h2o@summary$nevent)
  checkEquals(summaryCoxPH.r$coefficients[,-5L],
              summaryCoxPH.h2o@summary$coefficients[,-5L],
              tolerance = tolerance)
  checkEquals(summaryCoxPH.r$conf.int[,1:2],
              summaryCoxPH.h2o@summary$conf.int[,1:2],
              tolerance = tolerance)
  checkEquals(summaryCoxPH.r$logtest[1:2], summaryCoxPH.h2o@summary$logtest[1:2],
              tolerance = tolerance)
  checkEquals(summaryCoxPH.r$sctest[1:2],  summaryCoxPH.h2o@summary$sctest[1:2],
              tolerance = tolerance)
  checkEquals(summaryCoxPH.r$rsq,          summaryCoxPH.h2o@summary$rsq,
              tolerance = tolerance)
  checkEquals(summaryCoxPH.r$waldtest[1:2],
              c(round(summaryCoxPH.h2o@summary$waldtest[1L], 2),
                      summaryCoxPH.h2o@summary$waldtest[2L]),
              tolerance = tolerance)

  survfitCoxPH.h2o <- survfit(myCoxPH.h2o)
  survfitCoxPH.r   <- survfit(myCoxPH.r)
  cat("\nH2O Cox Proportional Hazards Model Survival Fit\n")
  print(survfitCoxPH.h2o)
  cat("\nsurvival Package Cox Proportional Hazards Model Survival Fit\n")
  print(survfitCoxPH.r)
  checkEquals(survfitCoxPH.r$n,        survfitCoxPH.h2o$n)
  checkEquals(survfitCoxPH.r$time,     survfitCoxPH.h2o$time)
  checkEquals(survfitCoxPH.r$n.risk,   survfitCoxPH.h2o$n.risk)
  checkEquals(survfitCoxPH.r$n.event,  survfitCoxPH.h2o$n.event)
  checkEquals(survfitCoxPH.r$n.censor, survfitCoxPH.h2o$n.censor)
  checkEquals(survfitCoxPH.r$surv,     survfitCoxPH.h2o$surv,
              tolerance = sqrt(tolerance))
  checkEquals(survfitCoxPH.r$type,     survfitCoxPH.h2o$type)
  checkEquals(survfitCoxPH.r$cumhaz,   survfitCoxPH.h2o$cumhaz,
              tolerance = sqrt(tolerance))
  checkEquals(survfitCoxPH.r$std.err,  survfitCoxPH.h2o$std.err,
              tolerance = sqrt(tolerance))

  checkEquals(coef(myCoxPH.r),       coef(myCoxPH.h2o), tolerance = tolerance)
  checkEquals(coef(summaryCoxPH.r)[,-5L],  coef(summaryCoxPH.h2o)[,-5L],
              tolerance = tolerance)
  checkEquals(extractAIC(myCoxPH.r), extractAIC(myCoxPH.h2o),
              tolerance = tolerance)
  checkEquals(extractAIC(myCoxPH.r,   k = log(myCoxPH.r$n)),
              extractAIC(myCoxPH.h2o, k = log(myCoxPH.r$n)),
              tolerance = tolerance)
  checkEquals(logLik(myCoxPH.r),     logLik(myCoxPH.h2o),
              tolerance = tolerance)
  checkEquals(vcov(myCoxPH.r),       vcov(myCoxPH.h2o),
              tolerance = tolerance)

  invisible()
}
