checkCoxPHModel <- function(myCoxPH.h2o, myCoxPH.r,
                            tolerance = .Machine$double.eps^0.5, ...) {
  require(RUnit, quietly = TRUE)
  cat("H2O Cox Proportional Hazards Model\n")
  print(myCoxPH.h2o)
  cat("\nsurvival Package Cox Proportional Hazards Model\n")
  print(myCoxPH.r)
  checkEquals(myCoxPH.r$var,       myCoxPH.h2o@model$var)
  checkEquals(myCoxPH.r$loglik,    myCoxPH.h2o@model$loglik)
  checkEquals(myCoxPH.r$score,     myCoxPH.h2o@model$score)
  checkTrue  (                     myCoxPH.h2o@model$iter >= 1L)
  checkEquals(myCoxPH.r$means,     myCoxPH.h2o@model$means)
  checkEquals(myCoxPH.r$method,    myCoxPH.h2o@model$method)
  checkEquals(myCoxPH.r$n,         myCoxPH.h2o@model$n)
  checkEquals(myCoxPH.r$nevent,    myCoxPH.h2o@model$nevent)
  checkEquals(myCoxPH.r$wald.test, myCoxPH.h2o@model$wald.test,
              tolerance = sqrt(tolerance))

  summaryCoxPH.h2o <- summary(myCoxPH.h2o)
  summaryCoxPH.r   <- summary(myCoxPH.r)
  cat("\nH2O Cox Proportional Hazards Model Summary\n")
  print(summaryCoxPH.h2o)
  cat("\nsurvival Package Cox Proportional Hazards Model Summary\n")
  print(summaryCoxPH.r)
  checkEquals(summaryCoxPH.r$n,            summaryCoxPH.h2o@summary$n)
  checkEquals(summaryCoxPH.r$loglik,       summaryCoxPH.h2o@summary$loglik)
  checkEquals(summaryCoxPH.r$nevent,       summaryCoxPH.h2o@summary$nevent)
  checkEquals(summaryCoxPH.r$coefficients[,-5L],
              summaryCoxPH.h2o@summary$coefficients[,-5L])
  checkEquals(summaryCoxPH.r$conf.int[,1:2],
              summaryCoxPH.h2o@summary$conf.int[,1:2])
  checkEquals(summaryCoxPH.r$logtest[1:2], summaryCoxPH.h2o@summary$logtest[1:2])
  checkEquals(summaryCoxPH.r$sctest[1:2],  summaryCoxPH.h2o@summary$sctest[1:2])
  checkEquals(summaryCoxPH.r$rsq,          summaryCoxPH.h2o@summary$rsq)
  checkEquals(summaryCoxPH.r$waldtest[1:2],summaryCoxPH.h2o@summary$waldtest[1:2],
              tolerance = sqrt(tolerance))

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
  checkEquals(survfitCoxPH.r$surv,     survfitCoxPH.h2o$surv)
  checkEquals(survfitCoxPH.r$type,     survfitCoxPH.h2o$type)
  checkEquals(survfitCoxPH.r$cumhaz,   survfitCoxPH.h2o$cumhaz)
  checkEquals(survfitCoxPH.r$std.err,  survfitCoxPH.h2o$std.err)

  checkEquals(coef(myCoxPH.r),       coef(myCoxPH.h2o))
  checkEquals(coef(summaryCoxPH.r)[,-5L],  coef(summaryCoxPH.h2o)[,-5L])
  checkEquals(extractAIC(myCoxPH.r), extractAIC(myCoxPH.h2o))
  checkEquals(extractAIC(myCoxPH.r,   k = log(myCoxPH.r$n)),
              extractAIC(myCoxPH.h2o, k = log(myCoxPH.r$n)))
  checkEquals(logLik(myCoxPH.r),     logLik(myCoxPH.h2o))
  checkEquals(vcov(myCoxPH.r),       vcov(myCoxPH.h2o))  

  invisible()
}