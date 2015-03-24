setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

test.GLM.offset <- function(conn) {
  Log.info("Importing lung.csv data...\n")
  lung.hex = h2o.uploadFile(conn, locate("smalldata/glm_test/lung.csv"))
  lung.hex$log_pop <- log(lung.hex$pop)
  lung.sum = summary(lung.hex)
  print(lung.sum)

  lung.r = read.csv(locate("smalldata/glm_test/lung.csv"), header = TRUE)
  lung.r = na.omit(lung.r)

  Log.info(cat("H2O GLM (poisson)"))
  lung.glm.h2o = h2o.glm(y = 4, x = 1:2, data = lung.hex, family = "poisson", link = "log", offset = "log_pop")
  print(lung.glm.h2o)

  Log.info(cat("{stats} glm (poisson)"))
  lung.glm.r = glm(cases ~ city + age + offset(log(pop)), family = "poisson", data = lung.r)

  checkEqualsNumeric(lung.glm.h2o@model$deviance, lung.glm.r$deviance, tolerance = 0.01)
  checkEqualsNumeric(lung.glm.h2o@model$aic, lung.glm.r$aic, tolerance = 0.01)
  checkEqualsNumeric(lung.glm.h2o@model$null.deviance, lung.glm.r$null.deviance, tolerance = 0.01)
  checkEqualsNumeric(lung.glm.h2o@model$coefficients["city.Horsens"], lung.glm.r$coefficients["cityHorsens"], tolerance = 0.01)
  checkEqualsNumeric(lung.glm.h2o@model$coefficients["city.Kolding"], lung.glm.r$coefficients["cityKolding"], tolerance = 0.01)
  checkEqualsNumeric(lung.glm.h2o@model$coefficients["city.Vejle"], lung.glm.r$coefficients["cityVejle"], tolerance = 0.01)
  checkEqualsNumeric(lung.glm.h2o@model$coefficients["age"], lung.glm.r$coefficients["age"], tolerance = 0.01)
  checkEqualsNumeric(lung.glm.h2o@model$coefficients["Intercept"], lung.glm.r$coefficients["(Intercept)"], tolerance = 0.01)
  checkEqualsNumeric(lung.glm.h2o@model$coefficients["log_pop"], 1.0, tolerance = 1e-10)

  testEnd()
}

doTest("GLM offset test", test.GLM.offset)