checkGLMModel <- function(myGLM.h2o, myGLM.r) {
  coeff.mat = as.matrix(myGLM.r$beta)
  numcol = ncol(coeff.mat)
  coeff.R = c(coeff.mat[,numcol], Intercept = as.numeric(myGLM.r$a0[numcol]))
  print("H2O Coefficients")
  print(myGLM.h2o@model$coefficients)
  print("R Coefficients")
  print(coeff.R)

  print("SORTED COEFFS")
  print("H2O Coefficients")
  print(sort(myGLM.h2o@model$coefficients))
  print("R Coefficients")
  print(sort(coeff.R))
  checkEqualsNumeric(sort(myGLM.h2o@model$coefficients), sort(coeff.R), tolerance = 3.8)
  checkEqualsNumeric(myGLM.h2o@model$null.deviance, myGLM.r$nulldev, tolerance = 1.5)
}

# Used to check glmnet models that have an extra intercept term
checkGLMModel2 <- function(myGLM.h2o, myGLM.r) {
  coeff.mat = as.matrix(myGLM.r$beta)
  numcol = ncol(coeff.mat)
  coeff.R = c(coeff.mat[,numcol][1:length(coeff.mat)-1], Intercept = as.numeric(myGLM.r$a0[numcol]))
#  print("H2O Coefficients")
#  print(myGLM.h2o@model$coefficients)
#  print("R Coefficients")
#  print(coeff.R)

  print("H2O NULL DEVIANCE and DEVIANCE")
  print(myGLM.h2o@model$null.deviance)
  print(myGLM.h2o@model$deviance)
  print("GLMNET NULL DEVIANCE and DEVIANCE")
  print(myGLM.r$nulldev)
  print(deviance(myGLM.r))

  print("SORTED COEFFS")
  print("H2O Coefficients")
  print(sort(myGLM.h2o@model$coefficients))
  print("R Coefficients")
  print(sort(coeff.R))
  checkEqualsNumeric(myGLM.h2o@model$deviance, deviance(myGLM.r), tolerance = 0.1)
  checkEqualsNumeric(sort(myGLM.h2o@model$coefficients), sort(coeff.R), tolerance = 0.5)
}