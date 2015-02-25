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



### Functions to compare model deviance and coefficients
compare_deviance <- function(h2o_model, glmnet_model){
  print(paste("Deviance in GLMnet Model : " , deviance(glmnet_model)))
  print(paste("Deviance in H2O Model    : " , h2o_model@model$deviance))
  diff = deviance(glmnet_model) - h2o_model@model$deviance
  if(diff < 2E-2) {
    return("PASS")
  } else {
    return ("Deviance in H2O model doesn't match up to GLMnet!")
  }  
}


compare_coeff <- function(h2o_model, glmnet_model){
  ncol = length(glmnet_model$a0)
  h2o_coeff = h2o_model@model$coefficients
  raw_glm_coeff = glmnet_model$beta[,ncol]
  coeffNames = names(h2o_coeff)
  fun <- function(coeffName) {
    if(!coeffName=="Intercept"){
      raw_glm_coeff[coeffName]
    } else {
      glmnet_model$a0[[ncol]]
    }
  }
  glmnet_coeff = sapply(coeffNames, fun)
  diff = abs(glmnet_coeff - h2o_coeff)
  print(rbind(h2o_coeff, glmnet_coeff))
  if(all(diff < 2E-2)) {
    return("PASS")
  } else {
    return ("Coefficients in H2O model doesn't match up to GLMnet!")
  }
}