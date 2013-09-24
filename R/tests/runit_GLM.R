# Test generalized linear modeling in H2O
checkGLMModel <- function(myGLM.h2o, myGLM.r) {
  coeff.mat = as.matrix(myGLM.r$beta)
  numcol = ncol(coeff.mat)
  coeff.R = c(coeff.mat[,numcol], Intercept = as.numeric(myGLM.r$a0[numcol]))
  print(myGLM.h2o@model$coefficients)
  print(coeff.R)
  checkEqualsNumeric(myGLM.h2o@model$coefficients, coeff.R, tolerance = 0.5)
  
  checkEqualsNumeric(myGLM.h2o@model$null.deviance, myGLM.r$nulldev, tolerance = 0.5)
}

test.GLM.benign <- function() {
  cat("\nImporting benign.csv data...\n")
  serverH2O = new("H2OClient", ip=myIP, port=myPort)
  benign.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/benign.csv")
  benign.sum = summary(benign.hex)
  print(benign.sum)
  
  benign.data = read.csv(text = getURL("https://raw.github.com/0xdata/h2o/master/smalldata/logreg/benign.csv"), header = TRUE)
  benign.data = na.omit(benign.data)

  myY = "3"; myY.r = as.numeric(myY) + 1
  for(maxx in 10:13) {
    myX = 0:maxx
    myX = myX[which(myX != myY)]; myX.r = myX + 1
    myX = paste(myX, collapse=",")
    
    cat("\nH2O GLM (binomial) with parameters:\nX:", myX, "\nY:", myY, "\n")
    benign.h2o = h2o.glm(y = myY, x = myX, data = benign.hex, family = "binomial", nfolds = 5, alpha = 0.5)
    print(benign.h2o)
    
    # benign.glm = glm.fit(y = benign.data[,myY.r], x = benign.data[,myX.r], family = binomial)
    benign.glm = glmnet(y = benign.data[,myY.r], x = data.matrix(benign.data[,myX.r]), family = "binomial", alpha = 0.5)
    checkGLMModel(benign.h2o, benign.glm)
  }
}

test.GLM.prostate <- function() {
  cat("\nImporting prostate.csv data...\n")
  serverH2O = new("H2OClient", ip=myIP, port=myPort)
  prostate.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", "prostate.hex")
  prostate.sum = summary(prostate.hex)
  print(prostate.sum)
  
  prostate.data = read.csv(text = getURL("https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv"), header = TRUE)
  prostate.data = na.omit(prostate.data)
  
  myY = "1"; myY.r = as.numeric(myY) + 1
  for(maxx in 3:8) {
    myX = 1:maxx
    myX = myX[which(myX != myY)]; myX.r = myX + 1
    myX = paste(myX, collapse=",")
    
    cat("\nH2O GLM (binomial) with parameters:\nX:", myX, "\nY:", myY, "\n")
    prostate.h2o = h2o.glm(y = myY, x = myX, data = prostate.hex, family = "binomial", nfolds = 10, alpha = 0.5)
    print(prostate.h2o)
    
    # prostate.glm = glm.fit(y = prostate.data[,myY.r], x = prostate.data[,myX.r], family = binomial)
    prostate.glm = glmnet(y = prostate.data[,myY.r], x = data.matrix(prostate.data[,myX.r]), family = "binomial", alpha = 0.5)
    checkGLMModel(prostate.h2o, prostate.glm)
  }
}

test.GLM.covtype <- function() {
  cat("\nImporting covtype.20k.data...\n")
  serverH2O = new("H2OClient", ip=myIP, port=myPort)
  # covtype.hex = h2o.importFile(serverH2O, "../../UCI/UCI-large/covtype/covtype.data")
  covtype.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/covtype/covtype.20k.data")
  covtype.sum = summary(covtype.hex)
  print(covtype.sum)
  
  myY = "54"
  myX = ""
  # max_iter = 8
  
  # L2: alpha = 0, lambda = 0
  start = Sys.time()
  covtype.h2o1 = h2o.glm(y = myY, x = myX, data = covtype.hex, family = "binomial", nfolds = 2, alpha = 0, lambda = 0)
  end = Sys.time()
  cat("\nGLM (L2) on", covtype.hex@key, "took", as.numeric(end-start), "seconds\n")
  print(covtype.h2o1)
  
  # Elastic: alpha = 0.5, lambda = 1e-4
  start = Sys.time()
  covtype.h2o2 = h2o.glm(y = myY, x = myX, data = covtype.hex, family = "binomial", nfolds = 2, alpha = 0.5, lambda = 1e-4)
  end = Sys.time()
  cat("\nGLM (Elastic) on", covtype.hex@key, "took", as.numeric(end-start), "seconds\n")
  print(covtype.h2o2)
  
  # L1: alpha = 1, lambda = 1e-4
  start = Sys.time()
  covtype.h2o3 = h2o.glm(y = myY, x = myX, data = covtype.hex, family = "binomial", nfolds = 2, alpha = 1, lambda = 1e-4)
  end = Sys.time()
  cat("\nGLM (L1) on", covtype.hex@key, "took", as.numeric(end-start), "seconds\n")
  print(covtype.h2o3)
}