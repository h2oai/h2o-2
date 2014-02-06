setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

test.GLM.benign <- function(conn) {
  Log.info("Importing benign.csv data...\n")
  benign.hex = h2o.uploadFile.VA(conn, locate("smalldata/logreg/benign.csv"), "benign.hex")
  benign.sum = summary(benign.hex)
  print(benign.sum)
  
  benign.data = read.csv(locate("smalldata/logreg/benign.csv"))
  benign.data = na.omit(benign.data)
  
  myY = 4;
  for(maxx in 11:14) {
    myX = 1:maxx;
    myX = myX[which(myX != myY)]
    
    Log.info(cat("A)H2O GLM (binomial) with parameters:\nX:", myX, "\nY:", myY, "\n"))
    benign.glm.h2o = h2o.glm(y = myY, x = myX, data = benign.hex, family = "binomial", nfolds = 5, alpha = 0.5)
    print(benign.glm.h2o)
    
    benign.glm = glmnet(y = benign.data[,myY], x = data.matrix(benign.data[,myX]), family = "binomial", alpha = 0.5)
    checkGLMModel(benign.glm.h2o, benign.glm)
  }
 
  testEnd()
}

doTest("GLM Test: Benign Data", test.GLM.benign)

