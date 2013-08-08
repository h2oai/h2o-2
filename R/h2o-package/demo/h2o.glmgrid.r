source("H2O_Load.R")
localH2O = new("H2OClient", ip = "localhost", port = 54321)
h2o.checkClient(localH2O)

prostate.hex = h2o.importURL(localH2O, path = "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", key = "prostate.hex")
alpha = c(0.25,0.5,0.75)
lambda = c(1,10)
  for(i in 1:length(lambda)){
    for(j in 1:length(alpha)){
      print(paste("for alpha=",alpha[j],"   ",", lambda=",lambda[i],sep=""),quote=F)                                       
      print(h2o.glm(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, family = "binomial", nfolds = 10, alpha =alpha[j] ,lambda=lambda[i]))
      cat("\n")
    }
  }
