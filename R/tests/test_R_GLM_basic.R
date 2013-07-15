# Demo to test R functionality
# To invoke, need R 3.0.1 as of now
# R -f test_R_GLM_basic.R --args Path/To/H2O_S4.R H2OServer:Port
args <- commandArgs(trailingOnly = TRUE)
if(length(args) != 2)
  stop("Usage: R -f test_R_GLM_basic.R --args Path/To/H2O_S4.R H2OServer:Port")
source(args[1])
argsplit = strsplit(args[2], ":")[[1]]
h2o = new("H2OClient", ip=argsplit[1], port=as.numeric(argsplit[2]))

prostate.hex = importURL(h2o, "http://www.stanford.edu/~anqif/prostate.csv", "prostate.hex")
summary(prostate.hex)
h2o.glm(y = "CAPSULE", x = "AGE,RACE,PSA,DCAPS", data = prostate.hex, family = "binomial", nfolds = 10, alpha = 0.5)

iris.hex = importURL(h2o, "http://www.stanford.edu/~anqif/iris.csv")
summary(iris.hex)
h2o.glm(y = "0", x = "1,2,3,4", data = iris.hex, family = "gaussian", nfolds = 10, alpha = 0.5)