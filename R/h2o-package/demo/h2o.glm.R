# This is a demo of H2O's GLM function
# It imports a data set, parses it, and prints a summary
# Then, it runs GLM with a binomial link function using 10-fold cross-validation
library(h2o)
h2o = new("H2OClient", ip="localhost", port=54321)

prostate.hex = importFile(h2o, system.file("extdata", "prostate.csv", package="h2o"), "prostate.hex")
summary(prostate.hex)
prostate.glm = h2o.glm(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, family = "binomial", nfolds = 10, alpha = 0.5)
print(prostate.glm)