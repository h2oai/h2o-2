# Demo to test R functionality
# To invoke, need R 3.0.1 as of now
# R -f H2O_S4TestDemo.R
source("H2O_S4.R")
h2o = new("H2OClient", ip="localhost", port=54321)

prostate.hex = importURL(h2o, "http://www.stanford.edu/~anqif/prostate.csv", "prostate.hex")
prostate.sum = summary(prostate.hex)
prostate.glm = h2o.glm(y = "CAPSULE", x = "AGE,RACE,PSA,DCAPS", data = prostate.hex, family = "binomial", nfolds = 10, alpha = 0.5)
prostate.km = h2o.kmeans(prostate.hex, 5)

prostate.data = read.csv("../smalldata/logreg/prostate.csv", header = TRUE)
prostate.sum2 = summary(prostate.data)
prostate.glm2 = glm(CAPSULE ~ AGE + RACE + PSA + DCAPS, data = prostate.data, family = binomial)
prostate.km2 = kmeans(prostate.data, centers = 5)

# iris.hex = importURL(h2o, "http://www.stanford.edu/~anqif/iris.csv")
iris.hex = importFile(h2o, "C:/Users/Anqi/workspace/h2o/smalldata/iris/iris.csv")
iris.sum = summary(iris.hex)

iris.data = read.csv("../smalldata/iris/iris.csv", header = FALSE)
iris.sum2 = summary(iris.data)