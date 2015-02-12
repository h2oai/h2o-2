# Demo to test R functionality
# To invoke, need R 2.13.0 or higher
# R -f H2OTestDemo.R

source("H2O_Load.R")
# library(h2o)
localH2O = new("H2OClient", ip = "localhost", port = 54321)
# h2o.checkClient(localH2O)

# Test using prostate cancer data set
prostate.hex = h2o.importURL(localH2O, path = "https://raw.github.com/h2oai/h2o/master/smalldata/logreg/prostate.csv", key = "prostate.hex")
prostate.sum = summary(prostate.hex)
print(prostate.sum)
prostate.glm = h2o.glm(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, family = "binomial", nfolds = 10, alpha = 0.5)
print(prostate.glm)
prostate.km = h2o.kmeans(data = prostate.hex, centers = 5, cols = c("AGE","RACE","GLEASON","CAPSULE","DCAPS"))
print(prostate.km)
prostate.rf = h2o.randomForest(y = "CAPSULE", x_ignore = c("ID","DPROS"), data = prostate.hex, ntree = 50, depth = 150)

# Test of random forest using iris data set
iris.hex = h2o.importFile(localH2O, path = "../smalldata/iris/iris.csv", key = "iris.hex")
iris.sum = summary(iris.hex)
print(iris.sum)
iris.rf = h2o.randomForest(y = "4", data = iris.hex, ntree = 50, depth = 100, classwt = c("Iris-versicolor"=20.0, "Iris-virginica"=30.0))
print(iris.rf)

# Test of k-means using random Gaussian data set
covtype.hex = h2o.importFile(localH2O, path = "../smalldata/covtype/covtype.20k.data")
covtype.sum = summary(covtype.hex)
print(covtype.sum)
covtype.km = h2o.kmeans(covtype.hex, 10)
print(covtype.km)

# Test import folder function
glm_test.hex = h2o.importFolder(localH2O, path = "../smalldata/glm_test")
for(i in 1:length(glm_test.hex))
  print(summary(glm_test.hex[[i]]))
  
  
#Test of GLMGrid using prostate cancer data set
prostate.hex = h2o.importURL(localH2O, path = "https://raw.github.com/h2oai/h2o/master/smalldata/logreg/prostate.csv", key = "prostate.hex")
prostate.sum = summary(prostate.hex)
prostate.glmgrid = h2o.glmgrid(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, family = "binomial", nfolds = 10, alpha = c(0.2,0.5,1),lambda=c(1e-4,1))
print(prostate.glmgrid)

# Test of PCA using prostate cancer data set
prostate.hex = h2o.importURL(localH2O, path = "https://raw.github.com/h2oai/h2o/master/smalldata/logreg/prostate.csv", key = "prostate.hex")
prostate.pca = h2o.prcomp(prostate.hex)
print(prostate.pca)
summary(prostate.pca)

# Test of gbm using iris data set
iris.hex = h2o.importFile(localH2O, path = "../smalldata/iris/iris.csv", key = "iris.hex")
iris.sum = summary(iris.hex)
print(iris.sum)
iris.gbm=h2o.gbm(data=iris.hex,destination="iris",y="4",ntrees=10,max_depth=8,learn_rate=.2,min_rows=10)
print(iris.gbm)
