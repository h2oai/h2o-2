## Install H2O R package (same version as H2O Server)
if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }
install.packages("h2o", repos=(c("http://h2o-release.s3.amazonaws.com/h2o/rel-mirzakhani/2/R", getOption("repos"))))

## Load H2O R libary
library(h2o)

## Connect to H2O Server (if needed, replace localhost with output of `boot2docker ip`)
h2oServer <- h2o.init(ip="localhost", port=8996)



## HIGGS

## Parse Higgs dataset
higgs_hex <- h2o.importFile(h2oServer, "higgs.100k.csv.gz")
summary(higgs_hex)
dim(higgs_hex)

## Split data into train/validation and store under user-given keys in the H2O Store
random <- h2o.runif(higgs_hex, seed = 123456789)
higgs_train_hex <- h2o.assign(higgs_hex[random < .75,], "higgs_train_hex")
higgs_validation_hex  <- h2o.assign(higgs_hex[random >= .75,], "higgs_validation_hex")

## Clean up temporaries
h2o.rm(h2oServer, grep(pattern = "Last.value", x = h2o.ls(h2oServer)$Key, value = TRUE))


## Run DL model
response = 1
low_level_predictors = c(2:22)
high_level_predictors = c(23:29)

help(h2o.deeplearning)

ll_model <- h2o.deeplearning(data=higgs_train_hex, validation=higgs_validation_hex, 
                             x=low_level_predictors, y=response)
ll_model

hl_model <- h2o.deeplearning(data=higgs_train_hex, validation=higgs_validation_hex, 
                             x=high_level_predictors, y=response)
hl_model

ll_hl_model <- h2o.deeplearning(data=higgs_train_hex, validation=higgs_validation_hex, 
                                x=c(low_level_predictors,high_level_predictors), y=response)
ll_hl_model

## Example how to make and export predictions
predictions <- h2o.predict(hl_model, higgs_validation_hex)
head(predictions)
h2o.exportFile(data=predictions, path="predictions")



## ADULT

## Run grid search on Adult
adult_hex <- h2o.importFile(h2oServer, "adult.gz")
dim(adult_hex)
summary(adult_hex)


## Set column names manually
colnames(adult_hex) <- c("age","workclass","fnlwgt","education","education-num","marital-status",
                         "occupation","relationship","race","sex","capital-gain","capital-loss",
                         "hours-per-week","native-country","income")
summary(adult_hex)

## Turn age into a categorical factor
adult_hex$age <- as.factor(adult_hex$age)
summary(adult_hex)

pred = 1:14
adult_grid <- h2o.deeplearning(data=adult_hex, x=pred, y="income",
                               activation=c("Rectifier"), l1=c(0,1e-6), l2=0,
                               hidden=list(c(20,20,20), c(50,50)), epochs=10)

## Find the best model (based on training error, since no validation dataset specified)
best_model <- adult_grid@model[[1]]
best_model
best_model@model$params$activation
best_model@model$params$l1
best_model@model$params$hidden

## Compute N-fold CV error for the best model
cv_model <- h2o.deeplearning(data=adult_hex, x=pred, y="income",
                             activation=c("Rectifier"), l1=best_model@model$params$l1,
                             hidden=best_model@model$params$hidden, epochs=10, nfold=5)
cv_model


## MNIST
## World-record parameters at http://learn.h2o.ai/content/hands-on_training/deep_learning.html

mnist_train_hex <- h2o.importFile(h2oServer, "mnist.train.csv.gz")
mnist_test_hex <- h2o.importFile(h2oServer, "mnist.test.csv.gz")
dim(mnist_train_hex)
dim(mnist_test_hex)

dl_model <- h2o.deeplearning(data=mnist_train_hex, validation=mnist_test_hex, hidden=c(20,20,20), epochs=1, x=1:784, y=785)
dl_model



## EXTRAS

## Compare to GBM
gbm_model <- h2o.gbm(data=mnist_train_hex, validation=mnist_test_hex, x=1:784, y=785, distribution="multinomial", n.trees=5)
gbm_model

## Run H2O K-Means
h2o.kmeans(data = mnist_train_hex[,1:784], centers = 50)

## Run R's K-Means (benchmark)
#kmeans(as.data.frame(mnist_train_hex[,1:784]), centers = 50)

## Run H2O PCA and plot variances of principal components
h2o_pca <- h2o.prcomp(data = mnist_train_hex[,1:784])
plot(h2o_pca@model$sdev^2)

## Upload Iris dataset from R's memory to the H2O Server and run H2O's K-Means
iris_hex <- as.h2o(h2oServer, iris)
summary(iris_hex)
h2o.kmeans(data = iris_hex[,1:4], centers = 3)

## Final cleanup
h2o.rm(h2oServer, grep(pattern = "Last.value", x = h2o.ls(h2oServer)$Key, value = TRUE))
h2o.ls(h2oServer)

## Go to http://localhost:8996 to inspect datasets, models, jobs, etc.
## For more examples, see http://learn.h2o.ai/content/