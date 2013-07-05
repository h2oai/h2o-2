# Compare performance of H2O GLM with R library glmnet
library(glmnet)
source("../R/H2O.R")
h2o.SERVER="localhost:54321"

rob.data <- read.table("../smalldata/categoricals/rob.txt", header = FALSE, sep = " ")
rob.data <- data.matrix(rob.data)

# Single run of GLM
# rob.glmnet <- glmnet(rob.data[,-c(1953)], rob.data[,1953], family = "binomial", lambda = rev(seq(0,300,3)))
system.time(glmnet(rob.data[,-c(1953)], rob.data[,1953], family = "binomial", lambda = rev(seq(0,300,3))))

# 10-fold cross-validation GLM
# rob.cv <- cv.glmnet(rob.data[,-c(1953)], rob.data[,1953], family = "binomial", nfolds = 14, lambda = rev(seq(0,300,3)))
system.time(cv.glmnet(rob.data[,-c(1953)], rob.data[,1953], family = "binomial", nfolds = 10, lambda = rev(seq(0,300,3))))

h2o.importFile(rob, "../smalldata/categoricals/rob.txt")
rob.glm <- h2o.glm(rob.hex, y = 1952, family = "binomial", nfolds = 10)
h2o.job_time(rob.glm$key)