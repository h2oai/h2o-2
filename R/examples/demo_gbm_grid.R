##
## Gridded GBM Accessing
## Problem Type: Binary Classification
##
## The demo incorporates a simple GBM grid search. The demo includes:
##
##  1. Extract all models
##  2. Extract the best model
##  3. Extract the second best model
##  4. Predict on test data (for each model in the grid)
##  5. Extract auc's on test data (for each model in the grid)
##  6. Extract the best model based on step 5.
##  7. Extract the parameters of the best model extracted in step 6.
##  8. Extract the variable importances for the best model.

library(h2o)

myIP <- "127.0.0.1"
myPort <- 54321
h <- h2o.init()

myData <- h2o.importFile(h, "https://raw.githubusercontent.com/0xdata/h2o/master/smalldata/logreg/prostate.csv")
myTestData <- myData  # just use the training data for demo


# Grid over depth 1:5
m <- h2o.gbm(y = 2, x = 3:9, data = myData, interaction.depth = 1:5, n.trees = 500, shrinkage = 1 / 500, importance=TRUE)



# Retrieve all of the models
# A user-defined function to be ported into upcoming versions of H2O
model <- function(h2o.grid) h2o.grid@model

##
## STEP 1: Extract all models
##
# `gbms` is a `list` of h2o models, each of which has an `@model` component
gbms <- model(m)

##
## STEP 2: Extract the best model
##
# The best gbm is the first element in the `gbms` list:
best <- gbms[[1]]  # `gbms` is a bonified R `list` object

##
## STEP 3: Extract the second best model
##
# The second best gbm is the second element in the `gbms` list:
second_best <- gbms[[2]]

# The third best gbm is the third element in the `gbms` list... and so on!

##
## STEP 4: Predict on test data with each model in the grid
##
# Let's take each model from the grid and predict on some test data
prediction.list <- lapply(gbms, h2o.predict, myTestData)

# `prediction.list` is a `list` of H2O data frames, one for each of the gbm models in the `gbms` list.

##
## STEP 5: Extract AUCs on the test data for each model
##
# Let's get the AUC on the test data:
aucs <- lapply(prediction.list, function(predicted, actual) { h2o.performance(predicted[,3], actual$CAPSULE)@model$auc}, myTestData)

# print the AUCs
aucs

##
## STEP 6a: Extract the best model based on step 5. Retrieve the ID
##
# retrieve the model id that did best on `myTestData`
best_model_id <- max(unlist(aucs), index=TRUE)

##
## STEP 6b: Extract the best model based on step 5. Retrieve the model.
##
# get the best model from the grid search
best_model <- gbms[[best_model_id]]

##
## Step 7: Extract the winning model's parameters.
##
# look at the parameters of the best model
best_model@model$params


##
## Step 8: Extract the variable importance for the best model.
##
# look at the variable importance
best_model@model$varimp

