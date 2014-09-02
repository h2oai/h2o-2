# This Demo shows how to access Variable Importance from different H2O algorithms namely, GBM, Random Forest, GLM, Deeplearning.
# Data source: Data is obtained from -https://archive.ics.uci.edu/ml/datasets/Bank+Marketing
# Expectation:  The predictor "duration" should be picked as the most important variable by all algos

setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test <- function(h) {


# Parse data into H2O
print("Parsing data into H2O")
m_data = h2o.importFile(h,normalizePath(locate("smalldata/bank-additional-full.csv")), key="m_data")
print("Expectation: All Algos should pick the predictor - 'duration' as the most important variable")

# Run summary
summary(m_data)

#Print Column names
colnames(m_data)

# Specify predictors and response
myX = 1:20
myY="y"

# Run GBM with variable importance
my.gbm <- h2o.gbm(x = myX, y = myY, distribution = "bernoulli", data = m_data, n.trees =100,
                  interaction.depth = 2, shrinkage = 0.01, importance = T) 

# Access Variable Importance from the built model
gbm.VI = my.gbm@model$varimp
print("Variable importance from GBM")
print(gbm.VI)

par(mfrow=c(2,2))
# Plot variable importance from GBM
barplot(t(gbm.VI[1]),las=2,main="VI from GBM")

#--------------------------------------------------
# Run random Forest with variable importance
my.rf = h2o.randomForest(x=myX,y=myY,data=m_data,classification=T,ntree=100,importance=T)

# Access Variable Importance from the built model
rf.VI = my.rf@model$varimp
print("Variable importance from Random Forest")
print(rf.VI)

rf.VI = rf.VI[order(rf.VI[1,],decreasing=T)]
# RF variable importance Without normalization, i.e scale =T
print("Variable importance from Random Forest without normalization")
print(t(rf.VI[1,]))

# RF variable importance With normalization, i.e scale =T (divide mean decrease accuracy by standard deviation)
norm_rf.VI =my.rf@model$varimp[1,]/my.rf@model$varimp[2,]
# Sort in decreasing order
nrf.VI = norm_rf.VI[order(norm_rf.VI[1,],decreasing=T)]
print("Variable importance from Random Forest with normalization")
print(t(nrf.VI[1,]))

# Plot variable importance from Random Forest
barplot(t(nrf.VI[1,]),beside=T,names.arg=row.names(t(nrf.VI[1,])),las=2,main="VI from RF")

#--------------------------------------------------
# Run GLM with variable importance, lambda search and using all factor levels
my.glm = h2o.glm(x=myX, y=myY, data=m_data, family="binomial",standardize=T,use_all_factor_levels=T,higher_accuracy=T,lambda_search=T,return_all_lambda=T,variable_importances=T)

# Select the best model picked by glm
best_model = my.glm@best_model

# Get the normalized coefficients of the best model 
n_coeff = abs(my.glm@models[[best_model]]@model$normalized_coefficients)  

# Access Variable Importance by removing the intercept term
VI = abs(n_coeff[-length(n_coeff)])                                     

glm.VI = VI[order(VI,decreasing=T)]
print("Variable importance from GLM")
print(glm.VI)

# Plot variable importance from glm
barplot(glm.VI[1:20],las=2,main="VI from GLM") 

#--------------------------------------------------
# Run deeplearning with variable importance
my.dl = h2o.deeplearning(x=myX,y=myY,data=m_data,classification=T,activation="Tanh",hidden=c(10,10,10),epochs=12,variable_importances=T)

# Access Variable Importance from the built model
dl.VI =my.dl@model$varimp
print("Variable importance from Deep Learning")
print(dl.VI)

# Plot variable importance from deeplearing
barplot(t(dl.VI[1:20]),las=2,main="VI from Deep Learning")


testEnd()
}

doTest("Plot to compare the Variable Importance as predicted by different algorithms on the bank-marketing dataset", test)

