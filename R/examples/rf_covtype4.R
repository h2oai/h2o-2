
library(h2o)

# debug files 
h2o.setLogPath(getwd(), "Command")
h2o.setLogPath(getwd(), "Error")
h2o.startLogging()

# in case connecting to existing cloud
ls
rm(list=ls())

local.h2o=h2o.init(Xmx="14g")
filePath <- "/home/0xdiag/datasets/standard/covtype.class4.10000.data"
cov=h2o.uploadFile(local.h2o,filePath,key="cov")

#spliting data into train/validation set
s = h2o.runif(cov)    # Useful when number of rows too large for R to handle
cov.train = cov[s <= 0.8,]
cov.valid = cov[s > 0.8,]
str(cov.train)

colnames(cov.train)
myX = 1:54
myY = 55

seed = 1234567890
depth = 20
ntree = 50
print("using balance.clsses=T in RF")
cov.rf=h2o.randomForest(x=myX,y=myY,data=cov.train,ntree=ntree, depth=depth,seed=seed,importance=T,validation=cov.valid, balance.classes=T)

print(cov.rf)
print(cov.rf@model)
print("All the below measures are reported on the validation set")
cov.rf@model$auc
cov.rf@model$confusion 
print("this confusion matrix is reported for threshold = 0.5 ")

print("The reported best cutoff is the threshold that maximizes F1 score for validation set")
cov.rf@model$best_cutoff 

print("All the below measures are reported for the best cutoff")
cov.rf@model$F1
cov.rf@model$accuracy
cov.rf@model$precision
cov.rf@model$recall
cov.rf@model$specificity
cov.rf@model$max_per_class_err

print("Reading in Test file")
filePath <- "/home/0xdiag/datasets/standard/covtype.class4.10000.data"
cov.test=h2o.uploadFile(local.h2o, filePath, key="cov.test")
model_object=cov.rf #cov.glm cov.rf cov.dl

pred = h2o.predict(model_object,cov.test) 
print(" The prediction object has 3 columns")
print("These are the prediction labels at threshold = 0.5")
head(pred$predict) # 
print("These are the probabilities for class label 0")
head(pred[,2])      # 
print("These are the probabilities for class label 1")
head(pred[,3])      #  

print("If you have a number as a class label then call it as head(pred$'0') head(pred$'1')")


perf = h2o.performance(pred[,3], cov.test$C55, measure = "F1")
print(" here the threshold used to calculate the performance measures is decided by maximizing F1 for the test set ")
perf@model$auc
perf@model$best_cutoff
perf@model$confusion

perf = h2o.performance(pred[,3], cov.test$C55, thresholds=cov.rf@model$best_cutoff )
print("here the threshold we use tis that wihich maximizes F1 for validaion set")
print(perf)
cov.rf@model$best_cutoff
perf@model$best_cutoff
perf@model$confusion
perf@model$precision
perf@model$accuracy
perf@model$auc
plot(perf,type="roc")


print("Adding a new prediction(0-1) column to the prediction table based on best threshold for maxF1 (on validation set)")
pred$Predicted_on_modelsBestCutoff = ifelse(pred[,3]< cov.rf@model$best_cutoff, 0, 1 )
head(pred)
pred$Predicted_on_modelsBestCutoff = as.factor(pred$Predicted_on_modelsBestCutoff)

print("This gives a weird formatted output because of inconsistency in the labels")
print("Matching the prior CM from h2o.performance() means we got the threshold right?")
CM=h2o.confusionMatrix(pred$Predicted_on_modelsBestCutoff,cov.test$C55)
print(CM)
