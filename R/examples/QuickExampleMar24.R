#Running H2O from terminal (using bleeding edge developer build)

#Start communication between H2O and R
library(h2o)
localH2O = h2o.init() 

#import an arbitrary data set
ceb.data.h2o<- h2o.uploadFile(localH2O, "Downloads/cebexpanded.csv")

#Suppose I want to treat MarriageDuration as a factor and not as real. 
#get the total number of columns in the data set and append new cols to the end. 
ncol(ceb.data.h2o)

#create a new column for the factor (instead of overwriting the existing column)
ceb.data.h2o[,10]<- as.factor(ceb.data.h2o[,1])


#create a new column as a function of data in existing columns - for example, mean centering the DependentVariable
ceb.data.h2o[,11]<- ceb.data.h2o[,9]-mean(ceb.data.h2o[,9])
summary(ceb.data.h2o)

#pull columns into H2O - once in R you can manipulate or test
mandv.ceb.R<- as.data.frame(ceb.data.h2o[,c(7,8)])
plot(mean.ceb.R[,1]~mean.ceb.R[,2])
new.col<- mean.ceb.R[,2]/mean.ceb.R[,1]

#pass back to H2O
ceb.data.h2o[,12]<- as.h2o(localH2O, new.col)
