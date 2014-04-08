#H2O-R script for Bay Area UseR group's meetup  
#Demoing data munging and model building

#Clear R 's var environment
rm(list=ls())

# Detach and remove old H2O package if it exists
if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }

# Install H2O from online repository
install.packages("h2o", repos=(c("http://h2o-release.s3.amazonaws.com/h2o/rel-kahan/5/R", getOption("repos")))) 

# Load H2O library
library(h2o)

# Start H2O with default or specified IP and port
myIP = '127.0.0.1'
myPort =  54321
local.h2o <- h2o.init(ip = myIP, port = myPort,Xmx="12g")

#Parsing data to H2O
system.time(air<-h2o.uploadFile.FV(local.h2o,"/Users/nidhimehta/Desktop/airlines.1987.2013.10p.csv.zip",key="air"))
dim(air)
colnames(air)
summary(air)

#Removing Columns from data frame
air_trim = h2o.assign(air[,setdiff(colnames(air), c("IsArrDelayed", "IsDepDelayed"))], key = "air_trim")
#air_trim <- h2o.assign(air[,-c(30,31)],key = "air_trim")
colnames(air_trim)
dim(air_trim)

#Range
range(air_trim$DepDelay)
range(air_trim$ArrDelay)

#Quantile
air.DepQs <- quantile(air_trim$DepDelay, prob = seq(0,1,.1), na.rm = T)
print(air.DepQs)
air.ArrQs <- quantile(air_trim$ArrDelay, prob = seq(0,1,.1), na.rm = T)
print(air.ArrQs)

#Cut
as.data.frame(h2o.table(h2o.cut(air_trim$DepDelay, breaks=c(-1000, -60, -15, 15, 60, 120, 180, 1500))))
as.data.frame(h2o.table(h2o.cut(air_trim$ArrDelay, breaks=c(-1000, -60, -15, 15, 60, 120, 180, 1500))))

#Outlier Removal
#outliers.indx = air$ArrDelay <= air.ArrQs[2] | air$ArrDelay >= air.ArrQs[10]
#arrdelay.outliers = h2o.assign(air[!outliers.indx,], "arrdelay.outliers")

#Adding columns to a Data frame
air_trim$IsDepDelayed <- ifelse(!is.na(air_trim$DepDelay) & air_trim$DepDelay > 15 , 1, 0)
air_trim$IsArrDelayed <- ifelse(!is.na(air_trim$ArrDelay) & air_trim$ArrDelay > 15 , 1, 0)

summary(air_trim)

# To Factor
air_trim$IsArrDelayed <- as.factor(air_trim$IsArrDelayed) 
air_trim$IsDepDelayed <- as.factor(air_trim$IsDepDelayed) 
str(air_trim)

#Freeing some memory
h2o.ls(local.h2o)
keys_to_remove = grep("Last\\.value\\.", x=h2o.ls(local.h2o)$Key, value=TRUE)
keys_to_remove
h2o.rm(local.h2o, keys_to_remove)
h2o.ls(local.h2o)

#Ddply
# Mean Departure delay by day of week
fun2 = function(df) { mean(df$DepDelay)}
h2o.addFunction(local.h2o, fun2)
res = h2o.ddply(air, "DayOfWeek", fun2)
colnames(res) = c("DayOfWeek", "Avg_DepDelay")
as.data.frame(res)    # Convert H2O dataset to R data frame

# Avg distance betwwen airports
#fun = function(df) { mean<- mean(df$Distance);sd <-sd(df$Distance) }
fun = function(df) { cbind( mean(df$Distance),sd(df$Distance), length(df$Distance) ) }
h2o.addFunction(local.h2o, fun)
system.time(res <- h2o.ddply(air, c("Origin","Dest"), fun))
avg_dis = as.data.frame(res)
head(avg_dis)
colnames(avg_dis) = c("Origin", "Dest", "Avg_Distance", "sd", "count")
cbind(avg_dis[order(avg_dis$Avg_Distance, decreasing = F)[1:10],], avg_dis[order(avg_dis$Avg_Distance, decreasing = T)[1:10],])

#How many departure delay records are misssing (i.e NA) per year
fun3 = function(df) { sum(is.na(df$DepDelay))}
h2o.addFunction(local.h2o, fun3)
res = h2o.ddply(air, "Year", fun3)
result = as.data.frame(res)    # Convert H2O dataset 
colnames(result) = c("YEAR", "Count_of_NAs")
cbind(result[order(result$Count_of_NAs, decreasing = F)[1:10],], result[order(result$Count_of_NAs, decreasing = T)[1:10],])

#Some Modeling

#Spliting Dataset into Train and validation sets
system.time(s <- h2o.runif(air))    # Useful when number of rows too large for R to handle
air.train = h2o.assign(air[s <= 0.8,], key = "air.train" )
air.valid = h2o.assign(air[s > 0.8,], key = "air.valid" )
colnames(air.train)

#Parsing test file
system.time(air.test<-h2o.uploadFile.FV(local.h2o,"/Users/nidhimehta/Desktop/airlines.1987.2013.05p.csv.zip",key="air.test"))
dim(air.test)

myX = c("Origin", "Dest", "Distance", "UniqueCarrier", "Month", "DayofMonth", "DayOfWeek", "Year" )
myY="IsDepDelayed"

# Run GBM on training and validation set 
air.gbm = h2o.gbm(x = myX, y = myY, distribution = "multinomial", data = air.train, n.trees = 5,n.minobsinnode=1, 
                  interaction.depth = 2, shrinkage = 0.01, n.bins = 20, validation = air.valid, importance = T)
air.gbm

# Run RF on training and validation set 
air.rf=h2o.randomForest.FV(x=myX,y=myY,data=air.train,ntree=5,
                           depth=20,seed=12,importance=T,validation=air.valid)
air.rf
air.rf@model
barplot(as.matrix(air.rf@model$varimp[1,]))
air.rf@model$auc

model_object=air.rf #air.gbm , air.glm

#predicting on test file 
pred = h2o.predict(model_object,air.test)
head(pred)

#Building confusion matrix
CM=h2o.confusionMatrix(pred$predict,air.test$IsDepDelayed)
print(CM)
dim(air.test)

#Plot ROC for test set
perf = h2o.performance(pred$YES,air.test$IsDepDelayed )
print(perf)
perf@model$precision
perf@model$accuracy
perf@model$auc
plot(perf,type="roc")

h2o.shutdown(local.h2o)

#---------------------------------------------------------------

#MultiNode demo
rm(list=ls())

myIP = "192.168.1.163"   
myPort =  54545
remote.h2o = h2o.init(ip = myIP, port = myPort)
#remote.h2o = h2o.init()

system.time(air <- h2o.importFile(remote.h2o,"/home/0xdiag/datasets/airlines/airlines_all.csv",key = "air"))
dim(air)
colnames(air)
summary(air)

myX = c("Origin", "Dest", "Distance", "UniqueCarrier", "Month", "DayofMonth", "DayOfWeek", "Year" )
myY="IsDepDelayed"

air.glm = h2o.glm.FV(x=myX, y=myY, data=air, family="binomial", nfolds=0,standardize=T)
air.glm
model_object=air.glm

#--------------------------------

