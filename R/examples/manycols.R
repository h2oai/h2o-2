# Install and Launch H2O R package
if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }
install.packages("h2o", repos=(c("http://h2o-release.s3.amazonaws.com/h2o/h2o-parsemanycols/5/R", getOption("repos"))))
library(h2o)

# Connect to cluster (14 nodes with -Xmx 50g each)

# Launch H2O Cluster with YARN on HDP2.1
#wget http://h2o-release.s3.amazonaws.com/h2o/h2o-parsemanycols/5/h2o-2.9.0.5.zip
#unzip h2o-2.9.0.5.zip
#cd h2o-2.9.0.5/hadoop
#hadoop fs -rmr myDir
#hadoop jar h2odriver_hdp2.1.jar water.hadoop.h2odriver -libjars ../h2o.jar -n 14 -mapperXmx 50g -output myDir -baseport 61111

h2oCluster <- h2o.init(ip="mr-0xd1", port=61111)

# Read data from HDFS
data.hex <- h2o.importFile(h2oCluster, "hdfs://mr-0xd6/datasets/15Mx2.2k.csv")

# Create 80/20 train/validation split
random <- h2o.runif(data.hex, seed = 123456789)
train <- h2o.assign(data.hex[random < .8,], "X15Mx2_2k_part0.hex")
valid <- h2o.assign(data.hex[random >= .8,], "X15Mx2_2k_part1.hex")

# Delete full data and temporaries
h2o.rm(h2oCluster, "15Mx2_2k.hex")
h2o.rm(h2oCluster, grep(pattern = "Last.value", x = h2o.ls(h2oCluster)$Key, value = TRUE))

response=2 #1:1000 imbalance
predictors=c(3:ncol(data.hex))

# Start modeling

# GLM
mdl.glm <- h2o.glm(x=predictors, y=response, data=train, lambda_search=T, family="binomial", max_predictors=100) #nfolds=5 is optional
mdl.glm

# compute validation error for GLM
pred.glm <- h2o.predict(mdl.glm, valid)
h2o.performance(pred.glm[,3], valid[,response], measure="F1")

# Random Forest
mdl.rf  <- h2o.randomForest(x=predictors, y=response, data=train, validation=valid, type="BigData", depth=15, importance=T, balance.classes = T, class.sampling.factors = c(1,250)) 
mdl.rf

# Gradient Boosted Trees
mdl.gbm <- h2o.gbm(x=predictors, y=response, data=train, validation=valid, importance=T, balance.classes = T, class.sampling.factors = c(1,250))
mdl.gbm