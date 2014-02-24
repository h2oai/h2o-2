# earl's demo file; does a logistic regression and a gbm on airlines plus other random useful things


#Config for LAPTOP: ip="localhost"; port=54321
#Config for 0xservers: launch a cluster on the servers and put an IP and Port for one of them. DO NOT LAUNCH ON STANDARD PORT NUMBER - instead launch on weird port numbers that start with 5...
#Config for S3: launch on S3

# configuration you may have to set:
ip = '127.0.0.1'; port = 55555

sessionInfo()

# standard preamble to connect to h2o
library( h2o )
h2o <- h2o.init(ip=ip, port=port, startH2O=F)


#PATH TO DATA ON COMPUTER: (USE h2o.importFile):/.../CliffDemo/airlines.1987.2013.05p.csv.zip)
#PATH TO DATA ON 0XSERVER (USE h2o.importFile): .../home/earl/newairlines/fixed
#PATH TO DATA ON S3: (USE h2o.importHDFS) s3n://h2o-airlines-unpacked/allyears.csv

airlines <- h2o.import#CHOOSE FROM ABOVE (h2o, path='/Users/earl/shared/20130926_data/airlines.1987.2013.05p.csv.zip')

summary(airlines)
head(airlines)

Y <- 'IsArrDelayed'
X <- c('Year', 'Month', 'DayofMonth', 'DayOfWeek', 'CRSDepTime', 'CRSArrTime', 'UniqueCarrier', 'FlightNum', 'CRSElapsedTime', 'Origin', 'Dest', 'Distance')

n.trees <- 5
model0 <- h2o.gbm(y=Y, x=X, distribution='multinomial', data=airlines, n.trees=n.trees, interaction.depth=3)
model0@model$confusion

preds <- h2o.predict(model0, newdata=airlines)

summary(preds)





# glm - logistic regression
#prostate.hex <- h2o.importURL(h2o, path = "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", key = "prostate.hex")
prostate.hex <- h2o.importFile(h2o, path = "/Users/earl/work/h2o/smalldata/logreg/prostate.csv", key = "prostate.hex")

# lets poke at the data a bit:
c( nrow(prostate.hex), ncol(prostate.hex) )
summary( prostate.hex )
head( prostate.hex )
tail( prostate.hex )

# notice that RACE imports as an integer in {0,1,2}; we want it to be a factor
prostate.hex$RACE = as.factor(prostate.hex$RACE)

prostate.glm = h2o.glm(y = "CAPSULE", x = c("AGE","RACE","PSA","GLEASON"), data = prostate.hex, family = "binomial", nfolds = 10, alpha = 0.5)
print(prostate.glm)
# note RACE.1, RACE.2

# predicting on itself, but let's get a prediction out
prostate.pred <- h2o.predict(prostate.glm, prostate.hex)
summary(prostate.pred)


prostate.rf <- h2o.randomForest(y='CAPSULE', data=prostate.hex, ntree=10, depth=5)





