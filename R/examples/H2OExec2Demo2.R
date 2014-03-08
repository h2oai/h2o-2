library(h2o)
myIP = "192.168.1.161"; myPort = 60024
# myIP = "23.22.188.100"; myPort = 54321
remoteH2O = h2o.init(ip = myIP, port = myPort, startH2O = FALSE)

# Import airlines dataset to H2O
# airPath = "hdfs://192.168.1.161/datasets/airlines.clean/earl/original/2007.csv.gz"
# airPath = "s3n://h2o-airlines-unpacked/allyears.1987.2013.csv"
airPath = "/home/tomk/airlines_all.csv"
airlines.hex = h2o.importFile.FV(remoteH2O, path = airPath, key = "airlines.hex")

# Print out basic summary
str(airlines.hex)
summary(airlines.hex)
head(airlines.hex)
tail(airlines.hex)

# Display count of State column's levels
colnames(airlines.hex)
# dest.count = h2o.table(airlines.hex$Dest)
# as.data.frame(dest.count)
origindest.count = h2o.table(airlines.hex[,c("Origin", "Dest")])
head(origindest.count)

# Extract small sample (with replacement)
airlines.samp = airlines.hex[sample(1:nrow(airlines.hex), 500),]
airlines.samp.df = as.data.frame(airlines.samp)
# myY = "ArrDelay"; myX = c("Origin", "Dest", "Distance", "FlightNum", "UniqueCarrier", "Month", "DayofMonth", "DayOfWeek", "CRSElapsedTime")
# airlines.glm = h2o.glm.FV(x = myX, y = myY, data = airlines.samp, family = "gaussian")
myY = "IsArrDelayed"
myX = c("Year", "Month", "DayofMonth", "DayOfWeek", "CRSDepTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "CRSElapsedTime", "Origin", "Dest", "Distance")
airlines.glm = h2o.glm.FV(x = myX, y = myY, data = airlines.hex, family = "binomial")
print(airlines.glm)

# Get quantiles and examine outliers
airlines.qs = quantile(airlines.hex$ArrDelay, probs = c(0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95), na.rm = TRUE)
print(airlines.qs)

# Note: Right now, assignment must be done manually with h2o.assign!
outliers.ind = airlines.hex$ArrDelay <= airlines.qs["5%"] | airlines.hex$ArrDelay >= airlines.qs["95%"]
temp = airlines.hex[outliers.ind,]
arrdelay.outliers = h2o.assign(temp, "arrdelay.outliers")
nrow(arrdelay.outliers)
head(arrdelay.outliers)

# Drop outliers from data
temp = airlines.hex[!outliers.ind,]
airlines.trim = h2o.assign(temp, "airlines.trim")
nrow(airlines.trim)

# Construct test and training sets
# s = runif(nrow(airlines.hex))
s = h2o.runif(airlines.hex, min = 0, max = 1)    # Use if number of rows too large for R to handle
temp = airlines.hex[s <= 0.8,]
airlines.train = h2o.assign(temp, "airlines.train")
temp = airlines.hex[s > 0.8,]
airlines.test = h2o.assign(temp, "airlines.test")
nrow(airlines.train) + nrow(airlines.test)

# Run GBM on training set and predict on test set
airlines.gbm = h2o.gbm(x = myX, y = myY, distribution = "multinomial", data = airlines.train, validation = airlines.test, n.trees = 5, interaction.depth = 3)
print(airlines.gbm)
airlines.pred = h2o.predict(airlines.gbm, airlines.test)
summary(airlines.pred)

# Create new column based on 75% quantile
# newCol = ncol(airlines.hex)+1
# airlines.hex[,newCol] = airlines.hex$DepTime - airlines.hex$CRSDepTime >= 15
# airlines.hex[,newCol+1] = airlines.hex$ArrTime - airlines.hex$CRSArrTime >= 15
airlines.hex$ArrDelay_High = airlines.hex$ArrDelay > airlines.qs["75%"]
# airlines.hex$DepDelay_High = airlines.hex$DepDelay > quantile(airlines.hex$DepDelay, probs = 0.75, na.rm = TRUE)
head(airlines.hex)
airlines.glm.lin = h2o.glm.FV(y = "ArrDelay_High", x = myX, data = airlines.hex, family = "binomial", nfolds = 2)
print(airlines.glm.lin)

# Beta: Demonstrate simple ddply implementation of mean over groups
fun = function(df) { sum(df[,16])/nrow(df) }
h2o.addFunction(remoteH2O, fun)
airlines.filt = airlines.hex[!is.na(airlines.hex$DepDelay),]
airlines.ddply = h2o.ddply(airlines.filt, "DayOfWeek", fun)
as.data.frame(airlines.ddply)