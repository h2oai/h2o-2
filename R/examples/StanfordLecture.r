# Detach and remove old H2O package if it exists
detach("package:h2o", unload = TRUE)
remove.packages("h2o", .libPaths())

# Install H2O from online repository
install.packages("h2o", repos=(c("http://s3.amazonaws.com/h2o-release/h2o/rel-jordan/3/R", getOption("repos"))))

# Load H2O library
library(h2o)

# Start H2O with default or specified IP and port
myIP = "192.168.1.163"   
myPort =  54321
remote.h2o = h2o.init(ip = myIP, port = myPort)
# remote.h2o = h2o.init()

# Upload data (http://stat-computing.org/dataexpo/2009/)
air = h2o.importFile(remote.h2o, "/home/nidhi/air/all_yr_air.csv", key = "air.hex")
# air = h2o.uploadFile(remote.h2o, "/Users/nidhimehta/Downloads/2008.csv", key = "air.hex")
# air = h2o.importURL(remote.h2o, "https://s3.amazonaws.com/h2o-airlines-unpacked/allyears2k.csv", key = "air.hex")

# Inspect data. (Note that air is an H2O parsed data object).
dim(air)
colnames(air)
head(air)
summary(air)

# Remove rows with any NAs 
air_noDepDelayedNAs = air[!is.na(air$DepDelay)]
dim(air_noDepDelayedNAs)

# Add a column to H2O dataset indicating whether flight was delayed
minutesOfDelayWeTolerate = 15
air_noDepDelayedNAs$IsDepDelayed = air_noDepDelayedNAs$DepDelay > minutesOfDelayWeTolerate
colnames(air_noDepDelayedNAs)
head(air_noDepDelayedNAs)

# Run logistic regression to predict whether a departure is delayed
myX = c("Origin", "Dest", "Distance", "UniqueCarrier", "Month", "DayofMonth", "DayOfWeek", "CRSElapsedTime")
myY = "IsDepDelayed"
air.glm = h2o.glm.FV(x = myX, y = myY, data = air_noDepDelayedNAs, family = "binomial", nfolds = 1, alpha = 0.25, lambda = 0.001)
print(air.glm)
sort(air.glm@model$coefficients)
air.glm@model$confusion

# Beta: Example of simple mean with ddply
fun = function(df) { sum(df[,16], na.rm = T)/nrow(df) }
h2o.addFunction(remote.h2o, fun)
res = h2o.ddply(air_noDepDelayedNAs, "DayOfWeek", fun)
as.data.frame(res)    # Convert H2O dataset to R data frame