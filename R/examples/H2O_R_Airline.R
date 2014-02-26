# Small test in R to null columns and check if summary works or not
library(h2o)
myip = "127.0.0.1"
myport = 54321
remoteH2O = h2o.init(ip = myIP, port = myPort, startH2O = TRUE, silentUpgrade = FALSE, promptUpgrade = TRUE)
air = h2o.importFile(remoteH2O,"/Users/Jay/Desktop/20131226_data/airlines_all.05p.csv.gz",key="air")
air.glm = h2o.glm.FV(x = myX, y = "IsDepDelayed", data = air, family = "binomial", nfolds = 1, alpha = 0.25,lambda=0.001)
print(air.glm)
print(air.glm@model$confusion)
air$DEST <- NULL
summary(air) # This fails. 
head(air) # This fails,
tail(air) This fail
