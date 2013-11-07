library(h2o)
h2o.installDepPkgs()
myIP = "127.0.0.1"; myPort = 54321
localH2O = h2o.init(ip = myIP, port = myPort, startH2O = TRUE, silentUpgrade = FALSE, promptUpgrade = TRUE)

# Import iris file to H2O
prosPath = system.file("extdata", "prostate.csv", package="h2oRClient")
prostate.hex = h2o.importFile.FV(localH2O, path = prosPath, key = "prostate.hex")

# Print out basic summary
summary(prostate.hex)
head(prostate.hex)
tail(prostate.hex)

# Get quantiles and examine outliers
prostate.qs = quantile(prostate.hex$PSA)
prostate.qs

# Note: Right now, assignment must be done manually with h2o.assign!
outliers.low = prostate.hex[prostate.hex$PSA <= prostate.qs[2],]
outliers.low = h2o.assign(outliers.low, "PSA.low")
outliers.high = prostate.hex[prostate.hex$PSA >= prostate.qs[10],]
outliers.high = h2o.assign(outliers.high, "PSA.high")

nrow(outliers.low) + nrow(outliers.high)
head(outliers.low); tail(outliers.low)
head(outliers.high); tail(outliers.high)

# Drop outliers from data
prostate.trim = prostate.hex[prostate.hex$PSA > prostate.qs[2],]
prostate.trim = h2o.assign(prostate.trim, "prostate.trim")
prostate.trim = prostate.trim[prostate.trim$PSA < prostate.qs[10],]
prostate.trim = h2o.assign(prostate.trim, "prostate.trim")
nrow(prostate.trim)

# Construct test and training sets
s = runif(nrow(prostate.hex))
prostate.train = prostate.hex[s <= 0.8,]
prostate.train = h2o.assign(prostate.train, "prostate.train")
prostate.test = prostate.hex[s > 0.8,]
prostate.test = h2o.assign(prostate.test, "prostate.test")
nrow(prostate.train) + nrow(prostate.test)

# Run GBM on training set and predict on test set
myY = "CAPSULE"; myX = setdiff(colnames(prostate.train), c(myY, "ID"))
prostate.gbm = h2o.gbm(x = myX, y = myY, distribution = "multinomial", data = prostate.train)
prostate.gbm
prostate.pred = h2o.predict(prostate.gbm, prostate.test)
summary(prostate.pred)
head(prostate.pred)
tail(prostate.pred)