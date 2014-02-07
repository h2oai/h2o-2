library(h2o)
myIP = "127.0.0.1"; myPort = 54321
localH2O = h2o.init(ip = myIP, port = myPort, startH2O = TRUE, silentUpgrade = FALSE, promptUpgrade = TRUE)

# Import prostate file to H2O
prosPath = system.file("extdata", "prostate.csv", package="h2oRClient")
prostate.hex = h2o.importFile(localH2O, path = prosPath, key = "prostate.hex")

# Print out basic summary
summary(prostate.hex)
head(prostate.hex)
tail(prostate.hex)

# Convert Race column to a factor variable
prostate.hex$RACE = as.factor(prostate.hex$RACE)
summary(prostate.hex)

# Display count of a column's levels
age.count = table(prostate.hex$AGE)    # Note: Currently only works on a single integer/factor column
# head(age.count)
as.data.frame(age.count)

# Extract small sample (without replacement)
prostate.samp = prostate.hex[sample(1:nrow(prostate.hex), 50),]
prostate.samp.df = as.data.frame(prostate.samp)    # Pull into R as a data frame
# glm(CAPSULE ~ AGE + PSA + VOL + GLEASON, family = binomial(), data = prostate.samp.df)
prostate.glm = h2o.glm.FV(x = c("AGE", "RACE", "PSA", "VOL", "GLEASON"), y = "CAPSULE", data = prostate.samp, family = "binomial")
print(prostate.glm)

# Get quantiles and examine outliers
prostate.qs = quantile(prostate.hex$PSA)
print(prostate.qs)

# Note: Right now, assignment must be done manually with h2o.assign!
PSA.outliers = prostate.hex[prostate.hex$PSA <= prostate.qs[2] | prostate.hex$PSA >= prostate.qs[10],]
# PSA.outliers.ind = prostate.hex$PSA <= prostate.qs[2] | prostate.hex$PSA >= prostate.qs[10]
# PSA.outliers = prostate.hex[PSA.outliers.ind,]
PSA.outliers = h2o.assign(PSA.outliers, "PSA.outliers")
nrow(PSA.outliers)
head(PSA.outliers)
tail(PSA.outliers)

# Drop outliers from data
prostate.trim = prostate.hex[prostate.hex$PSA > prostate.qs[2] & prostate.hex$PSA < prostate.qs[10],]
# prostate.trim = prostate.hex[!PSA.outliers.ind,]
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
prostate.gbm = h2o.gbm(x = myX, y = myY, distribution = "multinomial", data = prostate.train, validation = prostate.test)
print(prostate.gbm)
prostate.pred = h2o.predict(prostate.gbm, prostate.test)
summary(prostate.pred)
head(prostate.pred)
tail(prostate.pred)

# Create new column based on 25% and 75% quantiles
prostate.hex[,10] = prostate.hex$PSA <= prostate.qs["25%"]
head(prostate.hex)
# prostate.hex[,11] = prostate.hex$PSA >= prostate.qs["75%"]
prostate.glm.lin = h2o.glm.FV(y = 10, x = c("AGE", "RACE", "VOL", "GLEASON"), data = prostate.hex, family = "binomial")
print(prostate.glm.lin)

# Shutdown local H2O instance
h2o.shutdown(localH2O)