# Initialize H2O and check/install correct version of H2O R package
library(h2o)
localH2O = h2o.init(ip = "127.0.0.1", port = 54321)

# For hands-off demo of H2O vs. R
# H2O Import, Summary and GLM of small airlines data set on local machine
airlines.hex = h2o.importURL(localH2O, path = "https://raw.github.com/0xdata/h2o/master/smalldata/airlines/allyears2k_headers.zip", key = "airlines.hex")
summary(airlines.hex)
x_ignore = c("IsArrDelayed", "ActualElapsedTime", "ArrDelay", "DepDelay", "Canceled", "Diverted", "IsDepDelayed", "DepTime","ArrTime", "Cancelled", "CancellationCode", "CarrierDelay", "WeatherDelay","NASDelay", "SecurityDelay","TailNum", "LateAircraftDelay", "TaxiIn","TaxiOut")

myX = setdiff(colnames(airlines.hex), x_ignore)
airlines.glm = h2o.glm(x = myX, y = "IsArrDelayed", data = airlines.hex, family = "binomial", nfolds = 10, alpha = 0.5)
print(airlines.glm)

# For hands-on demo of running H2O remotely
# H2O Import, Summary and GLM of large airlines data set on remote machine
remoteH2O = h2o.init(ip = "192.168.1.161", port = 54329)

airlines_big.hex = h2o.importFile(remoteH2O, path = "/home/earl/./oldairlines/airlines.orig.all.withheader.25.csv", key = "airlines_big.hex")
summary(airlines_big.hex)
airlines_big.glm = h2o.glm(x = myX, y = "IsArrDelayed", data = airlines_big.hex, family = "binomial", nfolds = 10, alpha = 0.5)
print(airlines_big.glm)

# Still in Beta! H2O Data Munging on large airlines data set
head(airlines_big.hex, n = 10)
tail(airlines_big.hex)
summary(airlines_big.hex$ArrDelay)
airlines_small.data = as.data.frame(airlines_big.hex[1:10000,])
glm(IsArrDelayed ~ Origin + Dest, family = binomial, data = airlines_small.data)
