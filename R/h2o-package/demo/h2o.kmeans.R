# This is a demo of H2O's K-Means function
# It imports a data set, parses it, and prints a summary
# Then, it runs K-Means with k = 5 centers on a subset of characteristics
library(h2o)
localH2O = new("H2OClient", ip = "localhost", port = 54321)
h2o.checkClient(localH2O)

prostate.hex = h2o.importFile(localH2O, path = system.file("extdata", "prostate.csv", package="h2o"), key = "prostate.hex")
summary(prostate.hex)
prostate.km = h2o.kmeans(data = prostate.hex, centers = 5, cols = c("AGE","RACE","GLEASON","CAPSULE","DCAPS"))
print(prostate.km)