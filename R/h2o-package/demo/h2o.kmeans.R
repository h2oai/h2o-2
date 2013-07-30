# This is a demo of H2O's K-Means function
# It imports a data set, parses it, and prints a summary
# Then, it runs K-Means with k = 5 centers on a subset of characteristics
library(h2o)
h2o = new("H2OClient", ip="localhost", port=54321)

prostate.hex = importFile(h2o, system.file("extdata", "prostate.csv", package="h2o"), "prostate.hex")
summary(prostate.hex)
prostate.km = h2o.kmeans(prostate.hex, centers = 5, cols = c("AGE","RACE","GLEASON","CAPSULE","DCAPS"))
print(prostate.km)