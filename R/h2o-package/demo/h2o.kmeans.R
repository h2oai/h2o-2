# This is a demo of H2O's K-Means function
# It imports a data set, parses it, and prints a summary
# Then, it runs K-Means with k = 5 centers on a subset of characteristics
library(h2o)
localH2O = new("H2OClient", ip="localhost", port=54321)
h2o.checkClient(localH2O)

prostate.hex = h2o.importFile(localH2O, system.file("extdata", "prostate.csv", package="h2o"), "prostate.hex")
summary(prostate.hex)
prostate.km = h2o.kmeans(prostate.hex, centers = 10, cols = c("AGE","RACE","GLEASON","CAPSULE","DCAPS"))
print(prostate.km)

# Plot categorized data
if(!"fpc" %in% rownames(installed.packages())) install.packages(fpc)
library(fpc)
prostate.data = as.data.frame(prostate.hex)
prostate.clus = as.data.frame(prostate.km@model$cluster)
par(mfrow=c(1,1))
plotcluster(prostate.data, prostate.clus$response)
title("K-Means Classification for k = 10")

if(!"cluster" %in% rownames(installed.packages())) install.packages(cluster)
library(cluster)
clusplot(prostate.data, prostate.clus$response, color = TRUE, shade = TRUE)
pairs(prostate.data[,c(2,3,7,8)], col=prostate.clus$response)

# Plot k-means centers
par(mfrow = c(1,2))
prostate.ctrs = as.data.frame(prostate.km@model$centers)
plot(prostate.ctrs[,1:2])
plot(prostate.ctrs[,3:4])
title("K-Means Centers for k = 10", outer = TRUE, line = -2.0)