# This is a demo of H2O's K-Means function
# It imports a data set, parses it, and prints a summary
# Then, it runs K-Means with k = 5 centers on a subset of characteristics
# Note: This demo runs H2O on localhost:54321
library(h2o)
h2o.installDepPkgs()
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, promptUpgrade = FALSE)

prostate.hex = h2o.importFile(localH2O, system.file("extdata", "prostate.csv", package="h2oRClient"), "prostate.hex")
summary(prostate.hex)
prostate.km = h2o.kmeans(prostate.hex, centers = 10, cols = c("AGE","RACE","GLEASON","CAPSULE","DCAPS"))
print(prostate.km)

# Plot categorized data
# if(!"fpc" %in% rownames(installed.packages())) install.packages("fpc")
if("fpc" %in% rownames(installed.packages())) {
  library(fpc)
  prostate.data = as.data.frame(prostate.hex)
  prostate.clus = as.data.frame(prostate.km@model$cluster)
  par(mfrow=c(1,1))
  plotcluster(prostate.data, prostate.clus$response)
  title("K-Means Classification for k = 10")
}

# if(!"cluster" %in% rownames(installed.packages())) install.packages("cluster")
if("cluster" %in% rownames(installed.packages())) {
  library(cluster)
  clusplot(prostate.data, prostate.clus$response, color = TRUE, shade = TRUE)
}
pairs(prostate.data[,c(2,3,7,8)], col=prostate.clus$response)

# Plot k-means centers
par(mfrow = c(1,2))
prostate.ctrs = as.data.frame(prostate.km@model$centers)
plot(prostate.ctrs[,1:2])
plot(prostate.ctrs[,3:4])
title("K-Means Centers for k = 10", outer = TRUE, line = -2.0)
