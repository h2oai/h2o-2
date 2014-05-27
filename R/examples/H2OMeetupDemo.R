# Initialize H2O and check/install correct version of H2O R package
library(h2o)
localH2O = h2o.init(ip = "127.0.0.1", port = 54321)

# For hands-on audience participation
# H2O Import, Summary, GLM and K-Means on prostate cancer data set
# prostate.hex = h2o.importFile(localH2O, path = "../../smalldata/logreg/prostate.hex", key = "prostate.hex")
prostate.hex = h2o.importURL(localH2O, path = "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", key = "prostate.hex")
summary(prostate.hex)
prostate.glm = h2o.glm(x = c("AGE","RACE","PSA","GLEASON"), y = "CAPSULE", data = prostate.hex, family = "binomial", nfolds = 10, alpha = 0.5)
print(prostate.glm)
prostate.km = h2o.kmeans(data = prostate.hex, centers = 5, cols = c("AGE","RACE","GLEASON","CAPSULE","PSA"))
print(prostate.km)

# Still in Beta! H2O Data Munging on prostate cancer data set
head(prostate.hex, n = 10)
tail(prostate.hex)
summary(prostate.hex$AGE)
summary(prostate.hex[prostate.hex$AGE > 67,])
prostate.small = as.data.frame(prostate.hex[1:200,])
glm(CAPSULE ~ AGE + RACE + DPROS + DCAPS, family = binomial, data = prostate.small)

# R Import, Summary, GLM and K-Means on prostate cancer data set
prostate.data = read.csv(text = getURL("https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv"), header = TRUE)
# prostate.data = read.csv("../../smalldata/logreg/prostate.csv", header = TRUE)
summary(prostate.data)
prostate.glm2 = glm(CAPSULE ~ AGE + RACE + PSA + GLEASON, family = binomial, data = prostate.data)
print(prostate.glm2)
prostate.km2 = kmeans(prostate.data[c("AGE","RACE","GLEASON","CAPSULE","PSA")], centers = 5)
print(prostate.km2)
