source('./Utils/h2oR.R')

logging("\n======================== Begin Test ===========================\n")
serverH2O = new("H2OClient", ip=myIP, port=myPort)
checkKMModel <- function(myKM.h2o, myKM.r) {
  # checkEqualsNumeric(myKM.h2o@model$size, myKM.r$size, tolerance = 1.0)
  # checkEqualsNumeric(myKM.h2o@model$withinss, myKM.r$withinss, tolerance = 1.5)
  # checkEqualsNumeric(myKM.h2o@model$tot.withinss, myKM.r$tot.withinss, tolerance = 1.5)
  # checkEqualsNumeric(myKM.h2o@model$centers, myKM.r$centers, tolerance = 0.5)
}

# Test k-means clustering on benign.csv
test.km.benign <- function(serverH2O) {
  cat("\nImporting benign.csv data...\n")
  # benign.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/benign.csv")
  # benign.hex = h2o.importFile(serverH2O, normalizePath("../../smalldata/logreg/benign.csv"))
  benign.hex = h2o.uploadFile(serverH2O, "../../smalldata/logreg/benign.csv")
  benign.sum = summary(benign.hex)
  print(benign.sum)
  
  # benign.data = read.csv(text = getURL("https://raw.github.com/0xdata/h2o/master/smalldata/logreg/benign.csv"), header = TRUE)
  benign.data = read.csv("../../smalldata/logreg/benign.csv", header = TRUE)
  benign.data = na.omit(benign.data)
  
  for(i in 1:5) {
    cat("\nH2O K-Means with", i, "clusters:\n")
    benign.km.h2o = h2o.kmeans(data = benign.hex, centers = i)
    print(benign.km.h2o)
    benign.km = kmeans(benign.data, centers = i)
    checkKMModel(benign.km.h2o, benign.km)
  }
}

# Test k-means clustering on prostate.csv
test.km.prostate <- function(serverH2O) {
  cat("\nImporting prostate.csv data...\n")
  # prostate.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", "prostate.hex")
  # prostate.hex = h2o.importFile(serverH2O, normalizePath("../../smalldata/logreg/prostate.csv"))
  prostate.hex = h2o.uploadFile(serverH2O, "../../smalldata/logreg/prostate.csv")
  prostate.sum = summary(prostate.hex)
  print(prostate.sum)
  
  # prostate.data = read.csv(text = getURL("https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv"), header = TRUE)
  prostate.data = read.csv("../../smalldata/logreg/prostate.csv", header = TRUE)
  prostate.data = na.omit(prostate.data)
  
  for(i in 5:8) {
    cat("\nH2O K-Means with", i, "clusters:\n")
    prostate.km.h2o = h2o.kmeans(data = prostate.hex, centers = i, cols = "2")
    print(prostate.km.h2o)
    prostate.km = kmeans(prostate.data[,3], centers = i)
    checkKMModel(prostate.km.h2o, prostate.km)
  }
}

test.km.benign(serverH2O)
test.km.prostate(serverH2O)
