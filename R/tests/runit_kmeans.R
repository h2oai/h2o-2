# Test k-means clustering in H2O
checkKMModel <- function(myKM.h2o, myKM.r) {
  # checkEqualsNumeric(myKM.h2o@model$size, myKM.r$size, tolerance = 1.0)
  # checkEqualsNumeric(myKM.h2o@model$withinss, myKM.r$withinss, tolerance = 1.5)
  # checkEqualsNumeric(myKM.h2o@model$tot.withinss, myKM.r$tot.withinss, tolerance = 1.5)
  # checkEqualsNumeric(myKM.h2o@model$centers, myKM.r$centers, tolerance = 0.5)
}

test.KM.benign <- function() {
  cat("\nImporting benign.csv data...\n")
  serverH2O = new("H2OClient", ip=myIP, port=myPort)
  benign.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/benign.csv")
  benign.sum = summary(benign.hex)
  print(benign.sum)
  
  benign.data = read.csv(text = getURL("https://raw.github.com/0xdata/h2o/master/smalldata/logreg/benign.csv"), header = TRUE)
  benign.data = na.omit(benign.data)
  
  for(i in 1:5) {
    cat("\nH2O K-Means with", i, "clusters:\n")
    benign.h2o = h2o.kmeans(data = benign.hex, centers = i)
    print(benign.h2o)
    benign.km = kmeans(benign.data, centers = i)
    checkKMModel(benign.h2o, benign.km)
  }
}

test.KM.prostate <- function() {
  cat("\nImporting prostate.csv data...\n")
  serverH2O = new("H2OClient", ip=myIP, port=myPort)
  prostate.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", "prostate.hex")
  prostate.sum = summary(prostate.hex)
  print(prostate.sum)
  
  prostate.data = read.csv(text = getURL("https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv"), header = TRUE)
  prostate.data = na.omit(prostate.data)
  
  for(i in 5:8) {
    cat("\nH2O K-Means with", i, "clusters:\n")
    prostate.h2o = h2o.kmeans(data = prostate.hex, centers = i, cols = "2")
    print(prostate.h2o)
    prostate.km = kmeans(prostate.data[,3], centers = i)
    checkKMModel(prostate.h2o, prostate.km)
  }
}