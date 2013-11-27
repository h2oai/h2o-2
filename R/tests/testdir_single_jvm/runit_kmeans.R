source('./findNSourceUtils.R')

Log.info("======================== Begin Test ===========================\n")

checkKMModel <- function(myKM.h2o, myKM.r) {
  # checkEqualsNumeric(myKM.h2o@model$size, myKM.r$size, tolerance = 1.0)
  # checkEqualsNumeric(myKM.h2o@model$withinss, myKM.r$withinss, tolerance = 1.5)
  # checkEqualsNumeric(myKM.h2o@model$tot.withinss, myKM.r$tot.withinss, tolerance = 1.5)
  # checkEqualsNumeric(myKM.h2o@model$centers, myKM.r$centers, tolerance = 0.5)
}

# Test k-means clustering on benign.csv
test.km.benign <- function(conn) {
  Log.info("Importing benign.csv data...\n")
  # benign.hex = h2o.importURL(conn, "https..//raw.github.com/0xdata/h2o/master/smalldata/logreg/benign.csv")
  # benign.hex = h2o.importFile(conn, normalizePath("../../../smalldata/logreg/benign.csv"))
  benign.hex <- h2o.uploadFile(conn, locate("../../../smalldata/logreg/benign.csv"))
  benign.sum <- summary(benign.hex)
  print(benign.sum)
  
  # benign.data = read.csv(text = getURL("https..//raw.github.com/0xdata/h2o/master/smalldata/logreg/benign.csv"), header = TRUE)
  benign.data <- read.csv(locate("../../../smalldata/logreg/benign.csv"), header = TRUE)
  benign.data <- na.omit(benign.data)
  
  for(i in 2:6) {
    Log.info(paste("H2O K-Means with ", i, " clusters:\n", sep = ""))
    benign.km.h2o <- h2o.kmeans(data = benign.hex, centers = as.numeric(i), cols = colnames(benign.hex))
    print(benign.km.h2o)
    benign.km <- kmeans(benign.data, centers = i)
    checkKMModel(benign.km.h2o, benign.km)
  }
}

# Test k-means clustering on prostate.csv
test.km.prostate <- function(conn) {
  Log.info("Importing prostate.csv data...\n")
  # prostate.hex = h2o.importURL(conn, "https..//raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", "prostate.hex")
  # prostate.hex = h2o.importFile(conn, normalizePath("../../../smalldata/logreg/prostate.csv"))
  prostate.hex = h2o.uploadFile(conn, locate("../../../smalldata/logreg/prostate.csv"))
  prostate.sum = summary(prostate.hex)
  print(prostate.sum)
  
  # prostate.data = read.csv(text = getURL("https..//raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv"), header = TRUE)
  prostate.data = read.csv(locate("../../../smalldata/logreg/prostate.csv"), header = TRUE)
  prostate.data = na.omit(prostate.data)
  
  for(i in 5:8) {
    Log.info(paste("H2O K-Means with ", i, " clusters:\n", sep = ""))
    prostate.km.h2o = h2o.kmeans(data = prostate.hex, centers = as.numeric(i), cols = colnames(prostate.hex)[-1])
    print(prostate.km.h2o)
    prostate.km = kmeans(prostate.data[,3], centers = i)
    checkKMModel(prostate.km.h2o, prostate.km)
  }
}

conn <- new("H2OClient", ip=myIP, port=myPort)
tryCatch("KMeans FVec", test.km.benign(conn), warning = function(w) WARN(w), error = function(e) FAIL(e))
PASS()

tryCatch("KMeans FVec Test 2", test.km.prostate(conn), warning = function(w) WARN(w), error = function(e) FAIL(e))
PASS()
