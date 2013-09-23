# Test gradient boosting machines in H2O
grabRemote <- function(myURL, myFile) {
  temp <- tempfile()
  download.file(myURL, temp, method = "curl")
  aap.file <- read.csv(file = unz(description = temp, filename = myFile), as.is = TRUE)
  unlink(temp)
  return(aap.file)
}

checkGBMModel <- function(myGBM.h2o, myGBM.r) {
}

test.GBM.ecology <- function() {
  cat("\nImporting ecology_model.csv data...\n")
  serverH2O = new("H2OClient", ip=myIP, port=myPort)
  ecology.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/gbm_test/ecology_model.csv")
  ecology.sum = summary(ecology.hex)
  print(ecology.sum)
  
  ecology.data = read.csv(text = getURL("https://raw.github.com/0xdata/h2o/master/smalldata/gbm_test/ecology_model.csv"), header = TRUE)
  ecology.data = na.omit(ecology.data)
  
  cat("\nH2O GBM with parameters:\nntrees = 100, max_depth = 5, min_rows = 10, learn_rate = 0.1\n")
  ecology.h2o = h2o.gbm(ecology.hex, destination = "ecology.gbm", y = "Site", ntrees = 100, max_depth = 5, min_rows = 10, learn_rate = 0.1)
  print(ecology.h2o)
  
  allyears.gbm = gbm(Site ~ ., data = ecology.data, distribution = "gaussian", n.trees = 100, interaction.depth = 5, n.minobsinnode = 10, shrinkage = 0.1)
  checkGBMModel(allyears.h2o, allyears.gbm)
}

test.GBM.airlines <- function() {
  # allyears.data = grabRemote("https://raw.github.com/0xdata/h2o/master/smalldata/gbm_test/ecology_model.csv", "ecology.csv")
  # allyears.data = na.omit(allyears.data)
  # allyears.data = data.frame(rapply(allyears.data, as.factor, classes = "character", how = "replace"))
  
  # allCol = colnames(allyears.data)
  # allXCol = allCol[-which(allCol == "IsArrDelayed")]
  # ignoreFeat = c("CRSDepTime", "CRSArrTime", "ActualElapsedTime", "CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "TaxiIn", "TaxiOut", "Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")
  # ignoreNum = sapply(ignoreFeat, function(x) { which(allXCol == x) })
  
  # allyears.h2o = h2o.gbm(allyears.hex, destination = "allyears.gbm", y = "IsArrDelayed", x_ignore = ignoreNum, ntrees = 100, max_depth = 5, min_rows = 10, learn_rate = 0.1)
  
  # allyears.x = allyears.data[,-which(allCol == "IsArrDelayed")]
  # allyears.x = subset(allyears.x, select = -ignoreNum)
  # allyears.gbm = gbm.fit(y = allyears.data$IsArrDelayed, x = allyears.x, distribution = "bernoulli", n.trees = 100, interaction.depth = 5, n.minobsinnode = 10, shrinkage = 0.1)
}