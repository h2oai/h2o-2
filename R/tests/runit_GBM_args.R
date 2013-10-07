source('./Utils/h2oR.R')

#------------------------------ Begin Tests ------------------------------#
serverH2O = new("H2OClient", ip=myIP, port=myPort)
grabRemote <- function(myURL, myFile) {
  temp <- tempfile()
  download.file(myURL, temp, method = "curl")
  aap.file <- read.csv(file = unz(description = temp, filename = myFile), as.is = TRUE)
  unlink(temp)
  return(aap.file)
}


check.params <- function() {
  #current allowed params as of 10/04/2013 5:13PM PST: 
  cat("\nParameters of H2O GBM as of 10/04/2013\n")
  x <- c("x", "y", "data", "n.trees", "interaction.depth", "n.minobsinnode", "shrinkage")
  print(x)
  #grab params from newest build:
  cat("\nParameters of current build: \n")
  y <- h2o.gbm@signature
  print(y)
  expect_that(length(x), equals(length(y)))
  cat("\nExpect that all of the parameters in the old set are in the new set and vice-versa: ")
  print(x)
  print(y)
  print(x %in% y)
  print(y %in% x)
  expect_that(length(x), equals(sum(x %in% y)))
  expect_that(length(y), equals(sum(y %in% x)))
  cat("End of test.")
}

test_that("gbm params test", check.params)

