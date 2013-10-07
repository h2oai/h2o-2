source('./Utils/h2oR.R')

logging("\n======================== Begin Test ===========================\n")
view_max <- 10000 #maximum returned by Inspect.java

H2Ocon <- new("H2OClient", ip=myIP, port=myPort)

test.tail.numeric <- function(con) {
  cat("\nImporting USArrests.csv data...\n")
  arrests.hex = h2o.uploadFile(H2Ocon, "../../smalldata/pca_test/USArrests.csv", "arrests.hex")
  cat("\nCheck that tail works...")
  tail(arrests.hex)
  tail_ <- tail(arrests.hex)
  cat("\nCheck that we get a data frame back from the tail(hex)")
  expect_that(tail_, is_a("data.frame"))
  tail_2 <- tail(USArrests)
  rownames(tail_2) <- 1:nrow(tail_2) #remove state names from USArrests
  cat("\nCheck that the tail of the dataset is the same as what R produces: ")
  cat("\ntail(USArrests)\n")
  print(tail_2)
  cat("\ntail(arrests.hex)\n")
  print(tail_)
  expect_that(tail_, equals(tail_2))
  if( nrow(arrests.hex) <= view_max) {
    cat("\nTry doing tail w/ n > nrows(data). Should do same thing as R (returns all rows)")
    cat(paste("\n Data has ", paste(nrow(arrests.hex), " rows",sep=""),sep=""))
    cat("\n")
    tail_max <- tail(arrests.hex,nrow(arrests.hex) + 1)
  }
  print("End of test.")
}

test_that("tailTests",test.tail.numeric(H2Ocon))

