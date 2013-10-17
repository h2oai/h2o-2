source('./Utils/h2oR.R')

logging("\n======================== Begin Test ===========================\n")
view_max <- 10000 #maximum returned by Inspect.java

H2Ocon <- new("H2OClient", ip=myIP, port=myPort)

test.histogram <- function (con, path, key) {
  h2o.uploadFile(H2Ocon, path, key)
  h2o.data <- new('H2OParsedData2')
  h2o.data@h2o <- con
  h2o.data@key <- key
  h2o.hists <- histograms(h2o.data)
  r.data <- read.csv(path)
  for (i in 1:length(h2o.hists)) {
    if (is.numeric(r.data[,i])) {
      h2o.counts <- h2o.hists[[i]]$counts
      h2o.breaks <- h2o.hists[[i]]$breaks
      
      r.hist <- hist(r.data[,i], breaks=h2o.breaks, right=FALSE)
      # regularize
#      A <- h2o.counts / sum(h2o.counts)
#      B <- r.hist$counts / sum(r.hist$counts)
#      Binv <- 1 / B
#      Binv[is.nan(Binv)] <- 0
#      chisq <- (A - B) %*% ((A - B) * B)
#      logging(cat("chisq = ", chisq, "\n"))
#      expect_true(chisq < 0.0001)
      expect_true(max(h2o.counts - r.hist$counts) <= 1)
      logging(cat("Column ", i, " in ", path," successful.\n"))
    } else {
      logging(cat("Column ", i, " in ", path," skipped.\n"))
    }
  }
}

test.histogram(H2Ocon, '../../smalldata/cars.csv', 'cars.hex')
test.histogram(H2Ocon, '../../smalldata/pca_test/USArrests.csv', 'usarrests.hex')
test.histogram(H2Ocon, '../../smalldata/iris/iris22.csv', 'iris.hex')
test.histogram(H2Ocon, '../../smalldata/test/arit.csv', 'arit.hex')
