source('./Utils/h2oR.R')

logging("\n======================== Begin Test ===========================\n")
view_max <- 10000 #maximum returned by Inspect.java

H2Ocon <- new("H2OClient", ip=myIP, port=myPort)

test.histogram <- function (con, path, key) {
  h2o.uploadFile(H2Ocon, path, key)
  h2o.data <- new('H2OParsedData2')
  h2o@h2o <- con
  h2o@key <- key
  h2o.hists <- histograms(h2o.data)
  r.data <- read.csv(path)
  for (i in 1:length(h2o.hists)) {
    if (is.numeric(r.data[,i])) {
      r.hist <- hist(r.data[,i], breaks='scott', right=FALSE)
      h2o.counts <- h2o.hists[[i]]$counts
      h2o.breaks <- h2o.hists[[i]]$breaks
      expect_that(h2o.counts, equals(r.hist$counts))
      expect_that(h2o.breaks, equals(r.hist$breaks))
    }
  }
}

test.histogram(H2Ocon, '../../smalldata/cars.csv', 'cars.hex')
test.histogram(H2Ocon, '../../smalldata/pca_test/USArrests.csv', 'usarrests.hex')
test.histogram(H2Ocon, '../../smalldata/iris/iris22.csv', 'iris.hex')

