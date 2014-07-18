#
# date parsing and field extraction tests
#


setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')



datetest <- function(conn){

  Log.info('uploading date testing dataset')
  hdf <- h2o.importFile(conn, normalizePath(locate('smalldata/jira/v-11.csv')))
  # df should be 5 columns: ds1:5

  Log.info('data as loaded into h2o:')
  Log.info(head(hdf))

  # NB: columns 1,5 are currently unsupported as date types
  # that is, h2o cannot understand:
  # 1 integer days since epoch (or since any other date);
  # 2 dates formatted as %d/%m/%y (in strptime format strings)

  Log.info('adding date columns')
  # NB: h2o automagically recognizes and if it doesn't recognize, you're out of luck

  Log.info('extracting year and month from posix date objects')
  hdf$year2 <- year(hdf$ds2)
  hdf$year3 <- year(hdf$ds3)
  hdf$year4 <- year(hdf$ds4)
  hdf$mon2 <- month(hdf$ds2)
  hdf$mon3 <- month(hdf$ds3)
  hdf$mon4 <- month(hdf$ds4)
  hdf$idx2 <- year(hdf$ds2) * 12 + month(hdf$ds2)
  hdf$idx3 <- year(hdf$ds3) * 12 + month(hdf$ds3)
  hdf$idx4 <- year(hdf$ds4) * 12 + month(hdf$ds4)

  cc <- colnames(hdf)
  nn <- c( paste('year', 2:4, sep=''), paste('month', 2:4, sep=''), paste('idx', 2:4, sep='') )
  cc[ (length(cc) - length(nn) + 1):length(cc) ] <- nn
  colnames(hdf) <- cc

  Log.info('pulling year/month indices local')
  ldf <- as.data.frame( hdf )

  # build the truth using R internal date fns
  rdf <- read.csv(locate('smalldata/jira/v-11.csv'))
  rdf$days1 <- as.Date(rdf$ds1, origin='1970-01-01')
  rdf$days2 <- as.Date(rdf$ds2, format='%Y-%m-%d')
  rdf$days3 <- as.Date(rdf$ds3, format='%d-%b-%y')
  rdf$days4 <- as.Date(rdf$ds4, format='%d-%B-%Y')
  rdf$days5 <- as.Date(rdf$ds5, format='%d/%m/%y')

  months <- data.frame(lapply(rdf[,6:10], function(x) as.POSIXlt(x)$mon))
  years <- data.frame(lapply(rdf[,6:10], function(x) as.POSIXlt(x)$year))
  idx <- 12*years + months

  Log.info('testing correctness')
  expect_that( ldf$year2, equals(years[,2]) )
  expect_that( ldf$year3, equals(years[,3]) )
  expect_that( ldf$year4, equals(years[,4]) )

  expect_that( ldf$month2, equals(months[,2]) )
  expect_that( ldf$month3, equals(months[,3]) )
  expect_that( ldf$month4, equals(months[,4]) )

  expect_that( ldf$idx2, equals(idx[,2]) )
  expect_that( ldf$idx3, equals(idx[,3]) )
  expect_that( ldf$idx4, equals(idx[,4]) )

  testEnd()
}


doTest('date testing', datetest)
