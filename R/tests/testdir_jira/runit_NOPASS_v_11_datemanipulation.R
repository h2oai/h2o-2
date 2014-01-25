#
# date parsing and field extraction tests
#


setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source('../findNSourceUtils.R')



datetest <- function(conn){

  Log.info('uploading date testing dataset')
  df.h <- h2o.importFile(conn, locate('smalldata/jira/v-11.csv'))
  # df should be 5 columns: ds1:5

  Log.info('data as loaded into h2o:')
  Log.info(head(df.h))

  Log.info('adding date columns')
  df.h$days1 <- as.Date(df.h$ds1, origin='1970-01-01')
  df.h$days2 <- as.Date(df.h$ds2, format='%Y-%m-%d')
  df.h$days3 <- as.Date(df.h$ds3, format='%d-%b-%y')
  df.h$days4 <- as.Date(df.h$ds4, format='%d-%B-%Y')
  df.h$days5 <- as.Date(df.h$ds5, format='%d/%m/%y')

  Log.info('converting dates to posix date objects')
  df.h$p1 <- as.POSIXlt(df.h$days1)
  df.h$p2 <- as.POSIXlt(df.h$days2)
  df.h$p3 <- as.POSIXlt(df.h$days3)
  df.h$p4 <- as.POSIXlt(df.h$days4)
  df.h$p5 <- as.POSIXlt(df.h$days5)

  Log.info('extracting year and month from posix date objects')
  df.h$idx1 <- df.h$p1$year + 12 * df.h$p1$mon
  df.h$idx2 <- df.h$p2$year + 12 * df.h$p1$mon
  df.h$idx3 <- df.h$p3$year + 12 * df.h$p1$mon
  df.h$idx4 <- df.h$p4$year + 12 * df.h$p1$mon
  df.h$idx5 <- df.h$p5$year + 12 * df.h$p1$mon

  Log.info('pulling year/month indices local')
  df <- as.data.frame( df[, 16:20] )

  #stringified <- c('2006-01-03', '2009-07-15', '2009-09-30')
  idx1 <- c(840,889,1332)
  idx2 <- c(1272,1314,1316)

  Log.info('testing correctness')
  expect_that( all( idx1 == df[,1] ) )
  expect_that( all( idx1 == df[,2] ) )
  expect_that( all( idx2 == df[,3] ) )
  expect_that( all( idx2 == df[,4] ) )
  expect_that( all( idx2 == df[,5] ) )
  testEnd()
}



doTest('date testing', datetest)
