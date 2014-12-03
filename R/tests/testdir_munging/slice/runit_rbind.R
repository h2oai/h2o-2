##
##

setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

test.rbind <- function(conn) {
  Log.info('test rbind')

  hdf <- h2o.uploadFile(conn, locate('../../../smalldata/jira/pub-180.csv'))
  otherhdf <- h2o.uploadFile(conn, locate('../../../smalldata/jira/v-11.csv'))

  Log.info("rbind self")
  print(rbind(hdf, hdf))

  Log.info("rbind otherself")
  print(rbind(otherhdf, otherhdf))


 
  Log.info("cbind then rbind")
  a <- cbind(hdf[,1], hdf[,2], hdf[,3], hdf[,4])
  
  z <- rbind(a, hdf, a, hdf, a, hdf)

  print(dim(z))
  print(z)

  Log.info("cbind then rbind other")
  b <- h2o.uploadFile(conn, locate('../../../smalldata/jira/v-11.csv'))
  
  z <- rbind(b, otherhdf, b, otherhdf, b, otherhdf, b, otherhdf)
  print(dim(z))
  print(z)

  testEnd()
}

doTest("test rbind", test.rbind)

