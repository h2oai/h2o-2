setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source('../findNSourceUtils.R')

test.rdocasdataframe.golden <- function(H2Oserver) {
	
#Example from as.data.frame R doc

prosPath = system.file("extdata", "prostate.csv", package="h2oRClient")
prostate.hex = h2o.importFile(H2Oserver, path = prosPath)
prostate.data.frame<- as.data.frame(prostate.hex)
sum<- summary(prostate.data.frame)
head<- head(prostate.data.frame)


Log.info("Print output from as.data.frame call")
Log.info(paste("H2O Summary  :" ,sum))
Log.info(paste("H2O Head  : " , head))

testEnd()
}

doTest("R Doc as.data.frame", test.rdocasdataframe.golden)

