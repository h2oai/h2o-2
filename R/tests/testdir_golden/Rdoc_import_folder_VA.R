setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source('../findNSourceUtils.R')

test.import.folder.VA <- function(H2Oserver) {
	

myPath = paste(path.package("h2oRClient"), "extdata", sep="/")
all_files.hex = h2o.importFolder.VA(H2Oserver, path = myPath)
for(i in 1:length(all_files.hex))
  print(summary(all_files.hex[[i]]))

testEnd()
}

doTest("R Doc import folder VA", test.import.folder.VA)

