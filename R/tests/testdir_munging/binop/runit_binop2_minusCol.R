##
# Test: binop2 - opeartor
# Description: Check the '-' binop2 operator
# Variations: e1 - e2
#    e1 & e2 H2OParsedData
#    e1 Numeric & e2 H2OParsedData
#    e1 H2OParsedData & e2 Numeric
##

setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

#setupRandomSeed(42)

doSelect<-
function() {
    d <- select()
    dd <- d[[1]]$ATTRS
    if(any(dd$TYPES != "enum")) return(d)
    Log.info("No numeric columns found in data, trying a different selection")
    doSelect()
}

test.minus <- function(conn) {
  dataSet <- doSelect()
  dataName <- names(dataSet)
  dd <- dataSet[[1]]$ATTRS
  colnames <- dd$NAMES
  numCols  <- as.numeric(dd$NUMCOLS)
  numRows  <- as.numeric(dd$NUMROWS)
  colTypes <- dd$TYPES
  colRange <- dd$RANGE
  Log.info(paste("Importing ", dataName, " data..."))
  hex <- h2o.uploadFile(conn, locate(dataSet[[1]]$PATHS[1]), paste("r", gsub('-','_',dataName),".hex", sep = ""))
  anyEnum <- FALSE
  if(any(dd$TYPES == "enum")) anyEnum <- TRUE

  Log.info("Try adding scalar to a numeric column: 5 - hex[,col]")
  #col <- sample(colnames[colTypes != "enum"], 1)
  #col <- ifelse(is.na(suppressWarnings(as.numeric(col))), col, as.numeric(col) + 1)
  #col <- ifelse(is.na(suppressWarnings(as.numeric(col))), col, paste("C", col, sep = "", collapse = ""))
  df <- head(hex)
  col <- sample(colnames(df[!sapply(df, is.factor)]), 1)
  Log.info(paste("Using column: ", col))
 
  sliced <- hex[,col]
  Log.info("Placing key \"sliced.hex\" into User Store")
  sliced <- h2o.assign(sliced, "sliced.hex")
  print(h2o.ls(conn))

  Log.info("Minisuing 5 from sliced.hex")
  slicedMinusFive <- sliced - 5
  slicedMinusFive <- h2o.assign(slicedMinusFive, "slicedMinusFive.hex")

  Log.info("Original sliced: ")
  print(head(as.data.frame(sliced)))

  Log.info("Sliced - 5: ")
  print(head(as.data.frame(slicedMinusFive)))
  expect_that(as.data.frame(slicedMinusFive), equals(as.data.frame(sliced) - 5))

  Log.info("Checking left and right: ")
  slicedMinusFive <- sliced - 5
  fiveMinusSliced <- 5 - sliced

  Log.info("sliced - 5: ")
  print(head(slicedMinusFive))

  Log.info("5 - sliced: ")
  print(head(fiveMinusSliced))
  expect_that(abs(as.data.frame(slicedMinusFive)), equals(abs(as.data.frame(fiveMinusSliced))))

  Log.info("Checking the variation of H2OParsedData - H2OParsedData")

  hexMinusHex <- fiveMinusSliced - slicedMinusFive

  Log.info("fiveMinusSliced - slicedMinusFive: ")
  print(head(hexMinusHex))
  expect_that(as.data.frame(hexMinusHex), equals(as.data.frame(fiveMinusSliced) - as.data.frame(slicedMinusFive)))

  testEnd()
}

doTest("BINOP2 EXEC2 TEST: '-'", test.minus)

