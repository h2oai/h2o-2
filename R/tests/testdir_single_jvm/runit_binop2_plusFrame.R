##
# Test: binop2 + opeartor
# Description: Check the '+' binop2 operator
# Variations: e1 + e2
#    e1 & e2 H2OParsedData
#    e1 Numeric & e2 H2OParsedData
#    e1 H2OParsedData & e2 Numeric
##

source('./findNSourceUtils.R')

doSelect<-
function() {
    d <- select()
    dd <- d[[1]]$ATTRS
    if(any(dd$TYPES != "enum")) return(d)
    Log.info("No numeric columns found in data, trying a different selection")
    doSelect()
}

test.tail.numeric <- function(conn) {
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

  Log.info("Try adding scalar to frame: 5 + hex")
  if(anyEnum) expect_warning(fivePlusHex <- 5 + hex)
  else fivePlusHex <- 5 + hex

  Log.info("Original frame: ")
  print(head(as.data.frame(hex)))

  Log.info("5+hex:")
  print(head(as.data.frame(fivePlusHex)))

  Log.info("fivePlusHex - 5: ")
  print(head(as.data.frame(fivePlusHex - 5)))
  expect_that(dim(fivePlusHex), equals(dim(hex)))
  expect_that(head(fivePlusHex), equals(5 + head(hex)))

  Log.info("Checking left and right: ")
  hexPlusFive <- hex + 5
  fivePlusHex <- 5 + hex

  Log.info("hex + 5: ")
  print(head(hexPlusFive))
  
  Log.info("5 + hex: ")
  print(head(fivePlusHex))
  
  expect_that(hexPlusFive, equals(fivePlusHex))

  testEnd()
}

doTest("Tail Tests", test.tail.numeric)

