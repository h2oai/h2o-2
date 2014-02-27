##
# Test: binop2 + opeartor
# Description: Check the '+' binop2 operator
# Variations: e1 + e2
#    e1 & e2 H2OParsedData
#    e1 Numeric & e2 H2OParsedData
#    e1 H2OParsedData & e2 Numeric
##


setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

#setupRandomSeed(2078846715)

doSelect<-
function() {
    d <- select()
    dd <- d[[1]]$ATTRS
    if(any(dd$TYPES != "enum")) return(d)
    Log.info("No numeric columns found in data, trying a different selection")
    doSelect()
}

test.plus.onFrame <- function(conn) {
  dataSet <- doSelect()
  dataName <- names(dataSet)
  dd <- dataSet[[1]]$ATTRS
  colnames <- dd$NAMES
  numCols  <- as.numeric(dd$NUMCOLS)
  numRows  <- as.numeric(dd$NUMROWS)
  colTypes <- dd$TYPES
  colRange <- dd$RANGE

  Log.info(paste("Importing ", dataName, " data..."))
  hex <- h2o.uploadFile.FV(conn, locate(dataSet[[1]]$PATHS[1]), paste("r", gsub('-','_',dataName),".hex", sep = ""))
  originalDataType <- class(hex)
  hexOrig <- hex
  anyEnum <- FALSE
  if(any(dd$TYPES == "enum")) anyEnum <- TRUE

  Log.info("Try adding scalar to frame: 5 + hex")
  # if(anyEnum) expect_warning(fivePlusHex <- 5 + hex)
  # else fivePlusHex <- 5 + hex
  fivePlusHex <- 5 + hex  

  Log.info("Original frame: ")
  print(head(as.data.frame(hex)))

  Log.info("5+hex:")
  print(head(as.data.frame(fivePlusHex)))
  cat("\ndim(as.data.frame(fivePlusHex)) : ")
  cat(dim(as.data.frame(fivePlusHex)), "\n")
  cat("\ndim(fivePlusHex) : ")
  cat(dim(fivePlusHex), "\n\n")

  expect_that(dim(as.data.frame(fivePlusHex)), equals(dim(fivePlusHex)))

  Log.info("fivePlusHex - 5: ")
  # if(anyEnum) expect_warning(fivePlusHexMinusFive <- fivePlusHex -5)
  # else fivePlusHexMinusFive <- fivePlusHex - 5
  fivePlusHexMinusFive <- fivePlusHex - 5

  print(head(as.data.frame(fivePlusHexMinusFive)))

  expect_that(dim(fivePlusHex), equals(dim(hex)))
  Log.info("Expect that this frame: ")
  print(fivePlusHexHead <- fivePlusHex[1:6,])
  Log.info("Equals this one: ")
  fivePlusAsData <- 5 + as.data.frame(hex[1:6,])
  print(fivePlusAsData)
  #expect_that(fivePlusHexHead, equals(fivePlusAsData))
  Log.info("Are NAs being treated properly???")
  fPHH <- as.data.frame(fivePlusHexHead)
  print(fPHH)
  expect_that(fivePlusAsData, equals(fPHH))


  Log.info("Checking left and right: ")
  # if(anyEnum) expect_warning(hexPlusFive <- hex + 5)
  # else hexPlusFive <- hex + 5
  hexPlusFive <- hex + 5
  # if(anyEnum) expect_warning(fivePlusHex <- 5 + hex)
  # else fivePlusHex <- 5 + hex
  fivePlusHex <- 5 + hex

  Log.info("hex + 5: ")
  print(head(hexPlusFive))
  
  Log.info("5 + hex: ")
  print(head(fivePlusHex))
  
  expect_that(as.data.frame(hexPlusFive), equals(as.data.frame(fivePlusHex)))

  Log.info("Try to add two frames: hex + hex")
  hd <- as.data.frame(hex)
  hexPlusHex <- hex + hex
  print(head(hexPlusHex))
  hdPlushd <- hd + hd
  print(head(hdPlushd))
  expect_that(hd + hd, equals(as.data.frame(hexPlusHex)))

  testEnd()
}

doTest("BINOP2 EXEC2 TEST: '+' with Frames", test.plus.onFrame)

