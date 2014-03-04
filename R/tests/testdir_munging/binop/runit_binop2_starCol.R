##
# Test: binop2 * opeartor
# Description: Check the '*' binop2 operator
# Variations: e1 * e2
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

toDouble <- function(r) ifelse(is.integer(r), as.numeric(r), r)

test.slice.star <- function(conn) {
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

  Log.info("Try adding scalar to a numeric column: 5 * hex[,col]")
  col <- sample(colnames[colTypes != "enum"], 1)
  col <- ifelse(is.na(suppressWarnings(as.numeric(col))), col, as.numeric(col) + 1)
  col <- ifelse(is.na(suppressWarnings(as.numeric(col))), col, paste("C", col, sep = "", collapse = ""))
  Log.info(paste("Using column: ", col))
 
  sliced <- hex[,col]
  Log.info("Placing key \"sliced.hex\" into User Store")
  sliced <- h2o.assign(sliced, "sliced.hex")
  print(h2o.ls(conn))

  Log.info("*ing 5 to sliced.hex")
  slicedStarFive <- sliced * 5
  slicedStarFive <- h2o.assign(slicedStarFive, "slicedStarFive.hex")

  Log.info("Orignal sliced: ")
  df_head <- as.data.frame(sliced)
  df_head <- data.frame(apply(df_head, 1:2, toDouble))
  print(head(df_head))

  Log.info("Sliced * 5: ")
  df_slicedStarFive <- as.data.frame(slicedStarFive)
  df_slicedStarFive <- data.frame(apply(df_slicedStarFive, 1:2, toDouble))
  df_sliced <- as.data.frame(sliced)
  df_sliced <- data.frame(apply(df_sliced, 1:2, toDouble))
  print(head(df_slicedStarFive))

  expect_that(df_slicedStarFive, equals(5 * df_sliced  ))

  Log.info("Checking left and right: ")
  slicedStarFive <- sliced * 5
  fiveStarSliced <- 5 * sliced

  Log.info("sliced * 5: ")
  print(head(slicedStarFive))

  Log.info("5 * sliced: ")
  print(head(fiveStarSliced))

  df_slicedStarFive <- as.data.frame(slicedStarFive)
  df_slicedStarFive <- data.frame(apply(df_slicedStarFive, 1:2, toDouble))
  df_sliced <- as.data.frame(fiveStarSliced)
  df_fiveStarSliced <- data.frame(apply(df_sliced, 1:2, toDouble))

  expect_that(df_slicedStarFive, equals(df_fiveStarSliced))


  Log.info("Checking the variation of H2OParsedData * H2OParsedData")

  hexStarHex <- fiveStarSliced * slicedStarFive

  Log.info("FiveStarSliced * slicedStarFive: ")
  print(head(hexStarHex))
 
  Log.info("as.data.frame(fiveStarSliced) * as.data.frame(fiveStarSliced)")
  

  print(head(df_fiveStarSliced*df_fiveStarSliced))
  expect_that(as.data.frame(hexStarHex), equals(df_fiveStarSliced*df_fiveStarSliced))

  testEnd()
}

doTest("BINOP2 EXEC2 TEST: *", test.slice.star)

