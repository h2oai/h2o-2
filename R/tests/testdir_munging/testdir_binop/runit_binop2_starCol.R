##
# Test: binop2 * opeartor
# Description: Check the '*' binop2 operator
# Variations: e1 * e2
#    e1 & e2 H2OParsedData
#    e1 Numeric & e2 H2OParsedData
#    e1 H2OParsedData & e2 Numeric
##

source('./findNSourceUtils.R')

#setupRandomSeed(42)

doSelect<-
function() {
    d <- select()
    dd <- d[[1]]$ATTRS
    if(any(dd$TYPES != "enum")) return(d)
    Log.info("No numeric columns found in data, trying a different selection")
    doSelect()
}

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
  col <- ifelse(is.na(suppressWarnings(as.numeric(col))), col, paste("C", col, sep = "", collapse = ""))
  Log.info(paste("Using column: ", col))
 
  sliced <- hex[,col]
  Log.info("Placing key \"sliced.hex\" into User Store")
  sliced <- h2o.assign(sliced, "sliced.hex")
  print(h2o.ls(conn))

  Log.info("*ing 5 to sliced.hex")
  if(anyEnum) expect_warning(slicedStarFive <- sliced * 5)
  else slicedStarFive <- sliced * 5
  slicedStarFive <- h2o.assign(slicedStarFive, "slicedStarFive.hex")

  Log.info("Orignal sliced: ")
  print(head(as.data.frame(sliced)))

  Log.info("Sliced * 5: ")
  print(head(as.data.frame(slicedStarFive)))
  expect_that(as.data.frame(slicedStarFive), equals(5 *  as.data.frame(sliced)))

  Log.info("Checking left and right: ")
  if(anyEnum) expect_warning(slicedStarFive <- sliced * 5)
  else slicedStarFive <- sliced * 5
  if(anyEnum) expect_warning(fiveStarSliced <- 5 * sliced)
  else fiveStarSliced <- 5 * sliced

  Log.info("sliced * 5: ")
  print(head(slicedStarFive))

  Log.info("5 * sliced: ")
  print(head(fiveStarSliced))
  expect_that(as.data.frame(slicedStarFive), equals(as.data.frame(fiveStarSliced)))


  Log.info("Checking the variation of H2OParsedData * H2OParsedData")

  if(anyEnum) expect_warning(hexStarHex <- fiveStarSliced * slicedStarFive)
  else hexStarHex <- fiveStarSliced * slicedStarFive

  Log.info("FiveStarSliced * slicedStarFive: ")
  print(head(hexStarHex))
 
  Log.info("as.data.frame(fiveStarSliced) * as.data.frame(fiveStarSliced)")
  print(head(as.data.frame(fiveStarSliced)*as.data.frame(fiveStarSliced)))
  expect_that(as.data.frame(hexStarHex), equals(as.data.frame(fiveStarSliced)*as.data.frame(fiveStarSliced)))

  testEnd()
}

doTest("BINOP2 EXEC2 TEST: *", test.slice.star)

