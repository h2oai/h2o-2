##
# 
# Check the types of the columns returned from h2o after
# calling as.data.frame.
#
# Should check both VA and FV...
#
# Check that "num"-type columns (double columns) are "num"
# Check that "int"-type columns are "int"
# Check that factor (or character) type columns are factor/character
#
##

setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

# setupRandomSeed(1994831827)

data.frame.type.test <- function(conn) {
   #Do VA
   iris.VA <- h2o.uploadFile.VA(conn, locate("smalldata/iris/iris2.csv"))
   df.iris.VA <- as.data.frame(iris.VA)

   #Check each column:

   expect_that(typeof(df.iris.VA[,1]), equals("double"))
   expect_that(typeof(df.iris.VA[,2]), equals("double"))
   expect_that(typeof(df.iris.VA[,3]), equals("double"))
   expect_that(typeof(df.iris.VA[,4]), equals("double"))
   expect_that(class(df.iris.VA[,5]), equals("factor"))

   print(typeof(df.iris.VA[,1]))
   print(typeof(df.iris.VA[,2]))
   print(typeof(df.iris.VA[,3]))
   print(typeof(df.iris.VA[,4]))
   print(typeof(df.iris.VA[,5]))

   print(levels(df.iris.VA[,5]))
   print(levels(iris[,5]))
   expect_that(levels(df.iris.VA[,5]), equals(levels(iris[,5])))

   #Do FV
   iris.FV <- h2o.uploadFile.FV(conn, locate("smalldata/iris/iris2.csv"))
   df.iris.FV <- as.data.frame(iris.VA)

   #Check each column:

   expect_that(typeof(df.iris.FV[,1]), equals("double"))
   expect_that(typeof(df.iris.FV[,2]), equals("double"))
   expect_that(typeof(df.iris.FV[,3]), equals("double"))
   expect_that(typeof(df.iris.FV[,4]), equals("double"))
   expect_that(class(df.iris.FV[,5]), equals("factor"))

   #Check levels:
   expect_that(levels(df.iris.FV[,5]), equals(levels(iris[,5])))

   #Check on prostate data now...
   #Do VA
   prostate.VA <- h2o.uploadFile.VA(conn, locate("smalldata/logreg/prostate.csv"))
   df.prostate.VA <- as.data.frame(prostate.VA)

   #Check each column:

   expect_that(typeof(df.prostate.VA[,1]), equals("integer"))
   expect_that(typeof(df.prostate.VA[,2]), equals("integer"))
   expect_that(typeof(df.prostate.VA[,3]), equals("integer"))
   expect_that(typeof(df.prostate.VA[,4]), equals("integer"))
   expect_that(typeof(df.prostate.VA[,5]), equals("integer"))
   expect_that(typeof(df.prostate.VA[,6]), equals("integer"))
   expect_that(typeof(df.prostate.VA[,7]), equals("double"))
   expect_that(typeof(df.prostate.VA[,8]), equals("double"))
   expect_that(typeof(df.prostate.VA[,9]), equals("integer"))

   #Do FV
   prostate.FV <- h2o.uploadFile.FV(conn, locate("smalldata/logreg/prostate.csv"))
   df.prostate.FV <- as.data.frame(prostate.FV)

   #Check each column:
   expect_that(typeof(df.prostate.FV[,1]), equals("integer"))
   expect_that(typeof(df.prostate.FV[,2]), equals("integer"))
   expect_that(typeof(df.prostate.FV[,3]), equals("integer"))
   expect_that(typeof(df.prostate.FV[,4]), equals("integer"))
   expect_that(typeof(df.prostate.FV[,5]), equals("integer"))
   expect_that(typeof(df.prostate.FV[,6]), equals("integer"))
   expect_that(typeof(df.prostate.FV[,7]), equals("double"))
   expect_that(typeof(df.prostate.FV[,8]), equals("double"))
   expect_that(typeof(df.prostate.FV[,9]), equals("integer"))

   testEnd()
}

doTest("Type check data frame", data.frame.type.test)
