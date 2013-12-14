# setwd("/Users/tomk/0xdata/ws/h2o/R/tests/testdir_javapredict")

source('../findNSourceUtils.R')

#----------------------------------------------------------------------
# Print out a message with clear whitespace.
#
# Parameters:  x -- Message to print out.
#              n -- (optional) Step number.
#
# Returns:     none
#----------------------------------------------------------------------
heading <- function(x, n = -1) {
  Log.info("")
  Log.info("")
  if (n < 0) {
    Log.info(sprintf("STEP: %s", x))
  }
  else {
    Log.info(sprintf("STEP %2d: %s", n, x))
  }
  Log.info("")
  Log.info("")
}

#----------------------------------------------------------------------
# "Safe" system.  Error checks process exit status code.  stop() if it failed.
#
# Parameters:  x -- String of command to run (passed to system()).
#
# Returns:     none
#----------------------------------------------------------------------
safeSystem <- function(x) {
  print(sprintf("+ CMD: %s", x))
  res <- system(x)
  print(res)
  if (res != 0) {
    msg <- sprintf("SYSTEM COMMAND FAILED (exit status %d)", res)
    stop(msg)
  }
}

heading("BEGIN TEST")
conn <- new("H2OClient", ip=myIP, port=myPort)
n.trees <- 100 
interaction.depth <- 5
n.minobsinnode <- 10
shrinkage <- 0.1
x = c("sepal_len","sepal_wid","petal_len","petal_wid");
y = "species"

heading("Uploading train data")
iris_train.hex <- h2o.uploadFile(conn, locate("smalldata/iris/iris_train.csv"))

heading("Creating GBM model in H2O")
iris.gbm.h2o <- h2o.gbm(x = x, y = y, data = iris_train.hex, n.trees = n.trees, interaction.depth = interaction.depth, n.minobsinnode = n.minobsinnode, shrinkage = shrinkage)
print(iris.gbm.h2o)

heading("Downloading Java prediction model from H2O")
model_key <- iris.gbm.h2o@key
tmpdir_name <- sprintf("tmp_model_%s", as.character(Sys.getpid()))
cmd <- sprintf("rm -fr %s", tmpdir_name)
safeSystem(cmd)
cmd <- sprintf("mkdir %s", tmpdir_name)
safeSystem(cmd)
cmd <- sprintf("curl -o %s/%s.java http://%s:%d/2/GBMModelView.java?_modelKey=%s", tmpdir_name, model_key, myIP, myPort, model_key)
safeSystem(cmd)

heading("Uploading test data")
iris_test.hex <- h2o.uploadFile(conn, locate("smalldata/iris/iris_test.csv"))

heading("Predicting in H2O")

heading("Predicting in Java")

heading("Comparing predictions between H2O and Java")

heading("Cleaning up tmp files")
cmd <- sprintf("rm -fr %s", tmpdir_name)
safeSystem(cmd)

PASS_BANNER()
