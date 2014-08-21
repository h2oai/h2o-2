##
# Test: Saving and Loading GLM Model (HEX-1775)
# Description: Build GLM model, save model in R, copy model and load in R
##

setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
# setwd("/Users/tomk/0xdata/ws/h2o/R/tests/testdir_jira")

source('../findNSourceUtils.R')

test.hex_1775 <- function(conn) {
  temp_dir = tempdir()
  temp_subdir = paste(temp_dir, "tmp", sep = .Platform$file.sep)
  temp_subdir2 = paste(temp_dir, "tmp2", sep = .Platform$file.sep)
  dir.create(temp_subdir)
  
  # Test saving and loading of GLM model
  Log.info("Importing prostate.csv...")
  prostate.hex = h2o.importFile(conn, normalizePath(locate('smalldata/logreg/prostate.csv')))
  
  Log.info("Build GLM model and save to disk")
  prostate.glm = h2o.glm(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, family = "binomial", nfolds = 10, alpha = 0.5)
  prostate.pred = h2o.predict(object = prostate.glm, newdata = prostate.hex)
  prostate.pred.df = as.data.frame(prostate.pred)
  prostate.glm.path = h2o.saveModel(object = prostate.glm, dir = temp_subdir, save_cv  = TRUE, force = TRUE)
  # All keys removed to test that cross validation models are actually being loaded
  h2o.removeAll(object = conn)
 
  # Proving we can move files from one directory to another and not affect the load of the model
  Log.info(paste("Moving GLM model from", temp_subdir, "to", temp_subdir2))
  file.rename(temp_subdir, temp_subdir2)

  moved.path = paste(temp_subdir2, basename(prostate.glm.path), sep="/")
  Log.info(paste("Load GLM model saved at", moved.path))
  prostate.glm2 = h2o.loadModel(conn, moved.path)

  # Check to make sure predictions made on loaded model is the same as prostate.pred
  prostate.hex = h2o.importFile(conn, normalizePath(locate('smalldata/logreg/prostate.csv')))
  prostate.pred2 = h2o.predict(object = prostate.glm2, newdata = prostate.hex)
  prostate.pred2.df = as.data.frame(prostate.pred2)
  expect_equal(nrow(prostate.pred.df), 380)
  expect_equal(prostate.pred.df, prostate.pred2.df)
  
  testEnd()
}

doTest("HEX-1775 Test: Save and Load GLM Model", test.hex_1775)
