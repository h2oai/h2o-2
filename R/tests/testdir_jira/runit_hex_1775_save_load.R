##
# Test: Saving and Loading GLM Model (HEX-1775)
# Description: Build GLM model, save model in R, copy model and load in R
##

setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.hex_1775 <- function(conn) {
  temp_dir = tempdir()
  temp_subdir = paste(temp_dir, "tmp_copy", sep = .Platform$file.sep)
  dir.create(temp_subdir)
  
  # Test saving and loading of GLM model
  Log.info("Importing prostate.csv...")
  prostate.hex = h2o.importFile(conn, normalizePath(locate('smalldata/logreg/prostate.csv')))
  
  Log.info("Build GLM model and save to disk")
  prostate.glm = h2o.glm(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, family = "binomial", nfolds = 10, alpha = 0.5)
  prostate.glm.path1 = h2o.saveModel(object = prostate.glm, dir = temp_dir)
  
  Log.info(paste("Copying GLM model from", prostate.glm.path1, "to", temp_subdir))
  prostate.glm.path2 = paste(temp_subdir, basename(prostate.glm.path1), sep = .Platform$file.sep)
  file.copy(from = prostate.glm.path1, to = prostate.glm.path2)
  
  Log.info(paste("Load GLM model saved at", prostate.glm.path2))
  prostate.glm2 = h2o.loadModel(conn, prostate.glm.path2)

  # Check that loaded model is same as one built in R
  expect_equal(class(prostate.glm), class(prostate.glm2))
  expect_equal(prostate.glm@data, prostate.glm2@data)
  expect_equal(prostate.glm@model, prostate.glm2@model)
  expect_equal(prostate.glm@xval, prostate.glm2@xval)
  
  unlink(temp_subdir, recursive = TRUE)
  testEnd()
}

doTest("HEX-1775 Test: Save and Load GLM Model", test.hex_1775)