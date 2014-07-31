##
# Test: Saving and Loading H2O Models
# Description: Build an H2O model, save it, then load it again in H2O and verify no information was lost
##

setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.rdoc_save_model.golden <- function(conn) {
  temp_dir = tempdir()
  
  # Test saving and loading of GLM model
  Log.info("Importing prostate.csv...")
  prostate.hex = h2o.importFile(conn, path = "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", key = "prostate.hex")
  
  Log.info("Build GLM model and save to disk")
  prostate.glm = h2o.glm(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, family = "binomial", nfolds = 10, alpha = 0.5)
  prostate.glm.path = h2o.saveModel(object = prostate.glm, dir = temp_dir)
  
  Log.info(paste("Load GLM model saved at", prostate.glm.path))
  prostate.glm2 = h2o.loadModel(conn, prostate.glm.path)
  
  expect_equal(class(prostate.glm), class(prostate.glm2))
  expect_equal(prostate.glm@data, prostate.glm2@data)
  expect_equal(prostate.glm@model, prostate.glm2@model)
  expect_equal(prostate.glm@xval, prostate.glm2@xval)
  
  # Test saving and loading of Deep Learning model with validation dataset
  Log.info("Importing prostate_train.csv and prostate_test.csv...")
  prostate.train = h2o.importFile(conn, path = "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate_train.csv", key = "prostate.train")
  prostate.test = h2o.importFile(conn, path = "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate_test.csv", key = "prostate.test")
  
  Log.info("Build Deep Learning model and save to disk")
  prostate.dl = h2o.deeplearning(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.train, validation = prostate.test)
  prostate.dl.path = h2o.saveModel(prostate.dl, dir = temp_dir)
  
  Log.info(paste("Load Deep Learning model saved at", prostate.dl.path))
  prostate.dl2 = h2o.loadModel(conn, prostate.dl.path)
  
  expect_equal(class(prostate.dl), class(prostate.dl2))
  expect_equal(prostate.dl@data, prostate.dl2@data)
  expect_equal(prostate.dl@model, prostate.dl2@model)
  expect_equal(prostate.dl@valid, prostate.dl2@valid)
  
  testEnd()
}

doTest("R Doc Save Model", test.rdoc_save_model.golden)

