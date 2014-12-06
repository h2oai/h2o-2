setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

test.GLM.prostate <- function(conn) {
  Log.info("Importing prostate.csv data...\n")
  prostate.hex <- h2o.importFile(conn, "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", "prostate.hex")
  beta_constraints <- h2o.importFile(conn, "https://raw.github.com/0xdata/h2o/master/smalldata/beta_constraints.csv")
  glm <- h2o.glm(x = 3:9, y = 2, data = prostate.hex, family = "binomial", beta_constraints = beta_constraints)
  testEnd()
}

doTest("GLM Test: Prostate", test.GLM.prostate)

