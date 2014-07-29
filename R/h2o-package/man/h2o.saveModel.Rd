\name{h2o.saveModel}
\alias{h2o.saveModel}
\title{Save a H2OModel object to disk.}
\description{Save a H2OModel object to a disk and can be loaded back into H2O using h2o.loadModel.}
\usage{h2o.saveModel(object, dir, name, force=FALSE)}

\arguments{
  \item{object}{ An \code{\linkS4class{H2OModel}} object.}
  \item{dir}{ Directory the model file will be written to.}
  \item{name}{ Name of the file being saved.}
  \item{force}{ (Optional) If \code{force = TRUE} any existing file will be overwritten. Otherwise if the file already exists the operation will fail.}
 }
 
\value{
Returns path of model object saved.
}

\examples{
\dontrun{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE)
prostate.hex = h2o.importURL(localH2O, path = paste("https://raw.github.com", 
  "0xdata/h2o/master/smalldata/logreg/prostate.csv", sep = "/"), key = "prostate.hex")
prostate.glm = h2o.glm(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), 
  data = prostate.hex, family = "binomial", nfolds = 10, alpha = 0.5)
h2o.saveModel(object = prostate.glm, dir = "/Users/UserName/Desktop")
}
}
