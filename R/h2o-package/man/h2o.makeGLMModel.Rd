\name{h2o.makeGLMModel}
\alias{h2o.makeGLMModel}
\title{
Create a GLM Model.
}
\description{
The user can modified the coefficients in an already built GLM Model, 
modified GLM model will have warning attached letting the user know 
that it is a modified and hand made model object that is not built 
using native h2o.glm function.
}
\usage{
h2o.makeGLMModel(model, beta)
}
\arguments{
  \item{model}{An \code{\linkS4class{H2OGLMModel}} object whose coefficients
    will be changed.}
  \item{beta}{A named vector of the coefficients that will replace the coefficients
    named vector in the model.}
}
\value{
  Returns an object of class \code{\linkS4class{H2OGLMModel}} with slots \code{key},
  \code{data}, \code{model}, and \code{xval}.
}
\seealso{
  \code{\link{h2o.glm}}, \code{\linkS4class{H2OGLMModel}}
}
\examples{
# -- CRAN examples begin --
library(h2o)
localH2O = h2o.init()

# Run GLM of CAPSULE ~ AGE + RACE + PSA + DCAPS
prostatePath = system.file("extdata", "prostate.csv", package = "h2o")
prostate.hex = h2o.importFile(localH2O, path = prostatePath, key = "prostate.hex")
prostate.glm = h2o.glm(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, 
        family = "binomial", nfolds = 0, alpha = 0.5, lambda_search = FALSE, 
        use_all_factor_levels = FALSE, variable_importances = FALSE,
        higher_accuracy = FALSE)

# Change coefficient for AGE variable to 0.5
coeff = prostate.glm@model$coefficients
coeff["AGE"] = 0.5
prostate.glm2 = h2o.makeGLMModel(model = prostate.glm, beta = coeff)
# -- CRAN examples end --
}