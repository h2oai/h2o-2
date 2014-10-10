\name{h2o.coxph}
\alias{h2o.coxph}
\alias{h2o.coxph.control}
\alias{H2OCoxPHModel-class}
\alias{H2OCoxPHModelSummary-class}
\alias{coef.H2OCoxPHModel}
\alias{coef.H2OCoxPHModelSummary}
\alias{extractAIC.H2OCoxPHModel}
\alias{logLik.H2OCoxPHModel}
\alias{print.survfit.H2OCoxPHModel}
\alias{show,H2OCoxPHModel-method}
\alias{show,H2OCoxPHModelSummary-method}
\alias{summary,H2OCoxPHModel-method}
\alias{survfit.H2OCoxPHModel}
\alias{vcov.H2OCoxPHModel}
\title{
H2O: Cox Proportional Hazards Models
}
\description{
Fit a Cox Proportional Hazards Model.
}
\usage{
h2o.coxph(x, y, data, key = "", ties = c("efron", "breslow"),
          init = 0, control = h2o.coxph.control(...), ...)

h2o.coxph.control(lre = 9, iter.max = 20, ...)

# H2OCoxPHModel summary functions
\S4method{summary}{H2OCoxPHModel}(object, conf.int = 0.95, scale = 1, ...)
\S3method{survfit}{H2OCoxPHModel}(formula, newdata, conf.int = 0.95,
        conf.type = c("log", "log-log", "plain", "none"), ...)

# H2OCoxPHModel extractor functions
\S3method{extractAIC}{H2OCoxPHModel}(fit, scale, k = 2, ...)
\S3method{logLik}{H2OCoxPHModel}(object, ...)
\S3method{vcov}{H2OCoxPHModel}(object, ...)
}
\arguments{
  \item{x}{A character string containing the name of the numeric predictor in
    the model.}
  \item{y}{The character vector comprise of two or three elements representing
    \code{"(stop, event)"} or \code{"(stop, event)"} respectively.}
  \item{data}{An \code{\linkS4class{H2OParsedData}} object containing the
    variables in the model.}
  \item{key}{An optional unique hex key assigned to the resulting model.
    If none is given, a key will automatically be generated.}
  \item{ties}{A character string denoting which approximation method for
    handling ties should be used in the partial likelihood;
    one of either \code{"efron"} or \code{"breslow"}.}
  \item{init}{A numeric vector containing the finite starting values for the
    model coefficients.}
  \item{control}{The model fitting control arguments specified by
    \code{h2o.coxph.control}}.
  \item{lre}{A positive number for the log-relative error (LRE) of subsequent
    log partial likelihood calculations to determine convergence in
    \code{h2o.coxph}.}
  \item{iter.max}{A positive integer denoting the maximum number of iterations
    to allow for convergence in \code{h2o.coxph}.}
  \item{object, formula, fit}{An object of class \code{H2OCoxPHModel}.}
  \item{newdata}{An optional \code{\linkS4class{H2OParsedData}} object
    containing a new data set.}
  \item{conf.int}{An optional number that specifies the confidence interval
    level.}
  \item{conf.type}{An optional string that specifies the confidence interval
    type.}
  \item{scale}{An optional number that specifies the scale parameter of the
    model.}
  \item{k}{An optional number specifying the weight for the equivalent
    degrees of freedoms in the AIC calculation.}
  \item{\dots}{Additional arguments.}
}
\value{
An object of class \code{\linkS4class{H2OCoxPHModel}}.
}
\seealso{
\code{\link[survival]{coxph}},
\code{\link[survival]{summary.coxph}},
\code{\link[survival]{survfit.coxph}},
\code{\link[stats]{extractAIC}},
\code{\link[stats]{logLik}},
\code{\link[stats]{vcov}}
}
\examples{
library(h2o)
localH2O = h2o.init()

library(survival)
bladder.hex <- as.h2o(localH2O, bladder, key = "bladder.hex")
h2o.coxph(x = "number", y = c("stop", "event"), data = bladder.hex)
}
