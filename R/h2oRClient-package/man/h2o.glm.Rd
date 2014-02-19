\name{h2o.glm}
\alias{h2o.glm}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
H2O: Generalized Linear Models
}
\description{
Fit a generalized linear model, specified by a response variable, a set of predictors, and a description of the error distribution.
}
\usage{
h2o.glm(x, y, data, family, nfolds = 10, alpha = 0.5, lambda = 1e-05, epsilon = 1e-4, standardize = TRUE, tweedie.p = ifelse(family == 'tweedie', 1.5, as.numeric(NA)), thresholds, version = 1)
}
\arguments{
  \item{x}{
A vector containing the names of the predictors in the model.
}
  \item{y}{
The name of the response variable in the model.
}
  \item{data}{
An \code{\linkS4class{H2OParsedData}} object containing the variables in the model.
}
  \item{family}{
A description of the error distribution and corresponding link function to be used in the model. Currently, Gaussian, binomial, Poisson, gamma, and Tweedie are supported. When a model is specified as Tweedie, users must also specify the appropriate Tweedie power. 
}
  \item{nfolds}{
(Optional) Number of folds for cross-validation. The default is 10.
}
  \item{alpha}{
(Optional) The elastic-net mixing parameter, which must be in [0,1]. The penalty is defined to be \deqn{P(\alpha,\beta) = (1-\alpha)/2||\beta||_2^2 + \alpha||\beta||_1 = \sum_j [(1-\alpha)/2 \beta_j^2 + \alpha|\beta_j|]} so \code{alpha=1} is the lasso penalty, while \code{alpha=0} is the ridge penalty.
}
  \item{lambda}{The shrinkage parameter, which multiples \eqn{P(\alpha,\beta)} in the objective. The larger \code{lambda} is, the more the coefficients are shrunk toward zero (and each other).
}
  \item{epsilon}{
(Optional) Number indicating the cutoff for determining if a coefficient is zero.
  }
  \item{standardize}{
(Optional) Logical value indicating whether the data should be standardized (set to mean = 0, variance = 1) before running GLM.
  }
  \item{tweedie.p}{
  (Optional) The index of the power variance function for the tweedie distribution. Only used if \code{family = "tweedie"}.}
  \item{thresholds}{
  (Optional) Degree to which to weight the sensitivity (the proportion of correctly classified 1s) and specificity (the proportion of correctly classified 0s). The default option is joint optimization for the overall classification rate. Changing this will alter the confusion matrix and the AUC. Only used if \code{family = "binomial"}.
  }
  \item{version}{
  (Optional) The version of GLM to run. \code{version = 1} corresponds to the more stable ValueArray implementation, while \code{version = 2} selects the faster, but still beta stage FluidVecs implementation.
  }
}
\details{
%%  ~~ If necessary, more details than the description above ~~
IMPORTANT: Currently, to run GLM you must import files to a ValueArray object using \code{\link{h2o.importFile.VA}}, \code{\link{h2o.importFolder.VA}} or one of its variants.
}
\value{
An object of class \code{\linkS4class{H2OGLMModel}} with slots key, data, model and xval. The slot model is a list of the following components:
\item{coefficients }{A named vector of the coefficients estimated in the model.}
\item{rank }{The numeric rank of the fitted linear model.}
\item{family }{The family of the error distribution.}
\item{deviance }{The deviance of the fitted model.}
\item{aic }{Akaike's Information Criterion for the final computed model.}
\item{null.deviance }{The deviance for the null model.}
\item{iter }{Number of algorithm iterations to compute the model.}
\item{df.residual }{The residual degrees of freedom.}
\item{df.null }{The residual degrees of freedom for the null model.}
\item{y }{The response variable in the model.}
\item{x }{A vector of the predictor variable(s) in the model.}
\item{auc }{Area under the curve.}
\item{training.err }{Average training error.}
\item{threshold }{Best threshold.}
\item{confusion }{Confusion matrix.}
The slot xval is a list of \code{\linkS4class{H2OGLMModel}} objects representing the cross-validation models. (Each of these objects themselves has xval equal to an empty list).
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
\code{\link{h2o.importFile.VA}}, 
\code{\link{h2o.importFolder.VA}}, 
\code{\link{h2o.importHDFS.VA}}, 
\code{\link{h2o.importURL.VA}},
\code{\link{h2o.uploadFile.VA}}
}
\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, promptUpgrade = FALSE)
# Run GLM of CAPSULE ~ AGE + RACE + PSA + DCAPS
prostate.hex = h2o.importURL.VA(localH2O, path = "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", key = "prostate.hex")
h2o.glm(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, family = "binomial", nfolds = 10, alpha = 0.5)
# Run GLM of VOL ~ CAPSULE + AGE + RACE + PSA + GLEASON
myX = setdiff(colnames(prostate.hex), c("ID", "DPROS", "DCAPS", "VOL"))
h2o.glm(y = "VOL", x = myX, data = prostate.hex, family = "gaussian", nfolds = 5, alpha = 0.1)

# Run GLM of IsDepDelayed ~ Distance + Origin + Dest + UniqueCarrier on airlines dataset in HDFS
airlines.hex =  h2o.importURL.VA(localH2O, path = "https://raw.github.com/0xdata/h2o/master/smalldata/airlines/AirlinesTrain.csv.zip")
h2o.glm(x = c('Distance', 'Origin', 'Dest', 'UniqueCarrier'), y = 'IsDepDelayed', family = 'binomial', data = airlines.hex)
h2o.shutdown(localH2O)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
