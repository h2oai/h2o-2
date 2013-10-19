\name{h2o.pcr}
\alias{h2o.pcr}

\title{H2O: Principal Components Regression}

\description{
Runs GLM regression on PCA results, and allows for transformation of test data to match PCA transformations of training data.  
}
\usage{
h2o.pcr(x, y, data, ncomp, family, nfolds = 10, alpha = 0.5, lambda = 1e-05, tweedie.p)
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
  \item{ncomp}{
    A number indicating the number of principal components to use in the regression model.
  }
  \item{family}{
    A description of the error distribution and corresponding link function to be used in the model. Currently, Gaussian, binomial, Poisson, gamma, and Tweedie are supported.
  }
  \item{nfolds}{
    Number of folds for cross-validation. The default is 10.
  }
  \item{alpha}{
    The elastic-net mixing parameter, which must be in [0,1]. The penalty is defined to be \deqn{P(\alpha,\beta) = (1-\alpha)/2||\beta||_2^2 + \alpha||\beta||_1 = \sum_j [(1-\alpha)/2 \beta_j^2 + \alpha|\beta_j|]} so \code{alpha=1} is the lasso penalty, while \code{alpha=0} is the ridge penalty.
  }
  \item{lambda}{
    The shrinkage parameter, which multiples \eqn{P(\alpha,\beta)} in the objective. The larger \code{lambda} is, the more the coefficients are shrunk toward zero (and each other).
  }
  \item{tweedie.p}{The index of the power variance function for the tweedie distribution. Only used if \code{family = "tweedie"}
  }
}

\details{
%%  ~~ If necessary, more details than the description above ~~
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
}


\references{
%% ~put references to the literature/web site here ~
}
\author{
%%  ~~who you are~~
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
}
\examples{
library(h2o)
h2o.installDepPkgs()
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, promptUpgrade = FALSE)
# Run PCR on Prostate Data
prostate.hex = h2o.importURL(localH2O, path = "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", key = "prostate.hex")
h2o.pcr(x = c("AGE","RACE","PSA","DCAPS"), y = "CAPSULE", data = prostate.hex, family = "binomial", nfolds = 10, alpha = 0.5)
}
