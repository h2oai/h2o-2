\name{h2o.glm}
\alias{h2o.glm}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
H2O: Generalized Linear Model
}
\description{
Fit a generalized linear model, specified by a response variable, a set of predictors, and a description of the error distribution.
}
\usage{
h2o.glm(x, y, data, family, nfolds = 10, alpha = 0.5, lambda = 1e-05)
}
%- maybe also 'usage' for other objects documented here.
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
A description of the error distribution and corresponding link function to be used in the model. Currently, Gaussian, binomial, Poisson, and gamma are supported.
}
  \item{nfolds}{
Number of folds for cross-validation. The default is 10.
}
  \item{alpha}{
The elastic-net mixing parameter, which must be in [0,1]. The penalty is defined to be \deqn{P(\alpha,\beta) = (1-\alpha)/2||\beta||_2^2 + \alpha||\beta||_1 = \sum_j [(1-\alpha)/2 \beta_j^2 + \alpha|\beta_j|]} so \code{alpha=1} is the lasso penalty, while \code{alpha=0} is the ridge penalty.
}
  \item{lambda}{The shrinkage parameter, which multiples \eqn{P(\alpha,\beta)} in the objective. The larger \code{lambda} is, the more the coefficients are shrunk toward zero (and each other).
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
}
\value{
An object of class \code{\linkS4class{H2OGLMModel}} with slots key, data, and glm, where the last is a list of the following components:
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
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
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
# Run GLM of CAPSULE ~ AGE + RACE + PSA + DCAPS
prostate.hex = importURL(h2o, "https://raw.github.com/0xdata/h2o/master/smalldata/
   logreg/prostate.csv", "prostate.hex")
h2o.glm(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), data = prostate.hex, family = 
"binomial", nfolds = 10, alpha = 0.5)

# Run GLM of VOL ~ CAPSULE + AGE + RACE + PSA + GLEASON
myX = setdiff(colnames(prostate.hex), c("ID", "DPROS", "DCAPS", "VOL"))
h2o.glm(y = "VOL", x = myX, data = prostate.hex, family = "gaussian", nfolds = 5, alpha = 0.1)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
