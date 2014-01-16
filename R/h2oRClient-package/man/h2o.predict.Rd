\name{h2o.predict}
\alias{h2o.predict}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
H2O Model Predictions
}
\description{
Obtains predictions from various fitted H2O model objects.
}
\usage{
h2o.predict(object, newdata)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{object}{
A fitted \code{\linkS4class{H2OModel}} object for which prediction is desired.
}
  \item{newdata}{
(Optional) A \code{\linkS4class{H2OParsedData}} object in which to look for variables with which to predict. If omitted, the data used to fit the model \code{object@data} are used.
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
}
\value{
A \code{\linkS4class{H2OParsedData}} object containing the predictions.
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
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, promptUpgrade = FALSE)
# Run GLM of CAPSULE ~ AGE + RACE + PSA + DCAPS
prostate.hex = h2o.importURL.VA(localH2O, 
 + path = "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", 
 + key = "prostate.hex")
prostate.glm = h2o.glm(y = "CAPSULE", x = c("AGE","RACE","PSA","DCAPS"), 
 + data = prostate.hex, family = "binomial", 
 + nfolds = 10, alpha = 0.5)
# Get fitted values of prostate dataset
prostate.fit = h2o.predict(object = prostate.glm, newdata = prostate.hex)
summary(prostate.fit)

# Run K-Means Model on prostate data set
covPath = system.file("extdata", "covtype.csv", package="h2oRClient")
covtype.hex = h2o.importFile(localH2O, path = covPath)
covtype.km = h2o.kmeans(data = covtype.hex, centers = 5, 
 + cols = c(1, 2, 3))
#Get predicted cluster information for covtype dataset
covtype.clusters = h2o.predict(object = covtype.km, newdata = covtype.hex)
summary(covtype.clusters)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
