\name{h2o.interaction}
\alias{h2o.interaction}

\title{
Create interaction terms between categorical features of an H2O Frame
}
\description{
Create N-th order interaction terms between categorical features of an H2O Frame, N=0,1,2,3,...
}
\usage{
h2o.interaction(data, key, factors, max_factors, min_occurrence)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{data}{ An \code{\linkS4class{H2OParsedData}} object containing the variables in the model. }
  \item{key}{The unique hex key assigned to the created frame which has one extra column appended.}
  \item{factors}{Column indices of factors for which interaction is to be computed}
  \item{max_factors}{Max. number of factor levels in pair-wise interaction terms (if enforced, one extra catch-all factor will be made).}
  \item{min_occurrence}{Min. occurrence threshold for factor levels in pair-wise interaction terms.}
}
\value{
Returns an H2O data frame.
}
\examples{
library(h2o)
localH2O = h2o.init(beta = TRUE)
myframe = h2o.createFrame(localH2O, 'myframekey', rows = 20, cols = 5,
                          seed = -12301283, randomize = TRUE, value = 0,
                          categorical_fraction = 1.0, factors = 5, real_range = 1,
                          integer_fraction = 0, integer_range = 1, missing_fraction = 0.1,
                          response_factors = 1)[,-1]
interaction <- h2o.interaction(myframe, factors = c(3,1), max_factors = 10, min_occurrence = 1)
head(interaction, 20)
myframe <- cbind(myframe, interaction)
names(myframe)[ncol(myframe)] <- names(interaction)
head(myframe,20)
summary(myframe)
h2o.shutdown(localH2O)
}
