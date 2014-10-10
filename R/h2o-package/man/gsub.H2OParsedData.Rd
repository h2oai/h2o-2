\name{gsub.H2OParsedData}
\alias{gsub.H2OParsedData}
\title{Pattern Replacement}
\description{ \code{gsub.H2OParsedData}, a method for the \code{\link{gsub}} base method.}
\usage{\method{gsub}{H2OParsedData}(pattern, replacement, x, ignore.case, perl, fixed, useBytes)}
\arguments{
 \item{pattern}{A regex or string to match on.}
 \item{replacement}{A string that replaces the matched pattern.}
  \item{x}{An \code{\linkS4class{H2OParsedData}} object with a single factor column.}
  \item{ignore.case}{If TRUE, case will be ignored in the pattern match}
    \item{fixed}{ Used by the base method. Ignored by H2OParsedData strsplit. }
    \item{perl}{ Used by the base method. Ignored by H2OParsedData strsplit. }
    \item{useBytes}{ Used by the base method. Ignored by H2OParsedData strsplit. }
  }
\details{
  Matches a pattern and replaces all instances of the
  matched pattern with the replacement string.
}
\value{
An object of class "H2OParsedData".
}

\examples{
library(h2o)
localH2O <- h2o.init(ip = "localhost", port = 54321, startH2O = TRUE)
df <- data.frame(
    V1 = c("HELLO WoR@&^LD", "the dOg ATE", "my friENd BOb Ace", "mEow meOW"),
    V2 = c(92318, 34891.123, 21,99))
hex <- as.h2o(localH2O, df)
gsub("HELLO", "WHY HELLO THERE", hex$V1)
}
