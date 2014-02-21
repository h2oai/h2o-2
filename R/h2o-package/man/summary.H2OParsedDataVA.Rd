\name{summary.H2OParsedDataVA}
\alias{summary.H2OParsedDataVA}
\title{ Summarizes the columns of an H2O parsed ValueArray data set. }
\description{
\code{summary.H2OParsedDataVA}, a method for the \code{\link{summary}} generic. Summarizes the columns of an H2O parsed object or subset of columns and rows using vector notation (e.g. dataset[row, col])
}
\usage{\method{summary}{H2OParsedDataVA}(object, ...)}
\arguments{
  \item{object}{
  An \code{\linkS4class{H2OParsedDataVA}} object.}
  \item{...}{Additional arguments affecting the summary produced. (Currently unimplemented).}
}
\value{
A matrix displaying the minimum, 1st quartile, median, mean, 3rd quartile and maximum for each numeric column included in the request call,a summary of the levels and member counts for each factor column.  and a the levels and member counts of the elements in factor columns for all of the columns specified in the summary call. 
}
\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE)
prosPath = system.file("extdata", "prostate.csv", package="h2o")
prostate.hex = h2o.importFile.VA(localH2O, path = prosPath)
summary(prostate.hex)
summary(prostate.hex$GLEASON)
summary(prostate.hex[,4:6])
h2o.shutdown(localH2O)
}
