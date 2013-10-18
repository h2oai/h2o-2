\name{h2o.setColNames}
\alias{h2o.setColNames}
\title{
Set Column Names of Data Set
}
\description{
Sets the column names of a parsed data set to the values provided in a separate file.
}
\usage{
h2o.setColNames(data, col.names)
}
\arguments{
  \item{data}{
An \code{\linkS4class{H2OParsedData}} object containing the data set whose column names will be changed.
  }
  \item{col.names}{
An \code{\linkS4class{H2OParsedData}} object containing the new column names.
  }
}
\details{
This function modifies the \code{data} file directly. It does not create a new copy, and thus does not take up much additional memory. If the number of columns in \code{data} and \code{col.names} do not match, then it will simply set the names of the provided columns (from left to right) and skip the rest.
}
\value{
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
\references{
%%  ~~ possibly secondary sources and usages ~~
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
irisPath = system.file("extdata", "iris.csv", package="h2oRClient")
iris.hex = h2o.importFile(localH2O, path = irisPath, key = "iris.hex")
summary(iris.hex)
namesPath = system.file("extdata", "iris_header.csv", package="h2oRClient")
names.hex = h2o.importFile(localH2O, path = namesPath, key = "iris_names.hex")
h2o.setColNames(data = iris.hex, col.names = names.hex)
summary(iris.hex)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
