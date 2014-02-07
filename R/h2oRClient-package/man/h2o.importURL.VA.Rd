\name{h2o.importURL.VA}
\alias{h2o.importURL.VA}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Import Data from URL to ValueArray
}
\description{
Imports a file from the URL and parses it, returning an object containing the identifying hex key.
}
\usage{
h2o.importURL.VA(object, path, key = "", parse = TRUE, header, sep = "", col.names)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{object}{
An \code{\linkS4class{H2OClient}} object containing the IP address and port of the server running H2O.
}
  \item{path}{
The complete URL of the file to be imported. Each row of data appears as one line of the file.
}
  \item{key}{
(Optional) The unique hex key assigned to the imported file. If none is given, a key will automatically be generated based on the URL path.
}
\item{parse}{
(Optional) A logical value indicating whether the file should be parsed after import.
  }
    \item{header}{
(Optional) A logical value indicating whether the first row is the column header. If missing, H2O will automatically try to detect the presence of a header.
  }
  \item{sep}{
(Optional) The field separator character. Values on each line of the file are separated by this character. If \code{sep = ""}, the parser will automatically detect the separator.
  }
  \item{col.names}{
(Optional) An \code{\linkS4class{H2OParsedDataVA}} object containing the column header as its first and only row.
  }
}
\details{
This method should only be used for running GLM. For all other algorithms, please use \code{\link{h2o.importFile}}. WARNING: In H2O, import is lazy! Do not modify the data on hard disk until after parsing is complete. By default, h2o.importURL will automatically parse the file.
}
\value{
If \code{parse = TRUE}, the function returns an object of class \code{\linkS4class{H2OParsedDataVA}}, otherwise it returns an object of class \code{\linkS4class{H2ORawDataVA}}.
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
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, 
 + promptUpgrade = FALSE)
prostate.hex = h2o.importURL.VA(localH2O, 
 + path = "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", 
 + key = "prostate.hex")
summary(prostate.hex)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
