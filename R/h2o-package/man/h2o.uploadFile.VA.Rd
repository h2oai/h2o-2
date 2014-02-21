\name{h2o.uploadFile.VA}
\alias{h2o.uploadFile.VA}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Upload Local Data File to ValueArray
}
\description{
Uploads a file from the local drive and parses it to ValueArray data, returning an object containing the identifying hex key.
}
\usage{
h2o.uploadFile.VA(object, path, key = "", parse = TRUE, header, sep = "", col.names, silent = TRUE)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{object}{
An \code{\linkS4class{H2OClient}} object containing the IP address and port of the server running H2O.
}
  \item{path}{
The complete normalized file path of the file to be imported. Each row of data appears as one line of the file.
}
  \item{key}{
(Optional) The unique hex key assigned to the imported file. If none is given, a key will automatically be generated based on the URL path.
}
  \item{parse}{
(Optional) A logical value indicating whether the file should be parsed after import.
}
  \item{header}{
(Optional) A logical value indicating whether the first line of the file contains column headers. If left empty, the parser will try to automatically detect this.
  }
  \item{sep}{
(Optional) The field separator character. Values on each line of the file are separated by this character. If \code{sep = ""}, the parser will automatically detect the separator.
}
  \item{col.names}{
(Optional) A \code{\linkS4class{H2OParsedData}} object containing a single delimited line with the column names for the file.
  }
  \item{silent}{
(Optional) A logical value indicating whether or not to display an upload progress bar.
}
}

\details{
This method is identical to \code{\link{h2o.uploadFile}} with input parameter \code{version = 1}.

WARNING: In H2O, import is lazy! Do not modify the data on hard disk until after parsing is complete.
}
\value{
If \code{parse = TRUE}, the function returns an object of class \code{\linkS4class{H2OParsedDataVA}}, otherwise it returns an object of class \code{\linkS4class{H2ORawDataVA}}.
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
\code{\link{h2o.importFile.VA}, \link{h2o.importFolder.VA}, \link{h2o.importHDFS.VA}, \link{h2o.importURL.VA}}
}
\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE)
prostate.hex = h2o.uploadFile.VA(localH2O, path = system.file("extdata", "prostate.csv", package="h2o"), key = "prostate.hex")
summary(prostate.hex)
h2o.shutdown(localH2O)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
