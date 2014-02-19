\name{h2o.parseRaw.FV}
\alias{h2o.parseRaw.FV}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Parse Raw Data File to FluidVecs
}
\description{
Parses a raw data file to FluidVecs data, returning an object containing the identifying hex key.
}
\usage{
h2o.parseRaw.FV(data, key = "", header, sep = "", col.names)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{data}{
An \code{\linkS4class{H2ORawData}} object to be parsed.
}
  \item{key}{
(Optional) The hex key assigned to the parsed file.
  }
    \item{header}{
(Optional) A logical value indicating whether the first row is the column header. If missing, H2O will automatically try to detect the presence of a header.
  }
  \item{sep}{
(Optional) The field separator character. Values on each line of the file are separated by this character. If \code{sep = ""}, the parser will automatically detect the separator.
  }
  \item{col.names}{
(Optional) An \code{\linkS4class{H2OParsedData}} object containing the column header as its first and only row.
  }
}
\details{
This method should only be used to parse raw data imported using \code{\link{h2o.importFile.FV}}, \code{\link{h2o.importFolder.FV}}, or one of its variants. After the raw data file is parsed, it will be automatically deleted from the H2O server.
}
\value{
An object of class \code{\linkS4class{H2OParsedData}}, representing the parsed FluidVecs dataset.
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
\code{\link{h2o.importFile.FV}, \link{h2o.importFolder.FV}, \link{h2o.importHDFS.FV}, \link{h2o.importURL.FV}, \link{h2o.uploadFile.FV}}
}
\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, promptUpgrade = FALSE)
prosPath = system.file("extdata", "prostate.csv", package="h2oRClient")
prostate.raw = h2o.importFile.FV(localH2O, path = prosPath, parse = FALSE)
# Do not modify prostate.csv on disk at this point!
prostate.hex = h2o.parseRaw.FV(data = prostate.raw, key = "prostate.hex")
# After parsing, it is okay to modify or delete prostate.csv
h2o.shutdown(localH2O)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
