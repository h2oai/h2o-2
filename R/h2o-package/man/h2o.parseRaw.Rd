\name{h2o.parseRaw}
\alias{h2o.parseRaw}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Parse Raw Data File
}
\description{
Parses a raw data file, returning an object containing the identifying hex key.
}
\usage{
h2o.parseRaw(data, key = "")
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{data}{
An \code{\linkS4class{H2ORawData}} object to be parsed.
}
  \item{key}{
(Optional) The hex key assigned to the parsed file.
  }
}
\details{
After the raw data file is parsed, it will be automatically deleted from the H2O server.
}
\value{
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
localH2O = new("H2OClient", ip = "localhost", port = 54321)
h2o.checkClient(localH2O)
prosPath = system.file("extdata", "prostate.csv", package="h2o")
prostate.raw = h2o.importFile(localH2O, path = prosPath, parse = FALSE)
# Do not modify prostate.csv on disk at this point!
prostate.hex = h2o.parseRaw(data = prostate.raw, key = "prostate.hex")
# After parsing, it is okay to modify or delete prostate.csv
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
