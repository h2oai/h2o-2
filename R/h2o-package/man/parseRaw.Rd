\name{parseRaw}
\alias{parseRaw}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Parse Raw Data File
}
\description{
Parses a raw data file, returning an object containing the identifying hex key.
}
\usage{
parseRaw(data, key = "")
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{data}{
An \code{\linkS4class{H2ORawData}} object to be parsed.
}
  \item{key} {
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
h2o = new("H2OClient", ip = "localhost", port = 54321)
benign.raw = importFile(h2o, path="../smalldata/logreg/benign.csv", parse=FALSE)
benign.hex = parseRaw(benign.hex, "benign.hex")
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
