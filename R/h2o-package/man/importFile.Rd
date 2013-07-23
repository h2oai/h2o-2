\name{importFile}
\alias{importFile}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Import Local Data File
}
\description{
Imports a file from the local path and parses it, returning an object containing the identifying hex key.
}
\usage{
importFile(object, path, key)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{object}{
An \code{\linkS4class{H2OClient}} object containing the IP address and port of the server running H2O.
}
  \item{path}{
The path of the file to be imported. Each row of data appears as one line of the file. If it does not contain an absolute path, the file name is relative to the current working directory.
}
  \item{key}{
(Optional) The unique hex key assigned to the imported file. If none is given, a key will automatically be generated based on the file path.
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
}
\value{
An object of class \code{\linkS4class{H2OParsedData}}.
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
h2o = new("H2OClient", ip="localhost", port=54321)
benign.hex = importURL(h2o, "../smalldata/logreg/benign.csv", "benign.hex")
summary(benign.hex)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
