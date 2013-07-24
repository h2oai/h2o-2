\name{importFolder}
\alias{importFolder}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
  Import Local Directory
}
\description{
  Imports all the files in the local directory and parses them, returning a list of objects containing the identifying hex keys.
}
\usage{
  importFolder(object, path, parse = TRUE)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{object}{
    An \code{\linkS4class{H2OClient}} object containing the IP address and port of the server running H2O.
  }
  \item{path}{
    The path of the folder directory to be imported. Each row of data appears as one line of the file. If it does not contain an absolute path, the file name is relative to the current working directory.
  }
}
\details{
WARNING: In H2O, import is lazy! Do not modify the data files on hard disk until after parsing is complete.
}
\value{
  If \code{parse = TRUE}, the function returns a list of objects of class \code{\linkS4class{H2OParsedData}}, otherwise it returns a list of objects of class \code{\linkS4class{H2ORawData}}.
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
  glm_test.hex = importFolder(h2o, "../smalldata/glm_test")
  for(i in 1:length(glm_test.hex))
    print(summary(glm_test.hex[[i]]))
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
