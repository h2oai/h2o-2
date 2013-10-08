\name{h2o.importHDFS}
\alias{h2o.importHDFS}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
  Import from HDFS
}
\description{
  Imports a HDFS file or set of files in a directory and parses them, returning a list of objects containing the identifying hex keys.
}
\usage{
h2o.importHDFS(object, path, parse = TRUE)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{object}{
    An \code{\linkS4class{H2OClient}} object containing the IP address and port of the server running H2O.
  }
  \item{path}{
    The path of the file or folder directory to be imported. If it does not contain an absolute path, the file name is relative to the current working directory.
  }
  \item{parse}{
  (Optional) A logical value indicating whether the file should be parsed after import.
  }
}
\details{
WARNING: In H2O, import is lazy! Do not modify the data files on hard disk until after parsing is complete. By default, h2o.importHDFS will automatically parse the file.
}
\value{
When the path is a directory, if \code{parse = TRUE}, the function returns a list of objects of class \code{\linkS4class{H2OParsedData}}, otherwise it returns a list of objects of class \code{\linkS4class{H2ORawData}}. When the path is a single file, the same holds, except the function returns a single object rather than a list.
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
\code{\link{h2o.importFolder}}, \code{\link{h2o.importFile}}, \code{\link{h2o.importURL}}
}
\examples{
library(h2oWrapper)
h2oWrapper.installDepPkgs()
localH2O = h2oWrapper.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, promptUpgrade = FALSE)
covtype.hex = importHDFS(localH2O, path = "hdfs://192.168.1.173:54321/0xdiag/datasets/standard/covtype.data", parse = TRUE)
summary(covtype.hex)

stdfolder.data = importHDFS(localH2O, path = "hdfs://192.168.1.173:54321/0xdiag/datasets/standard", parse = FALSE)
stdfolder.parsed = vector("list", length(stdfolder.data))
for(i in 1:length(stdfolder.data))
  stdfolder.parsed[[i]] = h2o.parseRaw(stdfolder.data[[i]])
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
