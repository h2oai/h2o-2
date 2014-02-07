\name{h2o.importFolder.VA}
\alias{h2o.importFolder.VA}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
  Import Local Directory to ValueArray
}
\description{
  Imports all the files in the local directory and parses them, returning a list of objects containing the identifying hex keys.
}
\usage{
  h2o.importFolder.VA(object, path, pattern = "", parse = TRUE, header, sep = "", col.names)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{object}{
    An \code{\linkS4class{H2OClient}} object containing the IP address and port of the server running H2O.
  }
  \item{path}{
    The path of the folder directory to be imported. Each row of data appears as one line of the file. If it does not contain an absolute path, the file name is relative to the current working directory.
  }
  \item{pattern}{
(Optional) Character string containing a regular expression to match file(s) in the folder.
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
This method should only be used for running GLM. For all other algorithms, please use \code{\link{h2o.importFolder}}. WARNING: In H2O, import is lazy! Do not modify the data files on hard disk until after parsing is complete. By default, h2o.importFolder will automatically parse all files in the directory.
}
\value{
  If \code{parse = TRUE}, the function returns a list of objects of class \code{\linkS4class{H2OParsedDataVA}}, otherwise it returns a list of objects of class \code{\linkS4class{H2ORawDataVA}}.
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
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, 
 + silentUpgrade = TRUE, promptUpgrade = FALSE)
myPath = paste(path.package("h2oRClient"), "extdata", sep="/")
all_files.hex = h2o.importFolder.VA(localH2O, path = myPath)
for(i in 1:length(all_files.hex))
  print(summary(all_files.hex[[i]]))
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
