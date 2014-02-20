\name{h2o.importHDFS.FV}
\alias{h2o.importHDFS.FV}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
  Import from HDFS to FluidVecs
}
\description{
  Imports a HDFS file or set of files in a directory and parses them to FluidVecs data, returning a object containing the identifying hex key.
}
\usage{
h2o.importHDFS.FV(object, path, pattern = "", key = "", parse = TRUE, header, sep = "", col.names)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{object}{
    An \code{\linkS4class{H2OClient}} object containing the IP address and port of the server running H2O.
  }
  \item{path}{
    The path of the file or folder directory to be imported. If it does not contain an absolute path, the file name is relative to the current working directory.
  }
  \item{pattern}{
  (Optional) Character string containing a regular expression to match file(s) in the folder.
  }
  \item{key}{
  (Optional) The unique hex key assigned to the imported file. If none is given, a key will automatically be generated based on the file path.
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
(Optional) An \code{\linkS4class{H2OParsedData}} object containing the column header as its first and only row.
  }
}
\details{
This method is identical to \code{\link{h2o.importHDFS}} with input parameter \code{version = 2}.

When \code{path} is a directory, this method acts like \code{\link{h2o.importFolder}} and concatenates all data files in the folder into a single ValueArray object.

WARNING: In H2O, import is lazy! Do not modify the data files on hard disk until after parsing is complete.
}
\value{
If \code{parse = TRUE}, the function returns an object of class \code{\linkS4class{H2OParsedData}}, otherwise it returns an object of class \code{\linkS4class{H2ORawData}}. 
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
\seealso{
\code{\link{h2o.importFile.FV}, \link{h2o.importFolder.FV}, \link{h2o.importURL.FV}, \link{h2o.uploadFile.FV}}
}
\examples{
## Not run:
# This is an example of how to import files from HDFS.
# The user must modify the path to his or her specific HDFS path for this example to run.
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, promptUpgrade = FALSE)

iris.hex = h2o.importHDFS.FV(localH2O, path = "hdfs://192.168.1.161/datasets/runit/iris_wheader.csv", parse = TRUE)
summary(iris.hex)
iris_folder.hex = h2o.importHDFS.FV(localH2O, path = "hdfs://192.168.1.161/datasets/runit/iris_test_train")
summary(iris_folder.hex)
h2o.shutdown(localH2O)

## End(Not run)
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
