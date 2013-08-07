\name{h2o.startLauncher}
\alias{h2o.startLauncher}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Launch H2O GUI
}
\description{
Launches the H2O graphical user interface from inside R.
}
\usage{
h2o.startLauncher()
}
%- maybe also 'usage' for other objects documented here.
\details{
%%  ~~ If necessary, more details than the description above ~~
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
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.

## The function is currently defined as
function () 
{
    myOS = Sys.info()["sysname"]
    myHome = Sys.getenv("HOME")
    if (myOS == "Windows") 
        verPath = paste(myHome, "AppData/Roaming/h2o", sep = "/")
    else verPath = paste(myHome, "Library/Application Support/h2o", 
        sep = "/")
    myFiles = list.files(verPath)
    if (length(myFiles) == 0) 
        stop("Cannot find config file or folder")
    fileName = paste(verPath, tail(myFiles, n = 1), sep = "/")
    myVersion = strsplit(tail(myFiles, n = 1), ".txt")[[1]]
    launchPath = readChar(fileName, file.info(fileName)$size)
    if (is.null(launchPath) || launchPath == "") 
        stop(paste("No H2OLauncher.jar matching H2O version", 
            myVersion, "found"))
    temp = getwd()
    setwd(launchPath)
    if (myOS == "Windows") 
        shell.exec("H2OLauncher.jar")
    else system(paste("open", launchPath))
    setwd(temp)
  }
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
