\name{H2ONNModel-class}
\Rdversion{1.1}
\docType{class}
\alias{H2ONNModel-class}
\alias{show,H2ONNModel-method}

\title{Class \code{"H2ONNModel"}}
\description{
A class for representing neural network models.
}
\section{Objects from the Class}{
Objects can be created by calls of the form \code{new("H2ONNModel", ...)}.
%%  ~~ describe objects here ~~ 
}
\section{Slots}{
  \describe{
    \item{\code{key}:}{Object of class \code{"character"}, representing the unique hex key that identifies the model.}
    \item{\code{data}:}{Object of class \code{\linkS4class{H2OParsedData}}, which is the input data used to build the model.}
    \item{\code{valid}:}{Object of class \code{"H2OParsedData"}, representing the validation data set.}
    \item{\code{model}:}{Object of class \code{"list"} containing the following elements:
      \itemize{
        \item{\code{confusion}: The confusion matrix of the response, with actual observations as rows and predicted values as columns.}
        \item{\code{train_class_err}: Classification error on the training dataset.}
        \item{\code{train_sqr_err}: Mean-squared error on the training dataset.}
        \item{\code{train_cross_entropy}: Cross-entropy on the training dataset.}
        \item{\code{valid_class_err}: Classification error on the validation dataset.}
        \item{\code{valid_sqr_err}: Mean-squared error on the validation dataset.}
        \item{\code{valid_cross_entropy}: Cross-entropy on the validation dataset.}
        }
    }
  }
}
\section{Extends}{
Class \code{"\linkS4class{H2OModel}"}, directly.
}
\section{Methods}{
  \describe{
    \item{show}{\code{signature(object = "H2ONNModel")}: ... }
	 }
}
\seealso{
%%  ~~objects to See Also as \code{\link{~~fun~~}}, ~~~
%%  ~~or \code{\linkS4class{CLASSNAME}} for links to other classes ~~~
\code{\link{h2o.nn}}
}
\examples{
showClass("H2ONNModel")
}
\keyword{classes}
