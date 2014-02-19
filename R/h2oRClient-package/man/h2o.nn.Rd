\name{h2o.nn}
\alias{h2o.nn}
\title{
H2O: Neural Networks
}
\description{
Performs neural networks on an \code{\linkS4class{H2OParsedData}} object.
}
\usage{
h2o.nn(x, y, data, classification = TRUE, activation = "Tanh", layers = 500, rate = 0.01, l1_reg = 1e-4, l2_reg = 0.0010, epoch = 100, validation)
}

%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{x}{
A vector containing the names of the predictors in the model.
}
  \item{y}{
The name of the response variable in the model.
}
  \item{data}{
An \code{\linkS4class{H2OParsedData}} object containing the variables in the model.
}
  \item{classification}{
(Optional) A logical value indicating whether the algorithm should conduct classification.
}
  \item{activation}{
(Optional) A string indicating the activation function to use. Must be either "Tanh" or "Rectifier".
}
  \item{layers}{
(Optional) Size of the hidden layers.
}
  \item{rate}{
(Optional) The learning rate. A higher learning rate will cause the algorithm to be less stable, while a lower learning rate will result in slower convergence.
}
  \item{l1_reg}{
(Optional) L1 regularization parameter.
  }
  \item{l2_reg}{
(Optional) L2 regularization parameter.
  }
  \item{epoch}{
(Optional) Number of times the dataset should be iterated. (Can be less than 1.0).
  }
  \item{validation}{
(Optional) An \code{\linkS4class{H2OParsedData}} object indicating the validation dataset used to construct confusion matrix. If left blank, this defaults to the training data.}
}

\value{
An object of class \code{\linkS4class{H2ONNModel}} with slots key, data, and model, where the last is a list of the following components:
  \item{confusion }{The confusion matrix of the response, with actual observations as rows and predicted values as columns.}
  \item{train_class_err }{Classification error on the training dataset.}
  \item{train_sqr_err }{Mean-squared error on the training dataset.}
  \item{train_cross_entropy }{Cross-entropy on the training dataset.}
  \item{valid_class_err }{Classification error on the validation dataset.}
  \item{valid_sqr_err }{Mean-squared error on the validation dataset.}
  \item{valid_cross_entropy }{Cross-entropy on the validation dataset.}
}

\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, promptUpgrade = FALSE)
irisPath = system.file("extdata", "iris.csv", package="h2oRClient")
iris.hex = h2o.importFile(localH2O, path = irisPath)
h2o.nn(x = 1:4, y = 5, data = iris.hex, activation = "Rectifier", rate = 0.05)
h2o.shutdown(localH2O)
}
