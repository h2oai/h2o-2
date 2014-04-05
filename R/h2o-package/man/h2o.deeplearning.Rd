\name{h2o.deeplearning}
\alias{h2o.deeplearning}
\title{
H2O: Deep Learning Neural Networks
}
\description{
Performs Deep Learning neural networks on an \code{\linkS4class{H2OParsedData}} object.
}
\usage{
h2o.deeplearning(x, y, data, classification = TRUE, activation = "Tanh", 
  dropout = as.numeric(NA), layers = 500, rate = 0.01, annealing_rate = 1e-6, 
  l1_reg = 1e-4, l2_reg = 0.0010, mom_start = 0.0, mom_ramp = 1e6, 
  mom_stable = 0.0, epochs = 100, validation)
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
(Optional) A string indicating the activation function to use. Must be either "Tanh", 'TanhDropout", "Rectifier", "RectifierDropout", "Maxout" or "MaxoutDropout".
}
  \item{dropout}{
(Optional) Input layer dropout ratio.
  }
  \item{layers}{
(Optional) Size of the hidden layers.
}
  \item{rate}{
(Optional) The learning rate. A higher learning rate will cause the algorithm to be less stable, while a lower learning rate will result in slower convergence.
}
  \item{annealing_rate}{
(Optional) The learning annealing rate is equal to \deqn{rate / (1 + annealing_rate * samples)}.
  }
  \item{l1_reg}{
(Optional) L1 regularization parameter.
  }
  \item{l2_reg}{
(Optional) L2 regularization parameter.
  }
  \item{mom_start}{
(Optional) Initial momentum at the beginning of training.
  }
    \item{mom_ramp}{
(Optional) Number of training samples for which momentum increases.
  }
    \item{mom_stable}{
(Optional) Final momentum after the ramp is over.
  }
  \item{epochs}{
(Optional) Number of times the dataset should be iterated. (Can be less than 1.0).
  }
  \item{validation}{
(Optional) An \code{\linkS4class{H2OParsedData}} object indicating the validation dataset used to construct confusion matrix. If left blank, this defaults to the training data.}
}

\value{
An object of class \code{\linkS4class{H2ODeepLearningModel}} with slots key, data, and model, where the last is a list of the following components:
  \item{confusion }{The confusion matrix of the response, with actual observations as rows and predicted values as columns.}
  \item{train_class_err }{Classification error on the training dataset.}
  \item{train_sqr_err }{Mean-squared error on the training dataset.}
  \item{valid_class_err }{Classification error on the validation dataset.}
  \item{valid_sqr_err }{Mean-squared error on the validation dataset.}
}

\examples{
library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE)
irisPath = system.file("extdata", "iris.csv", package = "h2o")
iris.hex = h2o.importFile(localH2O, path = irisPath)
h2o.deeplearning(x = 1:4, y = 5, data = iris.hex, activation = "Tanh", rate = 0.05)
h2o.shutdown(localH2O)
}
