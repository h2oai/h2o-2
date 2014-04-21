.. _NBmath:


Naive Bayes
------------------------------

Naive Bayes (NB) is a classification algorithm that relies on strong
assumptions of the independence of covariates in applying Bayes
Theorem. NB models are commonly used as an alternative to decision
trees for classification problems. 

  
  
Defining a Naive Bayes Model
"""""""""""""""""""""""""""""
**Source:**

  The hex key associated with the data to be modeled. 

**Response:**

  The dependent variable to be predicted by the model. 

**Ignored Columns:**

  The set of features in the training data set to be omitted from
  model training. 

**Laplace:**

  Laplace smoothing is used to circumvent the modeling issues that can
  arise when conditional probabilites are 0. In particular this can
  occurr when a rare event appears in holdout or prediction data, but
  did not appear in the training data. Smoothing modifies the maximum
  likelihood estimates used to generate classification probabilities
  even when unknown cases are encountered. 

Naive Bayes Algorithm and Implementation
""""""""""""""""""""""""""""""""""""""""""
The algorithm is presented for the simplified binomial case without
loss of genearlity.

Under the Naive Bayes assumption of independence, given a training set
for a set of discrete valued features X 
:math:`{(X^{(i)},\ y^{(i)};\ i=1,...m)}`

The joint likelihood of the data can be expressed as: 

:math:`\mathcal{L} \: (\phi(y),\: \phi_{i|y=1},\:
\phi_{i|y=0})=\Pi_{i=1}^{m} p(X^{(i)},\: y^{(i)})`

The model can be parameterized by:

:math:`\phi_{i|y=0}=\ p(x_{i}=1|\ y=0);\: \phi_{i|y=1}=\ p(x_{i}=1|y=1);\: \phi(y)`

Where :math:`\phi_{i|y=0}=\ p(x_{i}=1|\ y=0)` can be thought of as the
fraction of the observed instances where feature :math:`x_{i}` is
observed, and the outcome is :math:`y=0`, :math:`\phi_{i|y=1}=\
p(x_{i}=1|\ y=1)` is the fraction of the observed instances where feature :math:`x_{i}` is
observed, and the outcome is :math:`y=1`, and so on.

The objective of the algorithm is then to maximize with respect to
:math:`\phi_{i|y=0}, \ \phi_{i|y=1},\ and \ \phi(y)` 

Where the maximum likelihood estimates are: 

:math:`\phi_{j|y=1}= \frac{\Sigma_{i}^m 1(x_{j}^{(i)}=1 \ \bigcap y^{i} = 1)}{\Sigma_{i=1}^{m}(y^{(i)}=1}`

:math:`\phi_{j|y=0}= \frac{\Sigma_{i}^m 1(x_{j}^{(i)}=1 \ \bigcap y^{i} = 0)}{\Sigma_{i=1}^{m}(y^{(i)}=0}`

:math:`\phi(y)= \frac{(y^{i} = 1)}{m}`



