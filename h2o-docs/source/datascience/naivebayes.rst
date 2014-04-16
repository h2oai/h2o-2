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
