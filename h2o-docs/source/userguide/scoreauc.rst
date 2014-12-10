

Score: Area Under Curve
=========================

**Actual** 

  Specify a parsed data set containing the true (known) values of the binomial
  data being predicted. 

**Vactual** 

  Specify the column of binomial data from the data set specified in
  **Actual**. 

**Predict** 

  Specify the parsed data set containing the predicted values for the
  dependent variable in question.

**VPredict**

  Specify the column in the data set specified in **Predict** containing the
  predicted value. This value need not be 0/1, but should be a
  probability and not log-likelihood. 

**Threshold Criterion** 

  Specify the criterion to be used in calculating the AUC. 
  Current options include: 
  
  - maximum F1 
  - maximum F2
  - maximum F0point5
  - maximum Accuracy
  - maximum Precision
  - maximum Recall
  - maximum Specificity
  - maximum absolute MCC
  - minimization of Per Class Error
