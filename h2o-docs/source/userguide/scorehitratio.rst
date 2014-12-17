
Score: Hit Ratio
=================

The Hit Ratio is the number of times that a correct prediction was made in
ratio to the number of total prediction names. For example, if 20 out of 35 predictions
were made correctly, the hit ratio is 20:35 or .57. 


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
  predicted value. H2O does not require this value to be 0/1, but it should be a probability and not log-likelihood. 

