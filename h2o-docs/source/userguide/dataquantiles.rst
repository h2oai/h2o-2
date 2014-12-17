.. _DataQuantiles:

Data: Quantiles (Request)
==========================


**Source Key** 

  Specify a key associated with the data set. 

**Column**

  Specify a column for the quantiles. 

**Quantile**

  A value bounded on the interval (0,1), where X is the value below
  which X as a percentage of the data fall. For instance, if the
  quantile .25 is specified, the value returned is the value
  within the range of the column of data below which 25% of the data
  fall.
    
  The range is 0-1 and the default is 0.5.

**Max Qbins** 

  The number of bins into which the column should be split before the
  quantile is calculated. As the number of bins approaches the number
  of observations the approximate solution approaches the exact
  solution. 

**Multiple Pass**
  
  To specify the number of passes, enter a value (0-2):
  
  - *0*: Calculate the best approximation of the requested quantile in one pass. 
  - *1*: Return the exact result (with a maximum iteration of 16 passes)
  - *2*: Return both a single pass approximation and multi-pass exact answer. 

**Interpolation Type**

  If the quantile falls between two in-data values, you must interpolate the true value of the quantile. To interpolate the true value of the quantile, enter 2 to use mean interpolation or enter 7 to use linear interpolation. 

  - *2*: Mean interpolation
  - *7*: Linear interpolation

**Max ncols**

  Specify the maximum number of columns for the quantile. 
  
