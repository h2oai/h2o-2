.. _DataCreateFrame:

Data: Create Frame
====================

Create an H2O data frame with optional randomization; supports categoricals, integers, reals, and missing values.

**key** 

  Specify a name for the data frame.. 


**rows** 

  Specify the number of rows. 
  
**cols** 

  Specify the number of columns. 
  
**seed**

  Specify a random number seed. 
  
**randomize**

  To randomize the frame, check the **randomize** checkbox. 
  
**value**

  Specify a constant value. Only applicable if **randomize** is not enabled. 
  
**real range**

  Specify the range for real variables (-range ... range). 
  
**categorical fraction** 

  Specify the fraction of categorical columns. Only applicable is **randomize** is enabled. 
  
**factors**

  Specify the factor levels for categorical variables. 
  
**integer fraction**

   Specify the fraction of integer columns. Only applicable is **randomize** is enabled. 
   
**integer range**

  Specify the range for integer variables (-range ... range). 
  
**binary fraction** 

  Specify the fraction of binary columns. Only applicable is **randomize** is enabled. 
  
**binary ones fraction**

 Specify the fraction of ones in binary columns. 
 
**missing fraction** 

 Specify the fraction of missing values. 
 
**response factors**

  Specify the number of factor levels of the first column. Enter `1` for real, `2` for binomial, or `N` for multinomial. 
  
**has response**

  To generate an additional response column, check the **has response** checkbox.                   
    
 
  
  
  