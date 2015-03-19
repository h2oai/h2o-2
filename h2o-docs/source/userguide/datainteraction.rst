.. _DataInteraction:

Data: Interaction
====================

Create N-th order interaction terms between categorical features of an H2O frame, N=0,1,2,3,...

**destination key** 

  Enter the key assigned to the created frame which has one extra column appended. 


**source**

  Enter the name of the source 


**interaction columns**

  Select the columns to use for the interaction. 

**pairwise**

  To create pairwise quadratic interactions between factors, check this checkbox. Otherwise, one higher-order interaction is used. This option is only applicable is there are three or more factors.  


**max factors**

  Specify the maximum number of factor levels in pairwise interaction terms. If enabled, one extra catch-all factor is made. 


**min occurrence**

  Specify the minimum occurrence threshold for factor levels in pairwise interaction terms. 

