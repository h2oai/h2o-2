
Score
=====

.. toctree::
   :maxdepth: 1

   scorePOJO
   scorepredict
   scoreconfusionmatrix
   scoreauc
   scorehitratio
   scorePCAscore
   scoregainslift
   scoreMultiModel
   
   
   
   
   
   
   
   
""""""""" 

Scoring Models
--------------

To find detailed information on the specification for scoring models generated
by a particular Algorithm, refer to the `Algorithms <http://docs.h2o.ai/datascience/top.html>`_ section of
the H2O documentation. 

In general, score models by choosing the appropriate algorithm
from the drop-down **Score** menu. Scoring requires the same format for testing data that was used for the training data. 

""""""""""

Predicting
----------

To leverage the model's predictive capability, go to the drop-down **Score** menu and select **Predict**. A column for the predicted variable is not required, but the data should correspond to the training data in all other dimensions. 

The prediction output is a single-column vector that corresponds row-for-row with the data set used for the prediction. H2O assigns a .hex key to the prediction that can be viewed by clicking the drop-down **Admin** menu and selecting **Jobs**. 

To download the prediction output to the working directory as a .csv file, click the CSV link at the top of the **Prediction Results** page.

 
