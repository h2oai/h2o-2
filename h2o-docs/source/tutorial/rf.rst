.. _RF_tutorial:

Random Forest Tutorial
----------------------

This tutorial walks through a Random Forest analysis and describes how to specify, run, and interpret Random Forest.

If you have never used H2O before, refer to the quick start guide
for how to run H2O on your computer: :ref:`GettingStartedFromaZipFile`. 

""""

Getting Started
"""""""""""""""

This tutorial uses a publicly-available Internet ads data set that can be found at  http://archive.ics.uci.edu/ml/machine-learning-databases/internet_ads/ 

The data are composed of 3279 observations, 1557 attributes, and an a
priori grouping assignment. The objective is to build a prediction
tool that predicts whether an object is an internet ad or not.
 

#. From the drop-down **Data** menu, select **Upload** and use the uploader to
   upload data.
 
#. The "Request Parse" page appears. If first row of the data set is a
   header, check the "header" checkbox.  No other changes are required.  

#. Click **Submit**. Parsing data into H2O generates a .hex key ("data name.hex").

.. image:: RFParse.png
   :width: 100 %

""""


Building a Model
""""""""""""""""

After parsing the data, select either **Random Forest** or **BigData Random Forest** from the **Build models using...** links at the top of the page. 
Select **Random Forest** to create a SpeeDRF model for classification, or select **BigData Random Forest** a DRF model that can be either classification or regression. 


#. Once data are parsed a horizontal menu appears at the top
   of the screen that displays "Build model using ... ". Select 
   *Random Forest* here, or go to the drop-down **Model** menu  and
   find Random Forest there.

#. In the "source" field, enter the .hex key generated during data parsing. 


#. From the "response" drop-down list, select the column to use for the response variable. 


#. To exclude an attribute from tree building, select it in the "Ignored columns" section.

#. (For BigData only)Check the "classification" checkbox to use classification or uncheck it to use regression. 

#. In the "ntrees" field, specify the number of trees to build (in this case, 150).
 

#. Specify the number of features on which the trees will
   split in the "mtries" field. For this example, use 1000. 


#. Specify the maximum distance from root to terminal
   node in the "max depth" field. For the example, use the default values.  


#. Make a selection from the drop-down "select stat type" list to choose between split criteria. Entropy maximizes information gain, where Gini seeks to isolate the dominant category at each node. For this example, select Gini. 




#. To correct unbalanced data, use "balance classes" and "sampling strategy". For this example, use the default values.


#. Specify the proportion of observations sampled when building any given tree in the "sample rate" field. The omitted observations are run down the tree, and the classification error rate of that tree is estimated using the error rate from this holdout set. 


.. image:: RequestRF1.png
   :width: 90%

""""

RF Output
"""""""""



For a SpeeDRF model or BigData RF classification model, the RF output is a confusion matrix detailing the classification error rates for each level in the range of the target variable. In addition to the confusion matrix, the overall
classification error, the number of trees, and data use descriptives
are included in the output. 

.. image:: RFoutput2.png
   :width: 100%

|

RF inspect also includes information about the specified
tuning parameters at the top of RFView. To generate predictions for another dataset, click the "Score on dataset" link at the top of the page. 

""""


RF Predict
""""""""""

To generate a prediction, click the *Score on dataset* link at the top
of the RFView page or go to the drop-down **Score** menu and select *Predict*. 

The predict function requires a .hex key associated with a model. To find the .hex key, go to the drop-down **Admin** menu and select *Jobs*.

|

.. image:: DropdownAdmin.png
   :width: 90 %



.. image:: Jobspage.png
   :width: 100 %

|

All jobs created in the current instance of H2O are listed
here. Find the appropriate job (in this example, "Random Forest 150
Trees"). Save the associated key to clipboard, and paste it into the
model key field on the "Request Generate Predictions" Page. Enter a .hex
key associated with a parsed data set other than the one used to build
the model. 

|

.. Image:: RequestPredict.png
   :width: 90 %


""""
