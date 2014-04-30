.. _GLM_tutorial:

GLM Tutorial
==============

The purpose of this tutorial is to walk the new user through 
Generalized Linear Analysis (GLM)  using   H\ :sub:`2`\ O.  
Users who have never used H\ :sub:`2`\ O before should see
:ref:`GettingStartedFromaZipFile` for additional instructions on how
to run H\ :sub:`2`\ O.


When to Use GLM
"""""""""""""""
The variable of interest relates to predictions or
inferences about a rate, an event, or a continuous 
measurement. Questions are about how a set of environmental 
conditions influence the dependent variable. 

Here are some examples: 
  
  "What attributes determine which customers will purchase, and which will not?"

  "Given a set of specific manufacturing conditions, how many units
  produced will fail?"

  "How many customers will contact help support in a given time
  frame?"

  

Getting Started
"""""""""""""""
This tutorial uses a publicly available data set that can be found at:

`UCI Machine Learning 

http://archive.ics.uci.edu/ml/machine-learning-databases/abalone/ 

The original data are the Abalone data, available from UCI
Machine Learning Repository. They are composed of 4177 observations on
9 attributes. All attributes are real valued, and continuous,
except for Sex and Rings, found in columns 0 and 8 respectively. 
Sex is categorical with 3 levels (male, female, and infant), and Rings
is an integer valued count. 

Before modeling, parse data into H\ :sub:`2`\ O: 

#. Under the drop down menu **Data** select *Upload*, and use the helper to
   upload data.  


#. User will be redirected to a page with the header "Request
   Parse". Select whether the first row of the data set is a
   header. All other settings can be left in default. Press Submit. 


#. Parsing data into H\ :sub:`2`\ O generates a .hex key of the form  "data name.hex"
 

.. image:: GLMparse.png
   :width: 100%



Building a Model
""""""""""""""""

#. Once data are parsed, go to the drop down menu **Model** and
   select *GLM*. 


#. In the **Source** field enter the .hex key for the data set. 


#. In the **Response** field select the column associated with the Whole Weight
   variable (column 5). 


#. In the **Ignored Columns** field select the columns associated with  (all other columns). 

#. Leave **Classification** and **Max Iter** in default. Classification is
   used when the dependent variable is a binomial classifier. Max iter
   is used to define the maximum number of iterations to be carried
   out by the algorithm in the event that it fails to converge. 

#. Leave the **Standardize** option unchecked (off). 


#. Set **Nfolds** equal to 0. When Nfolds is specified to be greater
   than 0, the GLM model will return N number of cross validation
   models. 

#. Specify **Family** to be *Gaussian*. 

#. Leave **Tweedie Variance Power** at zero; this option is only used
   for the Tweedie family of GLM models (like zero-inflated Poisson). 

#. Set **Alpha** equal to .3. The alpha parameter is the mixing
   parameter for L1 and L2 penalty.


#. Set **Lambda** equal to .002

#. Leave all other options in default, and press the **Submit**
   button. 


.. image:: GLMrequest.png
   :width: 100%



GLM Results
"""""""""""

GLM output includes coefficients (as well as normalized coefficients when
standardization is requested). Also reported are AIC and
error rate. An equation of the specified model is printed across the top
of the GLM results page in red. 



.. image:: GLMoutput.png
   :width: 100%



Validating on Testing Set
"""""""""""""""""""""""""
#. Models can be applied to holdout testing sets or prediction data,
   provided that the data are in the same format as the data
   originally used to generate the GLM model. 

#. At the top of the GLM results page is a horizontal menu titled
   **Actions**. Select Validate On Another Dataset. This same action can
   be completed by going to the **Score** drop down menu and selecting
   GLM.
 

#. In model key enter the .hex key found in the center of the GLM
   results page under the header **Validations** (this can also be found
   under the **Admin** drop down menu by selecting **Jobs**). 


#. In the Key field enter the .hex key associated with the testing
   data set. Press submit. 


Validation results report the same model statistics as were generated
when the model was originally specified.

.. image:: GLMvresults.png
   :width: 100%


THE END. 



