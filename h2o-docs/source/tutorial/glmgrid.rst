.. _GLMgrid_tutorial:

GLM Grid Tutorial
-------------------

The purpose of this tutorial is to walk the new user through 
a GLM grid analysis beginning to end. The objective is to  learn how to
specify, run, and interpret a GLM grid model using  H\ :sub:`2`\ O.  
Specification of GLM grid models are similar to GLM models, and all
parameters and results have the same meaning. The primary difference
between GLM and GLMgrid is that users can specify several different
models, and generate the specified models simultaneously. For more
information visit the GLM tutorial :ref:`GLM_tutorial`

Those who have never used H\ :sub:`2`\ O before should see the quick
start guide for additional instructions on how to run H\ :sub:`2`\ O.


When to Use GLM Grid
"""""""""""""""""""""
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

  "Given a set of conditions, which units will fail?"

AND

The error rates in prediction are likely to be sensitive to the degree
of regularization applied, or specified thresholds.

  

Getting Started
"""""""""""""""
This tutorial uses a publicly available data set that can be found at:

http://archive.ics.uci.edu/ml/machine-learning-databases/abalone/ 

The original data are the Abalone data set made available by UCI
Machine Learning repository. They are composed of 4177 observations of
9 attributes. All attributes are real valued continuous,
except for Sex and Rings, found in columns 0 and 8 respectively. 
Sex is categorical with 3 levels (male, female, and infant), and Rings
is integer valued. 

Before modeling, parse data into H\ :sub:`2`\ O as follows:

#. Under the drop down menu **Data** select Upload, and use the helper to
   upload data.  


#. User will be redirected to a page with the header "Request
   Parse". Select whether the first row of the data set is a
   header. All other settings can be left in default. Press Submit. 


#. Parsing data into H\ :sub:`2`\ O generates a .hex key ("data name.hex")
 

.. image:: GLMparse.png
   :width: 80%



Building a Model
""""""""""""""""

#. Once  data are parsed a horizontal menu will appear at the top
   of the screen reading "Build model using ... ". Select 
   GLM here, or go to the drop down menu **Model** and
   select GLM. 


#. In the Key field enter the .hex key for the data set. 


#. In the Y field select the column associated with the Whole Weight
   variable (column 5). 


#. In the X field select the columns associated with Sex, Length,
   Diameter, Height, and Rings (all other columns). 


#. Specify the distribution family to be Gaussian. This automatically
   sets the link field to identity. 


#. Lambda and alpha are the parameters that determine the 
   regularization of GLM models. To find detailed information on the
   specification of tuning parameters see the data science
   documentation on GLM. In GLMgrid specification a range of values
   can be specified by entering the desired set of values as a
   comma separated list, for example: 0.001, 0.01, 0.1, 1 will produce
   models at each of the four specified levels. The same syntax holds
   for specification of alpha, and of thresholds.


#. Leave n-folds at 10. This will produce 10 cross-validation models
   for each unique combination of specified parameters.


#. Under the options box marked expert settings, notice that
   standardization is ON by default. This option returns two sets of
   coefficients, the non-standardized coefficients, and standardized
   coefficients.  


.. image:: GLMgridrequest.png
   :width: 90%




GLM Grid Results
"""""""""""""""""

GLM grid output includes a table of the specified models, along with
each model's corresponding specification values. Individual models can
be viewed by clicking on the active link for each model. 
For individual models coefficients (as well as normalized coefficients when
standardization is requested), AIC and error rate are returned. An
equation of the specified model is printed across the top
of the GLM results page in red. 



.. image:: GLMgridoutput1.png
   :width: 90%

Individual model results

.. image:: GLMgridoutput2.png
   :width: 90%



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



