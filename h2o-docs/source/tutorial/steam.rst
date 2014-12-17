.. _STEAM_tutorial:

Multimodel Scoring Tutorial
===========================

This tutorial describes H2O's new scoring feature through H2O's Multi-modeling Scoring Engine, and assumes some model-building experience in H2O. The Multi-modeling Scoring Engine makes model comparison and model selection easy and user-friendly.

""""""

Getting Started
"""""""""""""""

This tutorial uses a publicly available data set that can be found at:
https://github.com/0xdata/h2o/wiki/Hacking-Airline-DataSet-with-H2O

The data originated from RITA and can be downloaded in yearly chunks.  The data used in the following example has 43978 observations and 34 columns, of which eight are selected as  predictor variables. Out of the eight columns, four are integer columns, one is continuous real, and the remaining three are categorical features with levels 132, 10, and 134.

The data is then farther split into test and train subsets. The training set is used to build GLM, Random Forest, and GBM models. For instructions on how to build these models, refer to:

	:ref:`GLM_tutorial`	

	:ref:`RF_tutorial`

	:ref:`GBM_tutorial`

""""

Model Comparison
""""""""""""""""

On the main page, choose a dataset to generate a list of compatible models. The models are compatible with a chosen dataset when the headers of the specified dataset are the same as the headers of the data used to build the models on the list.

To use multi-model comparison, the models must use classification. The following model types are supported: 

- Random Forest (SpeeDRF and Big Data)
- GLM
- GBM
- Deep Learning


#. From the **Datasets** menu, select a .hex key for scoring purposes.

#. A list of compatible models displays to the right of the **Datasets** navigator. Select  *View These Models*.

	.. image:: STEAMdata.png
		:width: 100%

#. The **Models** page displays the compatible models. Check the checkboxes in the **Models** sidebar for the models to use for the comparison. To select all models, check the checkbox next to "Showing all models." After selecting the models, click the **Score** button at the top of the page.

#. In the "Select dataset" pop-up window that appears, select the dataset to use for scoring and click **OK**.

	.. image:: STEAMmodels.png
		:width: 100%

#. The **Scoring Comparison** page displays the scored models. Select all the scores and click *Compare*.
    
#. By default, the scoring comparison displays in the *Tabular* tab with validation data.

	.. image:: STEAMtabular.png
        	 :width: 100%

#. Click the *Advanced* tab to select the models that appear on the ROC chart graph.

.. image:: STEAMadvance.png
		:width: 100%
		
|
8. Select a basis for comparison (scoring or threshold),the X-axis value, and the Y-axis value, then click **OK**. 

""""

Adding visualizations
"""""""""""""""""""""
In the advanced tab, you can sort your models by the AUC value and choose the model that most accurately classifies your validation set. You can also add additional graphs and visuals to compare all the different statistics. For example, plot the models' AUC by training time.

#. Below the graph under "More Visualizations", click the **Add Visualizations** button.

#. From the drop-down *Compare* list, select "Scoring." 

#. From the drop-down *X-Axis Field* list, select "Training Time (ms)." 

#. From the drop-down *Y-Axis Field* list, select "AUC" and click "OK".

	.. image:: STEAMadd1.png
		:width: 50%


	.. image:: STEAMadd2.png
		:width: 50%

""""

