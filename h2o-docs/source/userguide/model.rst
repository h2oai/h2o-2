
Model
=====


For detailed information on the specifications of each model, refer to :ref:`Data_Science`. Tutorials for each algorithm and model
specifications through R are available in the `Tutorial
Documentation <http://docs.h2o.ai/tutorial/top.html>`_. 

Specify a model by making a selection from the list of algorithms at the top of the **Inspect** page when data are parsed or by selecting the appropriate model from the drop-down **Model** menu. 

Each model requires a .hex key that is associated with a
data set. The entry fields use auto-complete, so you can often begin typing the name of the original data source to select the appropriate .hex key from the listed options. To locate .hex keys for data sets, select **View All** from the drop-down **Data** menu.  To locate .hex keys for all H2O actions, select **Jobs** from the drop-down **Admin** menu. 

If a large data set is used in the training and testing of a model,
H2O's capabilities are constrained by the amount of memory available on
the machine. To utilize H2O's full capability, the amount of memory
available should be about four times the file size of the data set, but
not more than the machine's total available memory. For instructions
on how to change the amount of memory allocated to H2O, see the "Important Notes" section in :ref:`GettingStartedFromaZipFile`. Advanced users should run H2O on a cloud
computing resource or server. 

""""""""""""""""""""""

Grid Search Models
-------------------

GLM and GBM both offer Grid Search Models. To use this
option in GLM, select **GLM Grid Search** from the 
drop-down **Model** menu. 

Each grid search modeling option allows users to generate multiple models
simultaneously and compare model criteria directly, rather than building models separately one at a time. Specify multiple
model configurations by entering different values of tuning parameters
separated by commas. For example, to specify three different values of
lambda (a regularization parameter in GLM Grid search), users might
enter: .001, .05, .1. 

When multiple values are specified for many tuning
parameters, grid search returns one model for each unique
combination. For example, in GBM, if users specify Ntrees as 50, 100,
200 and specify learning rates of 0.01 and 0.05, six models
are returned. 

The grid search results display in a table, showing the combination of tuning
parameters used for each model and basic model evaluation information,
as well as a link to each model. To access the details of each
model, click the model links in the table.


