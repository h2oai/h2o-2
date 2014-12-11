.. _KM_tutorial:


K Means Tutorial
================

This tutorial walks through a K-Means analysis and describes how to specify, run, and interpret a K-means model in  H2O.

If you have never used H2O before, refer to the quick start guide
for additional instructions on how to run H2O: :ref:`GettingStartedFromaZipFile`. 

Interested users can find details on the math behind K Means at: :ref:`KMmath`.

""""

Quick Start Video
"""""""""""""""""

.. raw:: html

  <object width="420" height="315"><param name="movie" value="http://www.youtube.com/v/KVeKfoMRMyQ?version=3&amp;hl=en_US"></param><param name="allowFullScreen" value="true"></param><param name="allowscriptaccess" value="always"></param><embed src="http://www.youtube.com/v/KVeKfoMRMyQ?version=3&amp;hl=en_US" type="application/x-shockwave-flash" width="420" height="315" allowscriptaccess="always" allowfullscreen="true"></embed></object>
  
""""""""""""""  

Getting Started
"""""""""""""""

This tutorial uses a publicly available data set that can be found at http://archive.ics.uci.edu/ml/datasets/seeds


The data are composed of 210 observations, 7 attributes, and an priori
grouping assignment. All data are positively valued and
continuous. Before modeling, parse data into H2O:


#. From the drop-down **Data** menu, select **Upload** and use the helper to
   upload data.

 
#. On the  "Request Parse" page that appears, check the "header" checkbox if the first row of the data set is a header. No other changes are required. 


#. Click **Submit**. Parsing data into H2O generates a .hex key of the form  "data name.hex"

.. image:: KMparse.png
   :width: 90%

""""""

Building a Model
""""""""""""""""

#. Once  data are parsed, a horizontal menu appears at the top
   of the screen that displays "Build model using ... ". Select 
   K Means here, or go to the drop-down **Model** menu and
   select K-Means.


#. In the "source" field, enter the .hex key associated with the
   data set. 


#. Specify a value for "k." For this dataset, use 3.  


#. Check the "normalize" checkbox to normalize data, but this is not required for this
   example. 

#. Select an option from the "Initialization" drop-down list. 
	
   - Plus Plus initialization chooses one initial center at random and weights the random selection of subsequent centers so that points furthest from the first center are more likely to be chosen. 
   - Furthest initialization chooses one initial center at random, and then chooses the next center to be the  point furthest away in terms of Euclidean distance. 
   - The default ("None") results in K initial centers being chosen independently at random.  

#. Enter a "Max Iter" (short for maximum iterations) value to specify the maximum number of iterations the algorithm processes.

#. Select the columns of attributes that should be used 
   in defining the clusters in the "Cols" section. In this example, all columns except column 7 (the a priori known clusters for this particular set) are selected. 


#. Click **Submit**.

.. image:: KMrequest.png
   :width: 90%

""""""

K-Means Output
""""""""""""""

The output is a matrix of the cluster assignments and the
coordinates of the cluster centers (in terms of the originally 
selected attributes). Your cluster centers may differ slightly. 
K-Means randomly chooses starting points and converges on 
optimal centroids. The cluster number is arbitrary and should
be thought of as a factor. 

.. image:: KMinspect.png 
   :width: 100%

""""""

K-means Next Steps
"""""""""""""""""""

For more information about the model, select *K-Means* from the
drop-down **Score** menu. Specify the K-Means model key and the 
.hex key for the original data set. 

The output that displays when you click **Submit** is the number of rows 
assigned to each cluster and the squared error per cluster. 

.. image:: KMscore.png
   :width: 90%

""""""

K-means Apply
"""""""""""""

To generate a prediction (assign the observations in a data set
to a cluster), select **K-means Apply** from the drop-down **Score** menu.
Specify the model and the  .hex key for the data, then click **Submit**. 

In the following example, cluster assignments have been generated
for the original data. Because the data have been sufficiently well 
researched, the ideal cluster assignments were known in
advance. Comparing known clusters with predicted clusters demonstrated
that this K-Means model classifies with a less than 10% error rate. 

.. image:: KMapply.png
   :width: 90%

""""""
