General
=======

H\ :sub:`2`\ O System Requirements 
----------------------------------

- 64-bit `Java Runtime Environment <https://www.java.com/en/download/>`_ (version 1.6 or later) for users who only want to run H2O; for users who want to compile their own builds, we strongly recommend using `Java Development Kit 1.7 <www.oracle.com/technetwork/java/javase/downloads/>`_ or later.

- At least 2g of RAM is needed to run H2O. However, the amount of memory needed for H2O to run efficiently depends on the size and complexity of the data and the algorithm employed. A good heuristic is that the amount of memory allocated to H2O should be at least four times the size of the data. 

- A reasonably modern web browser (for example, the latest version of Firefox, Safari, or Internet Explorer.)

- If you are running H2O on a server, enable TCP and UDP communication so that H2O can communicate with the server. For more information, refer to the `FAQ <http://h2o.ai/product/faq/#ClusterErrNoForm>`_. Advanced users may find additional documentation on running in specialized environments helpful: :ref:`Developer`. 

- For multinode clusters using several servers, we strongly recommend configuring all servers and nodes symmetrically. For example, allocating different amounts of memory to nodes in the same cluster can adversely impact performance.   

""""""""""""""""""""""""""""""""

User Interaction
----------------

There are a variety of options for accessing and running H2O. For instructions on how to get started using H2O (for example, through R, using Java, or via github), please see the `Quick Start Videos <http://docs.h2o.ai/tutorial/videos.html>`_ and `Walk-Through Tutorials <http://docs.h2o.ai/tutorial/top.html>`_. Step-by-step instructions on how to use each of the algorithms and tools can be found in tutorials. There is also a useful :ref:`glossary` available that explains H2O's computing and statistics terms. 

- Use a web browser to communicate directly with the embedded web server inside any of the H2O nodes.  All H2O nodes contain an embedded web server and they are all equivalent peers. 

- Interface with the H2O embedded web server via the REST API. The REST API accepts HTTP requests and returns JSON-formatted responses. 

- Use the H2O for R package, available from h2o.ai. This package uses H2O's REST API under the hood. Users can install the R package from the H2O-maintained cran. The H2O zip file and R+ H2O installation details are available on our `download page <http://h2o.ai/download/>`_.


How Data is Ingested into H\ :sub:`2`\ O
----------------------------------------

Data sets are not transmitted directly through the REST API. Instead,
the user sends a command (containing an HDFS path to the data set,
for example) either through the browser-based GUI or the REST API to ingest
data from the disk. 

H2O assigns a Key to the data set that can be used as a reference in
the future commands to the web server. 

For step-by-step instructions on how perform data ingestion and
parsing, please see the **Data** section of this User Guide: :ref:`Data`. 

Supported input data file formats include CSV, Gzip-compressed CSV, MS
Excel (XLS), ARFF, SVM-Light, HIVE file format, and others. 

