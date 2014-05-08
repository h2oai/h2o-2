General
=======

H\ :sub:`2`\ O System Requirements
-----------------------------------

64-bit Java 1.6 or higher (Java 1.7 is fine, for example)

While a minimum of 2g ram is needed on the machine where H\ :sub:`2`\
O will be running, the amount of memory needed for H\ :sub:`2`\ O to
run efficiently is dependent on the size and nature of data, and the
algorithm employed. A good heuristic is that the amount of memory
available be at least four times the size of the data being analyzed. 

A reasonably modern web browser (for example, the latest version of
Firefox, Safari or IE.)

Users who are running H\ :sub:`2`\ O on a server must ensure that the data are
available to that server (either via their network settings, or
because the data are on the same server.) Users who are running H\
:sub:`2`\ O on a laptop must ensure that the data are available to
that laptop. The specification of network settings is beyond the scope
of this documentation. Advanced users may find additional documentation on
running in specialized environments helpful: :ref:`Developer`. 

For multinode clusters utilizing several servers, it is strongly
recommended that all servers and nodes be symmetric and identically
configured. For example, allocating different amounts of memory to
nodes in the same cluster can adversely impact performance.   

User Interaction
----------------

Users have several options for interacting with H\ :sub:`2`\ O. 

A web browser can be used to communicate directly with the embedded
web server inside any of the H\ :sub:`2`\ O nodes.  All H\ :sub:`2`\ O
nodes contain an embedded web server, and they are all equivalent peers. 

Users can also choose to interface with the H\ :sub:`2`\ O embedded web server
via the REST API. The REST API accepts HTTP requests and returns
JSON-formatted responses. 

H\ :sub:`2`\ O can also be used via the H\ :sub:`2`\ O for R package,
available from 0xdata. This package uses H\ :sub:`2`\ O's REST API
under the hood. Users can install the R package from the  H\ :sub:`2`\
O maintained cran. The H\ :sub:`2`\ O zip file, and R+ H\ :sub:`2`\ O
installation details are available at: http://0xdata.com/downloadtable/. 

Data sets are not transmitted directly through the REST API. Instead,
the user sends a command (containing an HDFS path to the data set,
for example) either through the browser based GUI or via the REST API to ingest
data from disk. 

The data set is assigned a KEY in H\ :sub:`2`\ O that the user may refer to in
the future commands to the web server. 

How Data is Ingested into H\ :sub:`2`\ O
-----------------------------------------

For step by step instructions on how to carry out data ingestion and
parse, please see the **Data** section of this User Guide: :ref:`Data`. 

Supported input data file formats include CSV, Gzip-compressed CSV, MS
Excel (XLS), ARFF, SVM-Light, HIVE file format, and others. 


Using H\ :sub:`2`\ O
-----------------------

Step by step instructions on how to use each of the algorithms and
tools can be found in tutorials . Users have a variety of options for
accessing and running H\ :sub:`2`\ O. For instructions on how to get
started using H\ :sub:`2`\ O (for example through R, using Java, or
via git-hub), please see the Quick Start Guides, and Walk Through
Tutorials. New users may also find the :ref:`glossary` useful for 
familiarizing themselves with H\ :sub:`2`\ O's computing and statistics terms. 
