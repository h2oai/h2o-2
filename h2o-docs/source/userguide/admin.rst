The Admin Menu
================


Jobs
-----

Users can access information on each action undertaken in an instance
of H\ :sub:`2`\ O by accessing Jobs under the Admin drop down menu. From here it
is possible to find .hex keys associated with models and parsed
data. If a an action was cancelled, that information will be displayed here as 
well.

**Table Definitions:**

 **Key** 

   Table field appears with an "X" button for those keys
   that can be removed from current set of H\ :sub:`2`\ O objects. 
   For instance, if a user ran several different GLM models, but
   wishes to keep only one of these, other models can be removed by
   clicking on the "X" button. 

 **Description** 

   A description of the activity associated with a particular key. For
   instance, a data set that has been parsed into .hex format will
   have "Parse" in the description field. 

 **Destination key**

   The actual key associated with an H\ :sub:`2`\ O object. This can be thought
   of like a corollary to a file path on a users local computer. 

 **Start Time** 

   Time when a job was started. 

 **End Time** 

   Time when a job completed.

 **Progress**
  
   A status bar providing a visual indicator of job status and
   progress. Green and filling means that job is proceeding, but
   hasn't completed, green and full means that job completed
   successfully, and red means that the job was unable to complete or
   cancelled by user. 

.. image:: AdminJobs.png
   :width: 100%


**Cancelled** 

  A column indiciating whether the job was cancelled. 

**Result**

  A column indicating the status of the job.


Cluster Status
--------------

The status and location of a cluster can be verified by selecting
Cluster Status from the Admin drop down menu. 


 **Table Definitions:**
   In the provided table each node in an H\ :sub:`2`\ O cloud has a
   row of information. 


 **Name** 
  
   The name of the node. For example, if a user establishes three
   nodes on different servers, then Name will display the IP address
   and port in use for talking to each of those unique nodes. 

 **Num Keys** 

   The number of keys in the distributed value store. 

 **Value size bytes**  

   The aggregate size in bytes of all data on that
   node (including the set or subset of a users parsed data, but also
   the size in bytes of the information stored in the keys generated
   from modeling or data manipulation.)

 **Free men bytes** 

    The amount of free memory in the H\ :sub:`2`\ O node.

 **Tot mem bytes** 

   The total amount of memory in the H\ :sub:`2`\ O node. This value may vary
   over time depending on use.

 **Max men bytes** 

    The maximum amount of memory that the H\ :sub:`2`\ O node will
    attempt to use. 

 **Free disk bytes** 

   The amount of free memory in the ice root. When memory needs exceed
   the capacity of the node, the overflow is handled by ice root, the
   H\ :sub:`2`\ O corollary to disk memory.

 **Max disk bytes** 

   The maximum amount of memory that can be used from ice root. 

 **Num cpus** 

   The number of cores being used by the node


 **System Load** 
 
   The amount of computational work currently being carried out by the
   cluster. 

 **Node Healthy** 

   Indiacates whether the node indicated by the row of the cluster
   status table is healthy.

 **PID**

   Process ID number. 

 **Last contact** 

   The last time a specific node relayed communication about its
   status. Last contact should read "now" or some number less than 30
   seconds. If last contact is indicated to be more than 30 seconds
   ago, the node may be experiencing a failure. 

*Definitions for Fj threads hi, Fj threads low, Fj queue hi, Fj queue
low, RCPS, and TCPS Active have been omitted.* These fields are
primarily designed for by H\ :sub:`2`\ O programmers, and are in development. It
is likely that they will be removed in a future revision. 


Inspect Log
-------------

Users can inspect a log of all  H\ :sub:`2`\ O activities. 
Logs can be downloaded by clicking on the Download All Logs button in
the upper left hand corner of the page. 

.. image:: Logview.png
   :width: 100%



Shutdown
--------

When users are finished running a particular instance of H\ :sub:`2`\ O, the
program should be exited by selecting Shutdown from the Admin drop
down menu. Even if the user closes the browser window in which the H\ :sub:`2`\ O
instance is running, without explicitly stopping H\ :sub:`2`\ O, the cluster
associated with an H\ :sub:`2`\ O instance still exists. The user could return to
the browser based interface at any time, and access all of the prior
jobs within that instance of H\ :sub:`2`\ O. Resources are still being allocated
to H\ :sub:`2`\ O. In order to entirely quit an instance of H\ :sub:`2`\ O and free up the
resources allocated to the program, the user may use Shutdown to kill
the cluster.

.. image:: AdminShutdown.png
   :width: 100%
 


Advanced
--------

Timeline: displays for the user a time ordered list of H\ :sub:`2`\ O status
events (for example, when a cluster was started). When running a multi
cluster analysis, it may happen from time to time that a node
dies. This could occur for instance, if a server goes down. 


.. image:: AdminTimeline.png
   :width: 100%


Stack Dump: Advanced users and those oriented toward programming can
find error information here.  

.. image:: AdminStack.png
   :width: 100%





 


