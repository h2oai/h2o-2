Admin: Cluster Status 
=========================

To verify the status and location of a cluster, select **Cluster Status** from the drop-down **Admin** menu. In the table that displays, the information for each node in an H2O cloud displays in a separate row. A description for each column is provided below.

 **Name** 
  
   The name of the node. For example, if a user establishes three
   nodes on different servers, then the Name column displays the IP address
   and port used to communicate with each of those unique nodes. 

 **Num Keys** 

   The number of keys in the distributed value store. 

 **Value size bytes**  

   The aggregate size (in bytes) of all data on that
   node (including the set or subset of parsed data, but also
   the size in bytes of the information stored in the keys generated
   from modeling or data manipulation.)

 **Free mem bytes** 

    The amount of free memory in the H2O node.

 **Tot mem bytes** 

   The total amount of memory in the H2O node. This value may vary
   over time depending on use.

 **Max mem bytes** 

    The maximum amount of memory that the H2O node will
    attempt to use. 

 **Free disk bytes** 

   The amount of free memory in the ice root. When memory needs exceed
   the capacity of the node, the overflow is handled by ice root, which is the
   H2O corollary to disk memory.

 **Max disk bytes** 

   The maximum amount of memory that can be used from ice root. 

 **Num cpus** 

   The number of cores used by the node.


 **System Load** 
 
   The amount of computational work currently being performed by the
   cluster. 

 **Node Healthy** 

   Indicates if the node indicated by the row of the cluster
   status table is healthy or not.

 **PID**

   Process ID number. 

 **Last contact** 

   The last time a specific node relayed communication about its
   status. This column should read "now" or contain a number less than 30
   (seconds). If last contact is indicated to be more than 30 seconds
   ago, the node may be experiencing a failure. 

*Definitions for Fj threads hi, Fj threads low, Fj queue hi, Fj queue
low, RCPS, and TCPS Active have been omitted.* These fields are
primarily designed for use by H2O programmers, and are in development. It
is likely that they will be removed in a future revision. 
