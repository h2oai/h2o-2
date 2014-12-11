

Admin: Jobs
=============


To access information on each action performed in an instance
of H2O, select **Jobs** from the drop-down **Admin** menu. Use the Jobs page to find .hex keys associated with models and parsed data, as well as information about current, recent, and cancelled jobs. 

**Table Definitions:**

 **Key** 

   Contains an "X" button for keys that can be removed from current set of H2O objects. 
   To remove a key, click the "X" button.
    
 **Description** 

   Displays a description of the activity associated with a particular key. For
   instance, a data set that has been parsed into .hex format will
   have "Parse" in the description field. 

 **Destination key**

   Displays the actual key associated with an H2O object. This unique identifier is used as a reference to the H2O object. 

 **Start Time** 

   Displays the start time of a job. 

 **End Time** 

   Displays the completion time of a job.

 **Progress**
  
   Displays a status bar to indicate job status and
   progress. 
   
   - Green but not full: The job is in progress but hasn't completed. 
   - Green and full: The job completed successfully.  
   - Red: The job was unable to complete or was cancelled. 

.. image:: AdminJobs.png
   :width: 100%


**Cancelled** 

  Displays "true" if the job was cancelled or "false" if the job was not cancelled. 

**Result**

  Displays the status of the job (for example, "OK" or "FAILED").
