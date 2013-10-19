
GLM2
====

  

Supported HTTP methods and descriptions
---------------------------------------


URL
---

  http://<h2oHost>:<h2oApiPort>/GLM2.json

Input parameters
----------------


*  **destination_key**, a Key

   Destination key.  Since version 1

  
  **Possible JSON error field returns:**

  *  Argument 'destination_key' error: Argument 'destination_key' is required, but not specified

*  **source**, a Frame

   Source frame.  Since version 1

  
  **Possible JSON error field returns:**

  *  Argument 'source' error: Argument 'source' is required, but not specified



Output JSON elements
--------------------


*  **job_key**, a Key

   Job key.  Since version 1

*  **description**, a String

   Job description.  Since version 1

*  **start_time**, a long

   Job start time.  Since version 1

*  **end_time**, a long

   Job end time.  Since version 1

*  **exception**, a String

   Exception.  Since version 1



HTTP response codes
-------------------

  200 OK
  Success and error responses are identical.
