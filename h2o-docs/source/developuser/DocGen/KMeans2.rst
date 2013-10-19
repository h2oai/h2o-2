
KMeans2
=======

  

Supported HTTP methods and descriptions
---------------------------------------

  GET
  k-means

URL
---

  http://<h2oHost>:<h2oApiPort>/KMeans2.json

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

*  **cols**, a int[]

   Input columns (Indexes start at 0).  Since version 1

*  **ignored_cols_by_name**, a int[]

   Ignored columns by name.  Since version 1

*  **initialization**, a Initialization

   Clusters initialization.  Since version 1

*  **k**, a int

   Number of clusters.  Since version 1

  
  **Possible JSON error field returns:**

  *  Argument 'k' error: Argument 'k' is required, but not specified

*  **max_iter**, a int

   Maximum number of iterations before stopping.  Since version 1

  
  **Possible JSON error field returns:**

  *  Argument 'max_iter' error: Argument 'max_iter' is required, but not specified

*  **normalize**, a boolean

   Whether data should be normalized.  Since version 1

*  **seed**, a long

   Seed for the random number generator.  Since version 1



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

*  **iterations**, a int

   Iterations the algorithm ran.  Since version 1



HTTP response codes
-------------------

  200 OK
  Success and error responses are identical.
