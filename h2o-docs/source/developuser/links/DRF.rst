
DRF
===

  

Supported HTTP methods and descriptions
---------------------------------------


URL
---

  http://<h2oHost>:<h2oApiPort>/DRF.json

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

*  **response**, a Vec

   Column to use as class.  Since version 1

  
  **Possible JSON error field returns:**

  *  Argument 'response' error: Argument 'response' is required, but not specified

*  **classification**, a boolean

   Do Classification or regression.  Since version 1

*  **validation**, a Frame

   Validation frame.  Since version 1

*  **ntrees**, a int

   Number of trees.  Since version 1

*  **max_depth**, a int

   Maximum tree depth.  Since version 1

*  **min_rows**, a int

   Fewest allowed observations in a leaf.  Since version 1

*  **nbins**, a int

   Build a histogram of this many bins, then split at the best point.  Since version 1

*  **mtries**, a int

   Columns to randomly select at each level, or -1 for sqrt(#cols).  Since version 1

*  **sample_rate**, a float

   Sample rate, from 0. to 1.0.  Since version 1

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

*  **_ncols**, a int

   Active feature columns.  Since version 1

*  **_nrows**, a long

   Rows in training dataset.  Since version 1

*  **_nclass**, a int

   Number of classes.  Since version 1

*  **_ymin**, a int

   Minimum class number, generally 0 or 1.  Since version 1

*  **_distribution**, a long[]

   Class distribution, ymin based.  Since version 1



HTTP response codes
-------------------

  200 OK
  Success and error responses are identical.
