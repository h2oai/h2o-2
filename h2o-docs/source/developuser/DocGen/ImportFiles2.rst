
ImportFiles2
============

  

Supported HTTP methods and descriptions
---------------------------------------

  GET
  Map a file from the local host filesystem into H2O memory.  Data is loaded lazily, when the Key is read (usually in a Parse2 command, to build a Frame key).  (Warning: Every host in the cluster must have this file visible locally!)

URL
---

  http://<h2oHost>:<h2oApiPort>/ImportFiles2.json

Input parameters
----------------


*  **path**, a ExistingFile

   Existing file or directory.  Since version 1

  
  **Possible JSON error field returns:**

  *  Argument 'path' error: File not found
  *  Argument 'path' error: Argument 'path' is required, but not specified



Output JSON elements
--------------------


*  **files**, a String[]

   Files imported.  Imported files are merely Keys mapped over the existing files.  No data is loaded until the Key is used (usually in a Parse command)..  Since version 1

*  **keys**, a String[]

   Keys of imported files, Keys map 1-to-1 with imported files..  Since version 1

*  **fails**, a String[]

   File names that failed the integrity check, can be empty..  Since version 1



HTTP response codes
-------------------

  200 OK
  Success and error responses are identical.

Success Example
---------------

  curl -s ImportFiles2.json?path=smalldata/airlines

  {"error":"Argument 'path' error: File smalldata/airlines not found"}

Error Example
-------------

  curl -s ImportFiles2.json

  {"error":"Argument 'path' error: Argument 'path' is required, but not specified"}
