.. _R_Related:

R FAQ
=====

  In order for H2O and R to work together, an H2O instance that is specified in the R workspace must be running. If the H2O instance is terminated, the H2O package in R will no longer work because R cannot send or receive information to or from H2O's distributed analysis. Even if a new instance of H2O with the exact same IP and port number is started, users must re-establish the connection between  H2O and R using the command `h2o.init()` and restart their H2O work session. 
  

**Updating the R Package to Avoid a Version Mismatch**

 To avoid a version mismatch when upgrading or changing your version of H2O in R, perform the following steps :

#. Close all open Java instances to ensure all H2O instances  have been properly shut down or terminated.

#. Uninstall the previous version of H2O from R by running:

	::
    
	  if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
	  if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }

1. For Windows, verify there are no H2O remnants in your personal R library.

2. Download and/or install the H2O package version by following the instructions in :ref:`R_Installation`.

3. If you still run into trouble with `h2o.init()`, try running the following command in the terminal:

	::
  
	  $ java -Xmx1g -jar h2o.jar

4. Try running `h2o.init()` in R again. If the problem persists, please contact us at support@h2o.ai.

""""""""""""

**How Do I Manage Dependencies in R?**

The H2O R package utilizes other R packages (like lattice and curl). Get the binary from CRAN directly and install the package manually using the following command:

::

  >install.packages("path/to/fpc/binary/file", repos = NULL, type = "binary")

Users may find this page on installing dependencies helpful:

http://stat.ethz.ch/R-manual/R-devel/library/utils/html/install.packages.html

""""""""""""""

**Why is only one CPU being used when I start H2O from R?**

Depending on your R installation, it may be configured to run with only one CPU by default.
This is particularly common for Linux installations and can affect H2O when you use the
`h2o.init() function` to start H2O from R.

To confirm this is the issue, look in **/proc/<nnnnn>/status** at the **Cpus_allowed bitmask** (where nnnnn is the PID of R).

::

  (/proc/<nnnnn>/status: This configuration is BAD!)
  Cpus_allowed:   00000001
  Cpus_allowed_list:      0

If you see a bitmask with only one CPU allowed, then any H2O process called by R will inherit this limitation.
As a workaround, set the following environment variable before starting R:

::

  $ export OPENBLAS_MAIN_FREE=1
  $ R

Now you should see something like the following in /proc/<nnnnn>/status

::

  (/proc/<nnnnn>/status: This configuration is good.)
  Cpus_allowed:   ffffffff
  Cpus_allowed_list:      0-31

At this point, the `h2o.init()` function will start an H2O instance that can use more than one CPU.

""""""""""""


**Internal Server Error in R**

::
  
  brew install gnu-tar
  cd /usr/bin
  sudo ln -s /usr/local/opt/gnu-tar/libexec/gnubin/tar gnutar
  
  
""""""""""""  


**The data imports correctly but names() is not returning the column names.**

Your version of R is outdated - update to at least R 3.0.


""""""""""""

**Why are string entries being converted into NAs during Parse?**

Currently, columns with numeric values will have the string entries converted to NAs when during data ingestion:

::

   Data Frame in R		Data Frame in H2O
	V1  V2  V3  V4		     V1  V2  V3  V4
   1     1   6  11   A		1     1   6  11  NA
   2	 2   B   A   A		2     2  NA  NA  NA
   3 	 3   A  13  18		3     3  NA  13  18
   4	 4   C  14  19		4     4  NA  14  19
   5     5  10  15  20		5     5  10  15  20

If the numeric values in the column are intended as additional factor levels, then you can concatenate the values with a string and the column will parse as a enumerator column:

::

	V1  V2  V3  V4
   1     1  i6 i11   A
   2     2   B   A   A
   3     3   A i13 i18
   4     4   C i14 i19
   5     5 i10 i15 i20
   
   
    
""""""""""""""  


**Why does as.h2o(localH2O, data) generate the following error: Column domain is too large to be represented as an enum : 10001>10000?**

Like `h2o.uploadFile`, `as.h2o` uses a limited push method, where the user initiates a request for information transfer. For bigger data files or files with more than 10000 enumerators in a column, we recommend
saving the file as a .csv and import the data frame using `h2o.importFile(localH2O, pathToData)`.

""""""""""""""""