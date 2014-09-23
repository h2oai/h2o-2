.. _R_Related:

R and H\ :sub:`2`\ O
====================

  In order for H\ :sub:`2`\ O and R to work together, an instance of
  H\ :sub:`2`\ O must be running, and that instance of H\ :sub:`2`\ O
  must be specified in the R workspace. If the H\ :sub:`2`\ O instance
  is terminated the H\ :sub:`2`\ O package in R will no longer work
  because R will no longer be able to send information to 
  H\ :sub:`2`\ O's distributed analysis, and will no longer be able to
  get info mation back. Even if a new instance of H\ :sub:`2`\ O is
  started with the exact same IP and port number, users
  will need to reestablish the connection between  H\ :sub:`2`\ O and R
  using the call h2o.init(), and will have
  to restart their H\ :sub:`2`\ O work session. 
  

**Updating the R Package aka. Avoid Version Mismatch!**

H\ :sub:`2`\ O's R package is now available for download on CRAN but typically the 0xdata team continues to push new releases faster than CRAN typically upload more recent packages. To avoid a version mismatch when upgrading or changing your version of H\ :sub:`2`\ O in R please run through the following steps :

#. Close any Java instances up to kill any rogue H\ :sub:`2`\ O instances that hasn't been properly shut down or terminated.

#. Uninstall previous version of H\ :sub:`2`\ O from R by running :

	::
    
	  if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
	  if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }

#. For Windows especially check to make sure there are no remanants of H\ :sub:`2`\ O in your personal R library.

#. Download and/or install the H\ :sub:`2`\ O package version by following the instructions in our R user documentation.

#. If you still run into trouble with h2o.init() try running in the terminal:

	::
  
	  $ java -Xmx1g -jar h2o.jar

#. Go back to R and try running h2o.init() again. If the problem persist please contact us at support@0xdata.com.


**How Do I Manage Dependencies in R?**

The H\ :sub:`2`\ O R package utilizes other R packages (like lattice, and curl). From time to time
R will fail to download from CRAN and give an error. In that case it's best to get the binary from C
RAN directly and install the package manually using the call:

::

  >install.packages("path/to/fpc/binary/file", repos = NULL, type = "binary")

Users may find this page on installing dependencies helpful:

http://stat.ethz.ch/R-manual/R-devel/library/utils/html/install.packages.html


**Why is only one CPU being used when I start H2O from R?**

Depending on how you got your version of R, it may be configured to run with only one CPU by default.
This is particularly common for Linux installations.  This can affect H\ :sub:`2`\ O when you use the
h2o.init() function to start H\ :sub:`2`\ O from R.

You can tell if this is happening by looking in /proc/<nnnnn>/status at the Cpus_allowed bitmask (where nnnnn is the PID of R).

::

  (/proc/<nnnnn>/status: This configuration is BAD!)
  Cpus_allowed:   00000001
  Cpus_allowed_list:      0

If you see a bitmask with only one CPU allowed, then any H\ :sub:`2`\ O process forked by R will inherit this limitation.
To work around this, set the following environment variable before starting R:

::

  $ export OPENBLAS_MAIN_FREE=1
  $ R

Now you should see something like the following in /proc/<nnnnn>/status

::

  (/proc/<nnnnn>/status: This configuration is good.)
  Cpus_allowed:   ffffffff
  Cpus_allowed_list:      0-31

At this point, the h2o.init() function will start an H2O that can use more than one CPU.


**Internal Server Error in R**

::
  
  brew install gnu-tar
  cd /usr/bin
  sudo ln -s /usr/local/opt/gnu-tar/libexec/gnubin/tar gnutar


**The data import correctly but names() is not returning the column names.**

The version of R you are using is outdated, update to at least R 3.0.

**Why are string entries being converted into NAs during Parse?**

At the moment columns with numeric values will have the string entries converted to NAs when the data is being ingested:

::

   Data Frame in R		Data Frame in H2O
	V1  V2  V3  V4		     V1  V2  V3  V4
   1     1   6  11   A		1     1   6  11  NA
   2	 2   B   A   A		2     2  NA  NA  NA
   3 	 3   A  13  18		3     3  NA  13  18
   4	 4   C  14  19		4     4  NA  14  19
   5     5  10  15  20		5     5  10  15  20

If the numeric values in the column were meant to be additional factor levels then you can concatenate the values with a string and the column will parse as a enumerator column:

::

	V1  V2  V3  V4
   1     1  i6 i11   A
   2     2   B   A   A
   3     3   A i13 i18
   4     4   C i14 i19
   5     5 i10 i15 i20


**Why does as.h2o(localH2O, data) generate the error: Column domain is too large to be represented as an enum : 10001>10000?**

as.h2o like h2o.uploadFile uses a limited push method where the user initiates a request for information transfer; so it is recommended for bigger data files or files with more than 10000 enumerators in a column to
save the file as a csv and import the data frame using h2o.importFile(localH2O, pathToData).