.. _R_Installation:

Install H\ :sub:`2`\ O package in R
===================================

Currently, there are two different ways to install the H2O package in R. If you are using R 2.13.0 or later, the following instructions describe how to download from CRAN, how to download the build from the h2o.ai website, and how to install from the most recent source code.

- The h2o.ai `website <http://h2o.ai/download/>`_ has the most recent stable releases of H2O as well as the bleeding edge nightly build. 
- CRAN has a policy of updating packages every few weeks to months so the most recent or the last stable release would be available
- `GitHub <http://github.com/h2oai>`_ has most recent changes committed and a build will be made nightly from the source code; however, stability is not guaranteed.

""""""""""""""""""""""""""


Quick Start Video
"""""""""""""""""

.. raw:: html

 <object width="425" height="344"><param name="movie"
 value="http://www.youtube.com/v/deRpTB9k77k&hl=en&fs=1"></param><param
 name="allowFullScreen" value="true"></param><embed
 src="http://www.youtube.com/v/deRpTB9k77k&hl=en&fs=1"
 type="application/x-shockwave-flash" allowfullscreen="true"
 width="425" height="344"></embed></object>

""""""""""""""""""""""

Dependencies
""""""""""""

**Note**: If you are using RStudio, clear the workspace before installing or updating packages. 


- **For OS X:** In a new terminal window, enter ``sudo apt-get install libcurl4-openssl-dev``. After the download completes, open R and enter ``install.packages("RCurl")``. 

- **For Windows**: `Download <http://curl.haxx.se/dlwiz/>`_ the latest Curl package. Select **curl executable** as the package type, **Windows/Win32** or **Win64** (depending on your version of Windows), and select **Generic** as the flavour. If you selected **Windows/Win32** as the operating system, select **Unspecified** as the version. Download the file, extract it, and install it in R. 
	
- **For Linux (CentOS)**: After you install RStudio, install the ``R-devel`` package using ``yum install R-devel.x86_64``. Then, install curl-config using ``yum install libcurl-devel.x86_64``.


The H2O package is built with some required packages. To install the packages, use ``install.packages()`` (for example, ``install.packages(RCurl)``). 
To properly install H2O, install the following dependencies:

    - RCurl
    - rjson
    - statmod
    - survival 
    - stats
    - tools
    - utils 
    - methods
    
If your machine does not have curl-config, you must install the dependencies outside of R. Refer to the directions above for your OS.  

Depending on your system configuration, installation of R packages may fail due to dependencies. Before launching H2O, confirm the packages installed correctly by looking for the following output in R (where `<PackageName>` represents the R package and `<DirectoryPath>` represents the directory path of the package):  

::

	trying URL 'http://cran.rstudio.com/bin/macosx/mavericks/contrib/3.1/<PackageName>'
	Content type 'application/x-gzip' length 160850 bytes (157 Kb)
	opened URL
	==================================================
	downloaded 157 Kb


	The downloaded binary packages are in
		<DirectoryPath>

::

If the package did not install successfully, the following output displays:

::

	> install.packages("RCurl")
	Installing package into ‘/home/kevin/.Rlibrary’
	(as ‘lib’ is unspecified)
	trying URL 'http://cran.stat.ucla.edu/src/contrib/RCurl_1.95-4.5.tar.gz'
	Content type 'application/x-tar' length 878607 bytes (858 Kb)
	opened URL
	==================================================
	downloaded 858 Kb

	* installing *source* package ‘RCurl’ ...
	** package ‘RCurl’ successfully unpacked and MD5 sums checked
	checking for curl-config... no
	Cannot find curl-config
	ERROR: configuration failed for package ‘RCurl’
	* removing ‘/home/kevin/.Rlibrary/RCurl’

	The downloaded source packages are in
    ‘/tmp/RtmpnAjlbd/downloaded_packages’
	Warning message:
	In install.packages("RCurl") :
  	installation of package ‘RCurl’ had non-zero exit status

::

   
Download zip file from h2o.ai
"""""""""""""""""""""""""""""

**Step 1**

Download a release from our `website <http://h2o.ai/download/>`_. The downloaded package will contain both the
H2O jar file as well as the R tar package file for R installation. After download completes, unzip the file and navigate to the
R subdirectory with the tar package.

::

  $ unzip h2o-SUBST_PROJECT_VERSION.zip
  $ cd h2o-SUBST_PROJECT_VERSION/R
  $ pwd
    ~/Downloads/h2o-SUBST_PROJECT_VERSION/R


**Step 2**

Start R or Rstudio and install the R client package by running `install.packages` and entering the location of the tar file. To verify the installation, load the library
and check that a simple demo script runs.

::

  > install.packages("~/Downloads/h2o-SUBST_PROJECT_VERSION/R/h2o_SUBST_PROJECT_VERSION.tar.gz",
    repos = NULL, type = "source")
  > library(h2o)
  > demo(h2o.glm)
  
""""""""""""""""""""""""  

Download from CRAN
""""""""""""""""""

When downloading from CRAN, keep in mind that the initial download from CRAN contains only the R package. When running `h2o.init()` for the first time, R automatically downloads the corresponding H2O jar file before launching H2O.

::

  > install.packages("h2o")
  > library(h2o)
  > localH2O = h2o.init()

  H2O is not running yet, starting it now...
  Performing one-time download of h2o.jar from
        http://s3.amazonaws.com/h2o-release/h2o/rel-knuth/11/Rjar/h2o.jar
  (This could take a few minutes, please be patient...)
  
  
""""""""""""""""""""""""""
  

Download R Package directly from h2o.ai
"""""""""""""""""""""""""""""""""""""""""""

Download one of releases available on our `website <http://h2o.ai/download/>`_. Select the **INSTALL IN R** tab, then copy and paste the following code into R to install:
::

  # The following two commands remove any previously installed H2O packages for R.
  if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
  if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }

  # Next, we download, install and initialize the H2O package for R.
  install.packages("h2o", repos=(c("http://s3.amazonaws.com/h2o-release/h2o/master/1497/R", getOption("repos"))))
  library(h2o)
  localH2O = h2o.init()

  # Finally, let's run a demo to see H2O at work.
  demo(h2o.glm)
  
  
""""""""""""""""""""""""""""""  
  

Make a build from Git
"""""""""""""""""""""

**Step 1**

If you are a developer who wants to make changes to the R package before building and installing it, pull the
source code from `Git <https://github.com/h2oai/h2o>`_ and follow the instructions in `From Source Code (Github) <http://docs.h2o.ai/developuser/quickstart_git.html#quickstartgit>`_.

**Step 2**

After making the build, navigate to the Rcran folder with the R package in the build's directory, then run and install.

::

  Amy@LENOVO-PC ~/Documents/h2o/target/Rcran (master)
  $ R CMD INSTALL h2o_SUBST_PROJECT_VERSION.tar.gz
  * installing to library 'C:/Users/Amy/Documents/R/win-library/3.0'
  * installing *source* package 'h2o' ...
  ** R
  ** demo
  ** inst
  ** preparing package for lazy loading
  Warning: package 'statmod' was built under R version 3.0.3
  Creating a generic function for 'summary' from package 'base' in package 'h2o'
  Creating a generic function for 'colnames' from package 'base' in package 'h2o'
  Creating a generic function for 't' from package 'base' in package 'h2o'
  Creating a generic function for 'colnames<-' from package 'base' in package 'h2o'
  Creating a generic function for 'nrow' from package 'base' in package 'h2o'
  Creating a generic function for 'ncol' from package 'base' in package 'h2o'
  Creating a generic function for 'sd' from package 'stats' in package 'h2o'
  Creating a generic function for 'var' from package 'stats' in package 'h2o'
  Creating a generic function for 'as.factor' from package 'base' in package 'h2o'
  Creating a generic function for 'is.factor' from package 'base' in package 'h2o'
  Creating a generic function for 'levels' from package 'base' in package 'h2o'
  Creating a generic function for 'apply' from package 'base' in package 'h2o'
  Creating a generic function for 'findInterval' from package 'base' in package 'h2o'
  ** help
  *** installing help indices
  ** building package indices
  ** testing if installed package can be loaded
  *** arch - i386
  Warning: package 'statmod' was built under R version 3.0.3
  *** arch - x64
  Warning: package 'statmod' was built under R version 3.0.3
  * DONE (h2o)


**Step 3**

Verify that H2O installed properly:

::

  > library(h2o)
  > localH2O = h2o.init()


""""""""""""""""""""""""""""""""

Upgrading Packages
""""""""""""""""""

When upgrading H2O, upgrade the R package as well. To prevent a version mismatch, we
recommend manually upgrading R packages. For example, if you are running the bleeding edge developer build,
it’s possible that the code has changed, but that the revision number has not. In this case, manually upgrading ensures the most
current version of not only the H2O code, but the corresponding R code as well.

Simply detach the package and remove it from R before going through the installation process again:

::

  > if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
  > if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }

