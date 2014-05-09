.. _Data:

Data
=====

Ingesting Data
---------------

Ingesting data is the process of moving data from outside of H\ :sub:`2`\ O into
the running instance of H\ :sub:`2`\ O. To ingest data start from the drop down
menu **Data**, and select the appropriate option. Options and their uses are described below. 

 **Import Files:**

   In the path field specify an absolute path to the
   file. For example: Users/UserName/Work/dataset.csv. Press submit. 

   On the resulting screen the specified path will appear as a
   highlighted link. Clicking on the path automatically parses the 
   data. 

 **Import URL:** 

   Copy the URL where the raw data are displayed into the URL
   field. Users may wish to specify a Key; one is usually assigned
   using the original file name. In this case the URL will become part
   of the .hex, unless Key is otherwise specified.  For example, 
   original data can be found at: 
   http://archive.ics.uci.edu/ml/machine-learning-databases/internet_ads/ad.data

   Once the data are imported, users will be automatically sent to the
   Import URL page, where they can click on the KEY.  This automatically
   goes to the Inspect page. Users should not be worried at this point
   if data do not look as expected. This will be corrected when data are
   parsed.  

 **Import S3:** 

   In the field marked Bucket give the path to an existing AWS bucket
   where data are stored. 

 **Upload:**

    Click on the **Select File** button. A menu of files on the 
    computer or working directory will appear. Select the appropriate
    file, and click on **Choose.** When returned to the H\ :sub:`2`\ O
    screen press **Upload.**

 

Parsing Data
------------

Once data are ingested, they are available to H\ :sub:`2`\ O, but are
not yet in a format that H\ :sub:`2`\ O can process. Converting the data to 
an H\ :sub:`2`\ O usable format is called parsing. 

After ingestion users are directed to a **Request Parse** screen. To
parse data users can leave most options in default. For example, H\ :sub:`2`\ O
automatically determines separators in data sets. For most data
formats users will be automatically redirected to a page to request
parse, where they can simply press submit. Exceptions to this are
noted below. Once data are parsed a .hex key is displayed for the
user. This .hex key will be used to refer to the data set in all H\ :sub:`2`\ O
analysis, and should be noted. It can also be found at a later time
through the Admin menu by selecting Jobs, or through the **Data**
menu, by choosing **View All.** 

 **Import URL:**
   
   Click on "Parse into .hex format" displayed at the top of
   the inspect page after data are inhaled. Import URL takes users
   directly to parse. 

 **Parser Behavior**

   The data type in each column must be consistent. For example, when
   data are alpha-coded categorical, all entries must be alpha or
   alpha numeric. If numeric entries are detected by the parser, the
   column will not be processed. It will register all entries as
   NA. This is also true when NA entries are included in columns
   consisting of numeric data. Columns of alpha coded categorical
   variables containing NA entries will register NA as a distinct
   factor level. When missing data are coded as periods or dots in the
   original data set those entries are converted to zero.


Other Data Capabilities
-----------------------

Each of the following actions can be found in the Data drop down
menu. 

 **Inspect:**

    Used to view a inhaled or parsed data set. Select Inspect
    from the drop down menu Data. In Key enter the key or .hex key 
    associated with the desired data. 

 **View All:** 

   Used to view all data sets that have been inhaled or
   parsed into H\ :sub:`2`\ O. To remove a dataset from H\ :sub:`2`\ O
   click on the red X next to the data set key.  
 
 **Summary:** 

   Used to display descriptive statistics and histograms of
   any columns within a specific data set. Specify data by the
   associated .hex key in the Key field, and select variables of
   interest from the resulting list of variables. Summary can be found
   under the **Model** drop down menu.




Data Manipulation
------------------

Users who wish to manipulate their data after it has been parsed into
H\ :sub:`2`\ O have a set of tools to do via  H\ :sub:`2`\ O + R. 

