.. _InspectReturn:

Data: Inspect (Return)
========================

To view the Data Inspect page, go to the drop-down **Data**
menu, select **Inspect**, and specify the key associated with the
data. 

The Data Indices or Header (if included when data were imported and
parsed) display at the top of the page. 

**Jump to Row**
  To automatically move to a specified point in the data, 
  enter an row index number and click the **Jump to Row**
  button. 

**Info and Scrolling Buttons**
 To display the front page and summary information, click **info**. Select any of the numbers displayed to jump to that page in the data.

**Change Type** 
  To specify how the data in a particular column
  should be treated (as numeric or as factors), click the buttons below the headers. If the data type that was automatically selected during data parsing is not the correct specification, change data type by clicking the button **As Factor** for
  integer columns, or reverse this change by clicking **As Integer**. 
  Alphanumeric or alphabetical data are automatically classified as factor
  data and cannot be treated as numeric, and continuous real data
  cannot be converted to factor data because the number of possible
  factor levels is infinite. 

**Type** 
  Specifies the type of data contained in a column. 
  
   - *Int*: Integer; discrete numeric data (1, 2, 3, 4, ...)
   - *Real*: Continuous real data (1, 1.01, 1.1, 1.3, 1.55, ...)
   - *Enum*: Enumerative, Factor or Categorical data. (Red, Green, Blue,...)

**Min**
  Specifies the minimum value in a column of numeric data. 

**Max**
  Specifies the maximum value in a column of numeric data. 

**Mean** 
  Specifies the average value of the data in a numeric column. 

**Cardinality** 
  Specifies the number of unique levels in a column of factor data. 

**Missing**
  Specifies the number of missing values in a column of data. 


.. image:: inspect.png
   :width: 100%









