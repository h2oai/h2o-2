

Data: Summary (Request from Menu)
===================================

Summary returns a column by column detailed summary of parsed
data. For more information on the returned information see Summary. 

**Source**
  
  The .hex key associated with the data to be summarized. 
  
**Cols** 
  
  If a subset of columns is desired, specify that subset
  here. Default is to return a summary for all columns. 

**Max Ncols**

  The maximum number of columns to be summarized. 

**Max Qbins**

  The number of bins for quantiles. When large data are parsed, they
  are also binned and distributed across a cluster. When data are
  multimodal (or otherwise distinctly shaped), increasing the number
  of bins will allocate fewer data points to each bin and thus
  increase the accuracy of the quantiles returned. Increasing the
  number of bins for extremely large data can slow results depending
  on the memory allocated to computational tasks.   
