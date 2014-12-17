

Data: Summary (Request from Menu)
===================================

The Summary page displays a detailed column-by-column summary of the parsed
data.  

**Source**
  
  The .hex key associated with the summarized data. 
  
**Cols** 
  
  To specify a subset of columns for summarization, select them from the list. By default, the summary displays all columns. 

**Max Ncols**

  Specify the maximum number of columns to summarize. 

**Max Qbins**

  The number of bins for quantiles. When large data are parsed, they
  are also binned and distributed across a cluster. When data are
  multimodal (or otherwise uniquely shaped), increasing the number
  of bins allocates fewer data points to each bin and 
  increases the accuracy of the displayed quantiles. Increasing the
  number of bins for extremely large data can result in slower speeds, depending
  on the amount of memory allocated to computational tasks.   
