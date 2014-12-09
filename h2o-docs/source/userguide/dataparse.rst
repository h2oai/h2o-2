.. _DataParse:

Data: Parse
===============

Once data are ingested, they are available in H2O, but are
not yet in a format that H2O can process. To convert the data to
an H2O usable format, use parsing.

Parser Behavior
----------------

The data type in each column must be consistent. For example, if
data are alpha-coded categorical, all entries must be alphabetical or
alphanumeric. If the parser detects numeric entries, the
column is not processed. It registers all entries as
NA. This is also true when NA entries are included in columns
consisting of numeric data. Columns of alpha-coded categorical
variables containing NA entries are interpreted as NA at a distinct
factor level. If missing data is coded as periods or dots in the
original data set, those entries are converted to zero.

> **Note:** In general, the default options can be used.

**Parser Type**
Specify whether data are formatted as CSV, XLS, or SVMlight from this drop-down menu. In general, use the default setting - the parser recognizes most data formats, with some exceptions.

**Separator**
Select a separator type from the drop-down list of common separators. In general, use the default setting.

**Header**
If the first line of the file being parsed is a header (includes column names or indices), check this checkbox.

**Header with Hash**
To use a column header that begins with a hash symbol (#), check this checkbox. If you do not check this checkbox, any hash symbols are treated as comment indicators and are not parsed.

**Single Quotes**
To use single quotation marks (also known as apostrophes, ' ') as a field quotation character, check this checkbox. If you do not check this checkbox, any single quotation marks are removed during parsing.

**Header From File**
Specify a file key if the header for the data to be parsed is found
in another file that has already been imported to H2O.

**Exclude**
To omit columns from parsing, enter a comma-separated list of columns.

**Source Key**
(Auto-generated) Displays the file key associated with the imported data to be parsed.

**Destination Key**
Enter an optional user-specified name for the parsed data to use as a reference later in modeling. If left blank (the default option) a destination key is automatically assigned as *original file name*.hex.

**Preview**
Displays an auto-generated preview of parsed data.

**Delete on done**
To delete the imported data after parsing, check this checkbox. In general, this option is recommended, as retaining data will take
memory resources, but not aid in modeling because unparsed data
can't be acted on by H2O.
