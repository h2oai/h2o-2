# This is an example code for subsetting and filtering in H2O
# We'll split the dataset into "YES" entries and "NO" entries
# Then from that subset the dataset further to take a random sample of 500 "YES" and 500 "NO" each

# Detach and remove old H2O package if it exists
detach("package:h2o", unload=TRUE)
remove.packages("h2o",.libPaths())

# Install the same version of H2O from online repository that is running on your Hadoop Cluster 
install.packages("h2o", repos=(c("http://s3.amazonaws.com/h2o-release/h2o/master/1477/R", getOption("repos")))) 
#Load H2O library
library(h2o)

#Start and connect to an H2O instance
local.h2o = h2o.init(max_mem_size = '4g')

#Import small YES/NO data set into H2O
class_col = h2o.importFile(local.h2o,
                           "https://raw.githubusercontent.com/0xdata/h2o/63e04a6654ebcb2561852aca8f16b664a7b46400/smalldata/categoricals/simple_YES_NO.csv",
                           key="class_col")

#Check dimensions of data file
dim(class_col)
#Run summary, should have 9000 "YES" entries and 1000 "NO" entries
summary(class_col)

# Create a new h2o frame for row indices and cbind to the categorical column to avoid different chunk sizes
row_indices = h2o.exec(expr_to_execute = seq(1,nrow(class_col),1), h2o = class_col@h2o)
labels = class_col[,1]
label_col = cbind(labels, row_indices)

# Run the equivalent of which(label_col == "YES") and which(label_col == "NO")
idx0 = label_col[label_col[,1] == levels(labels)[1] ] [,2]
idx1 = label_col[label_col[,1] == levels(labels)[2] ] [,2]

# Produce a vector of random uniform numbers for each of the  class
random0 = h2o.runif(idx0)
random1 = h2o.runif(idx1)

# Subset the indices to take random 500 entries for each of the two classes
rows = 1000
select_idx0 = idx0[random0 <= ceiling((rows*0.5)/length(idx0))+0.01] [1:(rows*0.5)]
select_idx1 = idx1[random1 <= ceiling((rows*0.5)/length(idx1))+0.01] [1:(rows*0.5)]

# Subset the original H2OParsedData object by combining the indices of desired entries
all_idx = h2o.exec(c(select_idx0,select_idx1))
dataset = h2o.exec(class_col[all_idx,])
dataset
