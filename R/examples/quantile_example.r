# This is an example code to run Quantile function on a Hadoop Cluster
# Prior to running the R Script, launch the H2O hadoop jar on the cluster

# Detach and remove old H2O package if it exists
detach("package:h2o", unload=TRUE)
remove.packages("h2o",.libPaths())

# Install the same version of H2O from online repository that is running on your Hadoop Cluster 
install.packages("h2o", repos=(c("http://h2o-release.s3.amazonaws.com/h2o/rel-kahan/2/R", getOption("repos")))) 

#Load H2O library
library(h2o)

#Connect to H2O instance by specifying ip and port
myIP = "192.168.1.153"   
myPort =  54321
remote.h2o = h2o.init( ip = myIP, port = myPort )

#Import the data file from HDFS and parse it into H2O memory
qfile = h2o.importHDFS.FV(remote.h2o,"hdfs://abc.loc:2312/datasets/quant_file.csv",key="qfile")
#If the data file sits on your local machine, upload and parse it into H2O memory
#qfile=h2o.uploadFile.FV(remote.h2o,"/Users/nidhimehta/Desktop/quant_file.csv",key="qfile")

#Check dimensions of data file
dim(qfile)

#Run summary
summary(qfile)

#Print column names 
cname = colnames(qfile)
print(cname)

# Get a single quantile for a column
quantile(qfile$M1, probs = 0.60, na.rm = T)

#Get qunatiles for specified multiple probabilites for a column
quantile(qfile$M11,probs=seq(0,1,.1))
quantile(qfile$M1,probs=seq(0,1,.01))

#Get quartiles of a column
quantile(qfile$M10,probs=seq(0,1,.25), na.rm = T)

#Print 99.9 quantile for each column in the data file
for(i in 16:length(cname)){
  print(paste(cname[i],quantile(qfile[,i],prob = .999, na.rm = T)))
}

