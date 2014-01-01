# This example is formatted to run with the test directory utils files as if it were a test so that the reader can run directly from terminal (or whatever corrolary). 

setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source('../findNSourceUtils.R')
example.km2toy.golden <- function(H2Oserver)

#Import the toy km data set- a data set with four points sufficiently unbalanced that we have good expected solutions. 
toykm.csv
toyR<- read.csv(locate("smalldata/toykm.csv"), header=T)

#Request model k=1; which gives you the total variance for the whole data set. Within cluster SS by cluster is 189. We got that by calculating the mean of each feature (the columns V1, and V2). The mean is (-1.5, 0). We then calculated the deviance for each of the four points. For point 1 that is: 
#(2-(-1.5))^2+(4-0)^2
kmeans(toyR, centers=1)

#EX: calculate dev by hand
EV1<- (toyR[,1]-(mean(toyR[,1])))^2
EV2<- (toyR[,2]-(mean(toyR[,2])))^2
sV1<- sum(EV1)
sV2<- sum(EV2)
tot<- sum(sV1, sV2)
tot

#Compute model for k=2. Expect clustering vector of fml form A,A,B,B (pt1 and pt2 should be clustered together, and point 3 and point 4 should be clustered together)
rfit<- kmeans(toyR, centers=2)
rfit$withinss
rfit$centers


#Calculate within SS by hand: because this is the toy example, I've allowed reader to verify from printout of line 19 that centers are (-6, -5) and (3, 5), arbitrarily ordered. They will forever and always be at these values for this data set because the center of the cluster is defined to be the mean of the cluster. Because we know the memebers of the cluster we know the mean. The value for the center IS NOT RANDOM once cluster membership is determined. It can be calculated exactly from knowing cluster membership alone, and it is forever and always the same value given the same member points. 
ClusterAV1<- (toyR[1:2,1]-3)^2
ClusterAV2<- (toyR[1:2,2]-5)^2
totA<- sum(sum(ClusterAV1), sum(ClusterAV1))
totA

ClusterBV1<- (toyR[3:4,1]-(-6))^2
ClusterBV2<- (toyR[1:2,2]-(-5))^2
totB<- sum(sum(ClusterAV1), sum(ClusterAV1))
totB

#total sum of squqres = total variance in the whole data set, which is the same as k=1. We KNOW this to be 189 from lines 9-15
rfit$totss

#within sum of squres is comuted as given above, and should be a vector equal to the number of clusters
rfit$totss

#total within sum of squares is the sum of the sums of squres within each cluster. In this case it's SS Cluster A + SS cluster B = 4 + 4 = 8
rfit$tot.withinss

#the between ss is the the total sum of squres for the whole data set less the total within ss (line 34 minus line 40). It's important because we use it to assess the "fit" of the kmeans model, which you will see in line 45. 
rfit$betweenss

#to assess the fit of the kmeans model you want to know how much variance is explained by the cluster assignments. In this case it's 98.5% - which you see when you print out the model. That number comes from 
howgood=rfit$betweenss/rfit$totss
howgood*100

#Extend this to H2O Kmeans(fluid vecs), you can see total ss by setting k=1 a functionality not working at the time of this writing (Dec 2013)
h2o.toy<- h2o.importFile(h2o.conn, "../smalldata/toykm.csv")
h2o.toy<- h2o.uploadFile(H2Oserver, locate("../../smalldata/toykm.csv"), key="h2o.toy")
summary(h2o.toy)
h2o.fit<- h2o.kmeans(h2o.toy, centers=2, cols=c("C0", "C1"), iter.max = 100)
h2o.fit


testEnd()
}

doTest("K Means example", example.km2toy.golden)

#submitted by AILang, dec 2013 as a quick overview of how the basics of kmeans work in a mathy sense 