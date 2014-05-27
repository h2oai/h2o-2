
#loading h2o package
library(h2o)

#Connecting to remote h2o
 myIP = "192.168.1.162"
 myPort = 54321
 remoteH2O = h2o.init(ip = myIP, port = myPort, startH2O = TRUE)

#Parsing in the data file
 #air=h2o.importFile(remoteH2O,"hdfs://192.168.1.161/datasets/airlines_all.csv",key="air")
 # air=h2o.importFile.VA(remoteH2O,"/home/0xdiag/datasets/airlines/airlines_all.csv",key="air")
 air=h2o.importFile.FV(remoteH2O,"/home/0xdiag/datasets/airlines/airlines_all.csv",key="air")

 dim(air)
 colnames(air)
 head(air)
 summary(air)

#Finding busiest airport
 Dest.count = h2o.table(air$Dest)
 Dest.count
 dc=as.data.frame(Dest.count)
 dc
 dc[with(dc, order(count )), ]
 #dc$Dest[which(dc$count==max(dc$count))]
 
 
#Looking at arrival delay trends
 #Get quantiles and examine outliers
 air.qs = quantile(air$ArrDelay)
 air.qs
 
 # Note: Right now, assignment must be done manually with h2o.assign!
 outliers.indx = air$ArrDelay <= air.qs[2] | air$ArrDelay >= air.qs[10]
 temp = air[outliers.indx,]
 dim(temp)
 
 #check should return zero
 aa=as.data.frame(temp$ArrDelay)
 head(aa)
 aa[which(aa >air.qs[2] && aa<air.qs[10])]
 which(aa==max(aa))
 as.data.frame(temp[10774390,])
 
 arrdelay.outliers = h2o.assign(temp, "arrdelay.outliers")
 nrow(arrdelay.outliers)
 head(arrdelay.outliers)
 
#Droping outliers from data
 temp = air[!outliers.indx,]
 airlines.trim = h2o.assign(temp, "airlines.trim")
 nrow(airlines.trim)
 #check
 nrow(air)-nrow(airlines.trim) 
 nrow(arrdelay.outliers)
 
#Sampling from R
 ss=sample(nrow(air),20)
 ss
 s_frm=as.data.frame(air[ss,])
 #check
  s_frm[1,]
 head(air[ss[1],])
 
#Constructing test and train sets by sampling
 #creating a column as tall as airlines(nrow(air))
 s = h2o.runif(air)    # Useful when number of rows too large for R to handle
 #selecting 80% of rows at random
 temp = air[s <= 0.8,]
 
 airlines.train = h2o.assign(temp, "airlines.train")
 #check
 dim(airlines.train)
 nrow(air)*80/100
 
 temp = air[s > 0.8,]
 airlines.test = h2o.assign(temp, "airlines.test")
 dim(airlines.test)
 nrow(airlines.train) + nrow(airlines.test)
 nrow(air)

#Which months are not good for travel and when does maximum delays occur for yr 1988
#yr_seq=1988:2008
 y8=air[air[,1] == 1988,]
 head(y8)
 hh=matrix(NA,ncol=12,nrow=1)
 for (i in 1:12) {
    Y1988_1_A=y8[y8[,2]== i,15]
    s8=as.data.frame(Y1988_1_A)
    hh[i]=sum(s8[!is.na(s8)])
    #   s8=sum(as.data.frame(y8[y8[,2]== i,15]))
    print(hh[i])
 }
 plot(hh[1:12],type="l")


 #Running GLM
 myX = c("Origin", "Dest", "Distance", "UniqueCarrier", "Month", "DayofMonth", "DayOfWeek", "CRSElapsedTime")
 myY="IsDepDelayed"
 air.glm = h2o.glm.FV(x = myX, y = "IsDepDelayed", data = air, family = "binomial", nfolds = 1, alpha = 0.25,lambda=0.001)
 air.glm
 sort(air.glm@model$coefficients)
 air.glm@model$confusion
 
 #RF
 airVA=h2o.importFile.VA(remoteH2O,"/home/0xdiag/datasets/airlines/airlines_all.csv",key="airVA")
 rr=h2o.randomForest.VA(x=myX,y=myY,data=airVA)
#rr=h2o.randomForest(x=myX,y=myY,data=air)

#Factor Stuff
is.factor(air$Year)
air$Year=as.factor(air$Year)
#check
is.factor(air$Year)


