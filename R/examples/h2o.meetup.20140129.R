
#loading h2o package
library(h2o)

print(sprintf('h2o version: %s\n', packageVersion('h2o')))

#Connecting to h2o
myIP = '127.0.0.1'
myPort = 54321
loc <- h2o.init(ip=myIP, port=myPort, startH2O=T, Xmx='4g')

#Parsing in the data file
#air <- h2o.importFile(loc, 'hdfs://192.168.1.161/datasets/airlines_all.csv')
air <- h2o.importFile(loc, '/Users/earl/shared/20131226_data/airlines_all.005p.csv.gz')

# a quick peek at the data
str(air)
head(air)

# overall distribution of the data
summary(air)

# even more data poking
s <- sample(nrow(air), 30)
ss <- air[s,]
as.data.frame(ss)

as.data.frame(table(air$Year))
as.data.frame(table(air$Month))


# which airport are most and least busy?
cnts <- as.data.frame( table(air$Origin))
cnts <- cnts[ order(cnts$Count, decreasing=T), ]
rbind( head(cnts, 10), tail(cnts, 10) )

# reexamine the mapping: ArrDelay -> IsArrDelayed
quantile(air$ArrDelay, na.rm=T, c(0.01, 0.2, 0.5, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95))

# so maybe you don't really care if the arrival is delayed by 16 minutes; that removes a big chunk of class 1
air[, ncol(air)+1] <- air$ArrDelay < 15 | is.na(air$ArrDelay)

as.data.frame( table(air$ArrDelay > 0) )
as.data.frame( table(air$C31 > 0) )

# build the 2 models and compare:
X = c("Origin", "Dest", "Distance", "UniqueCarrier", "Month", "DayofMonth", "DayOfWeek", "CRSElapsedTime",
	'CRSDepTime', 'CRSArrTime', 'FlightNum', 'TailNum')
model0 <- h2o.glm.FV(y='IsArrDelayed', x=X, family='binomial', data=air)
model0@model$auc

# extract some interesting coefficients
coef0 <- model0@model$coefficients
coef0 <- coef0[ order(abs(coef0), decreasing=T)]
sort(coef0[1:20], decreasing=T)


# and compare to a model with a tweaked definition of late:
model1 <- h2o.glm.FV(y='C31', x=X, family='binomial', data=air)
model1@model$auc

coef1 <- model1@model$coefficients
coef1 <- coef1[ order(abs(coef1), decreasing=T)]
sort(coef1[1:20], decreasing=T)

# so the model seems somewhat sensitive to the exact definition of "delayed", while we probably aren't



# are flights ever early?
as.data.frame(table(air$ArrDelay < -15))



range(air$ArrDelay)
as.data.frame(table(h2o.cut(air$ArrDelay, breaks=c(-1000, -60, -15, 15, 60, 120, 180, 1500))))
summary(air)

# nested ifelse is being worked on: cut into 4 classes
air[, ncol(air)+1] <- ifelse(!is.na(air$ArrDelay) & air$ArrDelay < -60, 1, 0)
air[, ncol(air)+1] <- ifelse(!is.na(air$ArrDelay) & air$ArrDelay >=-60 & air$ArrDelay < 15, 2, 0)
air[, ncol(air)+1] <- ifelse(!is.na(air$ArrDelay) & air$ArrDelay > 15 & air$ArrDelay < 60, 3, 0)
air[, ncol(air)+1] <- ifelse(!is.na(air$ArrDelay) & air$ArrDelay >= 60, 4, 0)
#air[, ncol(air)+1] <- ifelse(is.na(air$ArrDelay), 5, 0)

basecol <- 33
air[, ncol(air+1)] <- air[,basecol] + air[,basecol + 1] + air[,basecol+2] + air[,basecol+3]

model.gbm <- h2o.gbm(y='C35', x=X, distribution='multinomial', n.trees=20, data=air)


# done
if(F){
	h2o.shutdown(loc, prompt=F)
}



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


arrdelay.outliers = h2o.assign(temp, "arrdelay.outliers")
nrow(arrdelay.outliers)
head(arrdelay.outliers)

#Dropping outliers from data
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


