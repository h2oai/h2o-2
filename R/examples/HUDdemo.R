library(h2o)
h2o.server<- h2o.init()

hud<- h2o.uploadFile(h2o.server, "h2o/smalldata/hud.clean.csv")

#Poke around at the data, take a look at the variables
head(hud)
str(hud)
summary(hud)
nrow(hud)
ncol(hud)


# In the original data there were two interesting columns for dependent variables - 
#ZSMHC: the total cost of living in the home including rent, utilities, insurance etc...
# and RENT, the amount of rent that the household paid. We want to pull the -6 entries from 
# those columns (since -6 is the HUD code for NA). Because we may want to consider the cost of 
# housing relative to income, we'll clean up the colum for income as well.


hud.new<- hud[(hud[,2]> -6 & hud[,6] > -6 & hud$ZINC > 0),]
str(hud.new)
summary(hud.new)
nrow(hud.new)

#Examining the distribution of the likely dependent variables:
quantile(hud.new$RENT)
summary(hud.new$RENT)
quantile(hud.new$ZSMHC)

summary(hud.new$ZSMHC)

#Both DVs look like they have a strong skew to the right tail - driving the mean well above the median value. (EG - if you look at the summary for $RENT- the 75th percentile is $1085, while the max is 5892. The rest of the data are clearly centered around a much lower value - making $5K a month for rent look like an outlier.)
# If you look at a table for RENT - you can see that the data are pretty clumpy around round numbers (i.e., people are more likely to pay $700 for # rent 
# than $678, or $723), and then there is a weird spike at the highest value of $5892 - about $3000 more than the second highest value. They might be #legitimate observations, but rents this high are well away from the rest of the distribution distribution, so we'll separate these highest values out for now, and consider the #rents that fall in the normal range. 
as.data.frame(table(hud.new$RENT))
hud.short<- hud.new[(hud.new$RENT< 3000),] #pull the extreme values of rent - 
hud.high<- hud.new[(hud.new$RENT > 3000),] #pull the extreme values of rent  into their own data frame to look at later-
summary(hud.short)

#Running a quick Kmeans model allows us to further characterize: (for instance, note in the cluster generated below that the rents in the upper middle group # also have much lower incidence of income from social safety nets, and lower incidence of rodents. At the highest rents level the incidents of all of these # increase again, suggesting that higher rents are not necessarily an indicator of higher quality housing) 
hud.epx<- as.data.frame(hud.short)
hud.epx<- as.h2o(h2o=h2o.server, hud.epx)
hud.kmeans<- h2o.kmeans(data=hud.epx, centers = 4, cols=c("RATS", "MICE", "MOLD", "POOR", "QSS", "QSSI", "QWKCMP", "QWELF", "RENT"), normalize=F)
hud.kmeans


#Even though we pared the data down from ~900 original columns to ~70 columns, we expect that some of the columns are highly
# collinear (for example, houses that report income from social safety nets are also more likely to report incomes near or below 
# the poverty line, or houses that have a washer are much more likely to also have a dryer, making one condition a reasonable predictor of the other). 

hud.PCAregress<- h2o.pcr(x=c("RATS", "MICE", "MOLD", "POOR", "QSS", "QSSI", "QWKCMP", "QWELF", "ZINC", "EVROD", "EROACH", "TOILET", "TUB", "REFR", "TRASH", "DISPL", "STOVE", "AIRSYS", "ELECT", "LIVING", "KITCH", "HALFB", "FAMRM", "DINING", "DENS", "BEDRMS", "BATHS", "COOK", "OVEN", "DRY", "WASH", "DISH", "PLUMB", "KITCHEN", "PHONE", "ROOMS", "APPLY", "VCHRMOV", "VCHER", "QRETIR", "QSELF", "ZINC", "ZADULT", "PER"), y="RENT", data=hud.short, ncomp=10, family="gaussian")
hud.PCAregress

#We can also just run a standard regression on the data set, and use regularization to tune. 
#Split data into test and train sets: 
hud.short[,71]<- h2o.runif(hud.short)
summary(hud.short[,71])
hud.short.train<- hud.short[(hud.short[,71]<= .80),]
hud.short.test<- hud.short[(hud.short[,71]> .80),]
nrow(hud.short.train)
nrow(hud.short.test)
summary(hud.short.test)

preds = c("REGMOR", "DIVISION", "REGION", "METRO", "STATE", "LMED", "LMEDA", "LMEDB", "FMR", "FMRA", "FMRB", "L30", "L50", "L80", "IPOV", "PER", "ZADULT", "ZINC", "ZINC2", "QSELF", "QSS", "QSSI", "QWELF", "QRETIR", "QWKCMP", "POOR", "VCHER", "VCHRMOV", "RENEW", "APPLY", "ROOMS", "PHONE", "KITCHEN", "PLUMB", "DISH", "WASH", "DRY", "OVEN", "COOK", "NUNIT2", "BATHS", "BEDRMS", "DENS", "DINING", "FAMRM", "HALFB", "KITCH", "LIVING" ,"OTHFN", "ELECT", "AIRSYS", "STOVE", "PORTH", "DISPL", "TRASH", "REFR", "TOILET", "TUB", "RATS", "MICE", "MOLD", "EROACH", "EVROD")
L = c(seq(from= 0, to = 1, by= .01))
hud.reg<- h2o.glm.FV(x=preds, y="ZSMHC", family="gaussian", standardize=T, alpha=c(0, 0.001, .01, .5), lambda = L, nfolds=0, data=hud.short.train)
hud.reg
hud.best<- h2o.glm.FV(x=preds, y="ZSMHC", family="gaussian", standardize=T, alpha=0, lambda =0, nfolds=0, data=hud.short.train)
hud.test<- h2o.predict(hud.best, hud.short.test)
summary(hud.test)



