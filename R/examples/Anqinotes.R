#LOAD IRIS (FROM WHEREVER IT ALREADY IS IN YOUR DEMO)
iris<- read.csv("~/Work/h2o/smalldata/iris/iris.csv", header=F)
summary(iris)

#manipulate data types - list things as number - I use this a lot. 
iris$V1F<- as.factor(iris$V1)

#more manipulation of data types- turn cats into ordered factor levels - I use this a lot, but it can also be totally done by recoding, which I think you already have. 
v<- c("A", "B", "B", "C", "A", "B", "C", "C", "A", "A")
as.ordered(v)

#produce boxplots - nice to have for vis, but NOT a top of the list thing. 
boxplot(iris$V1, iris$V2, iris$V3)

#calculate correlations - I like this. earl puts less stock in it because glm is more robust, but I think that it's important at least until we get PCA+GLM implemented. 
cor(iris$V1, iris$V2)


#generate the residuals from a glm model
#glm.obj = h2o.glm.obj, and I think this might need to have actual predictions hooked up - which currently works for everything expect binomial - THIS ISN'T DATA MUNGING- I know. but I like to be able to see plots of the residuals. It's something I do every time when I run GLM just to check the shape of the model. 
residuals(glm.obj, type="deviance")

#scale and center a matrix- maybe redundant with normalize, but does it before modeling (so that we can see the data before modeling rather than waiting for algo to center and not seeing it). - I don't know if this is the we need it for sure, because we do it in model, but if we want to do it before modeling then this might be useful. The other thing is that I might not want to center and scale, but I might want to just center because it changes the interpretation of some models (I mean center a lot to improve interpretability but don't always scale by sd when I want to preserve original scaled relationships)
iris.subset<- cbind(iris$V2, iris$V3, iris$V4)
iris.sub.scaled<- scale(iris.subset)
summary(iris.sub.scaled)

#add noise to a column vec (sometimes used in generated or bootstrapped data to add random variance) 
iris.noise<- jitter(iris$V1, factor = 1, amount=.001)
cov(iris.noise, iris$V1)

#transform data
summary(birth.df)
newbirth<- transform(birth.df$grams, 10*birth.df$grams)
meancent.grams<- transform(birth.df$grams, birth.df$grams-
+ mean(birth.df$grams))


#nice to haves
list all objects
ls()
mapply()