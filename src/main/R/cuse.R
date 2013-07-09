cuse = read.table("../smalldata/logreg/princeton/cuse.dat", header=TRUE)
attach(cuse)
lrfit <- glm( cbind(using, notUsing) ~  age + education + wantsMore , family = binomial)
lrfit
summary(lrfit)
