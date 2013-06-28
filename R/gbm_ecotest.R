library(gbm)
source("brt.functions.R")

ecology.data <- read.csv("../smalldata/gbm_test/ecology_model.csv")
angaus.tc5.lr01 <- gbm.step(data=ecology.data, gbm.x = 3:14, gbm.y = 2, family = "bernoulli", tree.complexity = 5, learning.rate = 0.01, bag.fraction = 0.5)
angaus.tc5.lr005 <- gbm.step(data=ecology.data, gbm.x = 3:14, gbm.y = 2, family = "bernoulli", tree.complexity = 5, learning.rate = 0.005, bag.fraction = 0.5)
gbm.plot(angaus.tc5.lr005, n.plots = 12, write.title = F)

ecology.int <- gbm.interactions(angaus.tc5.lr005)
print(ecology.int$rank.list)
print(ecology.int$interactions)

ecology_eval.data <- read.csv("../smalldata/gbm_test/ecology_eval.csv", as.is = T)
ecology_eval.data$Method <- factor(ecology_eval.data$Method, levels = levels(ecology.data$Method))
ecology.preds <- predict.gbm(angaus.tc5.lr005, ecology_eval.data, n.trees = angaus.tc5.lr005$gbm.call$best.trees, type = "response")
# gbm.predict.grids(angaus.tc5.lr005, ecology_eval.data, want.grids = F, sp.name = "preds")
dev <- calc.deviance(ecology_eval.data$Angaus_obs, ecology.preds, calc.mean = T)
print(paste("Deviance is ",dev,sep=""))