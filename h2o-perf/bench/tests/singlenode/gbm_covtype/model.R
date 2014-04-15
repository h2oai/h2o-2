source("../../../R/h2oPerf/prologue.R")
runGBM(x = 1:54, y = 55, distribution = "multinomial",
interaction.depth = 5, n.trees = 10, n.minobsinnode=10, shrinkage=0.02, n.bins=100)
source("../../../R/h2oPerf/epilogue.R")
