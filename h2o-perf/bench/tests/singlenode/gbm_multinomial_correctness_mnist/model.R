source("../../../R/h2oPerf/prologue.R")
runGBM(x = 1:784, y = 785, distribution = "multinomial", interaction.depth = 5, n.trees = 10, n.minobsinnode=10, shrinkage=0.02, n.bins=100)
sourae("../../../R/h2oPerf/epilogue.R"
