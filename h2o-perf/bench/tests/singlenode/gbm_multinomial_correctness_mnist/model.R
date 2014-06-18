source("../../../R/h2oPerf/prologue.R")
runGBM(x = 2:785, y = 1, distribution = "multinomial", interaction.depth = 5, n.trees = 10, n.minobsinnode=10, shrinkage=0.02, n.bins=100)
sourae("../../../R/h2oPerf/epilogue.R"
