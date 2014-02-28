source("../../R/h2oPerf/prologue.R")
runGBM(x = 1:54, y = 55, 
       distribution='multinomial', interaction.depth = 10, n.trees = 100,
       n.minobsinnode=10, shrinkage=0.02,n.bins = 100)
source("../../R/h2oPerf/epilogue.R")
