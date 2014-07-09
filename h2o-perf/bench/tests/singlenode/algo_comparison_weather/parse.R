source("../../../R/h2oPerf/prologue.R")

data_source <<- "https"

trainData    <<- "https://rattle.googlecode.com/svn-history/r402/trunk/package/rattle/inst/csv/weather.csv"
response <<- "RainTomorrow"

num_train_rows  <<- 355
num_explan_cols <<- 20

import("parsed.hex", trainData)

source("../../../R/h2oPerf/epilogue.R")
