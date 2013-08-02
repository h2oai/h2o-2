source("H2O.R")
localH2O = new("H2OClient", ip="localhost", port=54321)

p.url = "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv"
p.hex1 = h2o.importURL(localH2O, p.url)
p.hex2 = h2o.importURL(localH2O, p.url, key="prostate.url.hex")
p.hex3 = h2o.importURL(localH2O, p.url, key="prostate_header.url.hex", header=TRUE)
p.hex4 = h2o.importURL(localH2O, p.url, header=TRUE)

p.path = "../smalldata/logreg/prostate.csv"
h2o.importFile(localH2O, p.path)
h2o.importFile(localH2O, p.path, key="prostate.loc.hex", header=FALSE)
h2o.importFile(localH2O, p.path, key="prostate_header.loc.hex", header=TRUE)
h2o.importFile(localH2O, p.path, header=TRUE)