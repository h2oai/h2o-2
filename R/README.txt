H2O in R
--------

These instructions assume you are using R 2.14.0 or later.  


To install the h2o R package from the web, do the following
(substitute branch name for <bbb> and build number for <nnn>):

install.packages("h2o", repos=(c("http://s3.amazonaws.com/h2o-release/h2o/<bbb>/<nnn>/R", getOption("repos"))))
library(h2o)
localH2O = h2o.init()
demo(h2o.glm)


To install the h2o R package after building it yourself, do:

install.packages("h2o", repos=(c("file:///path/to/your/sourcecode/h2o/target/R", getOption("repos"))))
library(h2o)
localH2O = h2o.init()
demo(h2o.glm)


Useful Notes
""""""""""""   

First time users may need to download and install Java
in order to run H2O. The program is available free on the web, 
and can be quickly installed. Even though you will use Java to 
run H2O, no programming is necessary. 

