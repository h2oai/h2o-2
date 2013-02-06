
import numpy as np
from rpy2 import robjects as ro

# to get an RPY2 DataFrame from a csv file, in RPY2.3, you can just do:
# http://rpy.sourceforge.net/rpy2/doc-2.3/html/vector.html#rpy2.robjects.vectors.DataFrame.from_csvfile

# good glm info at http://data.princeton.edu/R/glms.html
df = ro.DataFrame.from_csvfile('1.csv', col_names=['a','b','c','d', 'y'])
formula = 'y ~ a+b+c+d' 
fit = ro.r.glm(formula=ro.r(formula), data=df, family=ro.r('binomial(link="logit")'))

# print ro.r.summary(fit)
coef = ro.r.coef(fit)
print "intercept     :", coef[0]
print "coefficient 1 :", coef[1]
print "coefficient 2 :", coef[2]
print "coefficient 3 :", coef[3]
print "coefficient 4 :", coef[4]

print "\ncoef:", ro.r.coef(fit)
print "residuals", ro.r.residuals(fit)
print "deviance", ro.r.deviance(fit)
print "predict", ro.r.predict(fit)
print "fitted", ro.r.fitted(fit)
print df

