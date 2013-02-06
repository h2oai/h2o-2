
import rpy2.robjects as R
import rpy2.robjects.numpy2ri as rpyn
from rpy2.robjects.packages import importr


r = R.r
g = R.globalenv

print "pi"
print type(r)
pi = r['pi']
print pi[0]
print type(pi)

print "piplus"
piplus = r('piplus = pi + 1')
print type (piplus)
print piplus
print piplus[0]


# some simple R things
print R.r.median(R.IntVector([1,2,3,4]))[0]

# create two R vectors and do correlation coefficient in R
a = R.IntVector([1,2,3,4])
b = R.IntVector([1,2,3,4])

# need the subscript to get authentic python type?
print R.r.cor(a,b,method="pearson")[0]

my_vec = R.IntVector([1,2,3,4])
my_chr_vec = R.StrVector(['aaa','bbb'])
my_float_vec = R.FloatVector([0.001,0.0002,0.003,0.4])

print "\nconvert to numpy array?"
vector = rpyn.ri2numpy(my_float_vec)
print vector

python_list = list(my_float_vec)
print python_list

bigger_vec = R.IntVector(a+b) # using multiple lists
print list(bigger_vec)


# linear regression
observed = R.FloatVector([1.1, 1.2, 1.3]) # native python list will not do!!
theoretical = R.FloatVector([1.15, 1.25, 1.35])
R.globalEnv['observed'] = observed
R.globalEnv['theoretical'] = theoretical
m = R.r.lm('theoretical ~ observed')
R.abline(lm,col='color') # add regression line to EXISTING plot
m['coefficients'][0][0] # intercept
m['coefficients'][0][1] # slope


#Histogram: mid-points: 
# print h.r['mids'][0], 
# counts: 
# print h.r['counts'][0]
# Standard deviation: 
print R.r.sd(R.IntVector([1,2,3,4]))[0]


# matrix
my_mat = R.r.matrix(my_vec,nrow=2)

a = [1,2,3]
b = [4,5,6]
m = R.r.matrix(a+b,nrow=2,dimnames=[ ['r1','r2'], ['c1','c2','c3'] ])
print R.r.rownames(m)
print R.r.colnames(m)


# chisq.test
result = R.r['chisq.test'](my_mat)
print R.r['chisq.test'](my_mat)
print result
# print result.r
print result.r['p.value'][0][0]

# binomial test
result = R.r['binom.test'](my_float_vec,p=bg_prob,alternative='g')
print result.r['p.value'][0][0]

# t test
a = [1,2,3,4]
b = [2,3,4,5]
result = R.r['t.test'](R.IntVector(a),R.IntVector(b),alternative='two.sided',paired=True)
print result.r['p.value'][0][0]
print result.r['estimate'][0][0]
# for paired test, if estimate > 0, then a is greater than b, else, b is greater than a (mean)


# FDR correction
# the pvalue_lst has to be FloatVector
# first we might have a list of test result objects, make pvalue list first
pvalue_lst = [v.r['p.value'][0][0] for v in testResult_lst]
p_adjust = R.r['p.adjust'](R.FloatVector(pvalue_lst),method='BY')
for v in p_adjust:
    print v


