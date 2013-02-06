import rpy2.robjects as robjects
from rpy2.robjects.packages import importr

r = robjects.r
g = robjects.globalenv
print type(r)

# from the R base package
# any other name comes from .globalEnv
pi = r['pi']
print pi[0]
print type(pi)


# r is callable with code to be evaluated
piplus = r('piplus = pi + 1')
print type(piplus)
print piplus
print piplus[0]


# explicitly get from .globalEnv
piplus_from_g = g['piplus']
print piplus_from_g
print piplus == piplus_from_g
print piplus[0] == piplus_from_g[0]


# define an R function and call it
r('''
f <- function(start=1,stop = 5) {
n = 0
for (j in start:stop) { n = n + j }
print (n)
}
''')

# it's available in .globalEnv
f = g['f']
f()


# but also from the running R process
f = r['f']



# interpolating an R object into code
letters = robjects.r['letters']
s = letters[1:6].r_repr()
rcode = 'paste(%s, collapse="-")' %(s)
res = robjects.r(rcode)
print(res)



# more on calling R functions
rsum = r['sum']
print rsum(robjects.IntVector([1,2,3]))[0]


# with keyword
rsort = r['sort']
iv = robjects.IntVector([3,1,2])
res = rsort(iv, decreasing=True)
print res.r_repr()



# plotting
gd = importr('grDevices')
ofn = '/home/kevin/plot.pdf'
gd.pdf(ofn)

x = robjects.IntVector(range(10))
y = r.rnorm(10)
r.layout(r.matrix(robjects.IntVector([1,2,3,2]), nrow=2, ncol=2))
r.plot(r.runif(10), y, xlab="runif", ylab="foo/bar", col="red")
gd.dev_off()



print "R special operators"
r('''
is_in <- function(value, container) {
value %in% container
}
''')

is_in = g['is_in']
L = robjects.StrVector(list('abcde'))
print is_in('a',L)
print is_in('f',L)

r('''m_mult <- function(m1,m0) { m1 %*% m0 }''')
m_mult = g['m_mult']
r('''mtoi <- function(m) { as.integer(m) }''')
mtoi = g['mtoi']

# an old friend
m0 = r.matrix(robjects.IntVector([0,1,1,1]), nrow=2)
m = m0
for i in range(10):
    print i + 3,
    m = m_mult(m,m0)
    print mtoi(m)

