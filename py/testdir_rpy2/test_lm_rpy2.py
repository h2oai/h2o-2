
from scipy import random
from numpy import hstack, array, matrix
from rpy2 import robjects 
from rpy2.robjects.packages import importr

base = importr('base')
stats = importr('stats') # import only once !

def test_regress(x):

    stats=importr('stats')

    x=random.uniform(0,1,100).reshape([100,1])
    y=1+x+random.uniform(0,1,100).reshape([100,1])

    x_in_r=create_r_matrix(x, x.shape[1])
    y_in_r=create_r_matrix(y, y.shape[1])

    formula=robjects.Formula('y~x')

    env = formula.environment
    env['x']=x_in_r
    env['y']=y_in_r

    fit=stats.lm(formula)

    coeffs = stats.coef(fit)
    resids = stats.residuals(fit)    
    fitted_vals = stats.fitted(fit)
    modsum = base.summary(fit)
    rsquared = modsum.rx2('r.squared')
    se = modsum.rx2('coefficients')[2:4]

    print "coeffs:", coeffs
    print "resids:", resids
    print "fitted_vals:", fitted_vals
    print "rsquared:", rsquared
    print "se:", se

    return (coeffs, resids, fitted_vals, rsquared, se) 


def create_r_matrix(py_array, ncols):
    if type(py_array)==type(matrix([1])) or type(py_array)==type(array([1])):
        py_array=py_array.tolist()
    r_vector=robjects.FloatVector(flatten_list(py_array))
    r_matrix=robjects.r['matrix'](r_vector, ncol=ncols)
    return r_matrix

def flatten_list(source):
    return([item for sublist in source for item in sublist])

x = []
test_regress(x)

