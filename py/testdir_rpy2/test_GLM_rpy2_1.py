
import numpy as np
from rpy2 import robjects as ro
import rpy2.rlike.container as rlc

def train(num_cross_validation_foldss, y_values, weights):
        x_float_vector = [ro.FloatVector(x) for x in np.array(num_cross_validation_foldss).transpose()]
        y_float_vector = ro.FloatVector(y_values)   
        weights_float_vector = ro.FloatVector(weights)
        names = ['v' + str(i) for i in xrange(len(x_float_vector))]
        d = rlc.TaggedList(x_float_vector + [y_float_vector], names + ['y'])
        data = ro.DataFrame(d)
        formula = 'y ~ '
        for x in names:
            formula += x + '+'
        formula = formula[:-1]
        fit_res = ro.r.glm(formula=ro.r(formula), data=data, weights=weights_float_vector,  family=ro.r('binomial(link="logit")'))

        ## print fit_res
        print "formula:", formula


num_cross_validation_foldss = np.array([(0,1,2,3,4,5,6)])
y_values = np.array([0])
weights =  np.array([1])

train(num_cross_validation_foldss, y_values, weights)



