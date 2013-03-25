import h2o_cmd, h2o
import re, random

def pickRandGlmParams(paramDict, params):
    colX = 0
    randomGroupSize = random.randint(1,len(paramDict))
    for i in range(randomGroupSize):
        randomKey = random.choice(paramDict.keys())
        randomV = paramDict[randomKey]
        randomValue = random.choice(randomV)
        params[randomKey] = randomValue
        if (randomKey=='x'):
            colX = randomValue

        # case only used if binomial? binomial is default if no family
        if 'family' not in params or params['family'] == 'binomial':
            maxCase = max(paramDict['case'])
            minCase = min(paramDict['case'])
            # make sure the combo of case and case_mode makes sense
            # there needs to be some entries in both effective cases
            if ('case_mode' in params):
                if ('case' not in params) or (params['case'] is None):
                    params['case'] = 1
                elif params['case_mode']=="<" and params['case']==minCase:
                    params['case'] += 1
                elif params['case_mode']==">" and params['case']==maxCase:
                    params['case'] -= 1
                elif params['case_mode']==">=" and params['case']==minCase:
                    params['case'] += 1
                elif params['case_mode']=="<=" and params['case']==maxCase:
                    params['case'] -= 1

    return colX

def simpleCheckGLM(self, glm, colX, allowFailWarning=False, allowZeroCoeff=False,
    prettyPrint=False, **kwargs):
    # h2o GLM will verboseprint the result and print errors. 
    # so don't have to do that
    # different when cross validation  is used? No trainingErrorDetails?
    GLMModel = glm['GLMModel']
    warnings = None
    if 'warnings' in GLMModel:
        warnings = GLMModel['warnings']
        # stop on failed
        x = re.compile("failed", re.IGNORECASE)
        # don't stop if fail to converge
        c = re.compile("converge", re.IGNORECASE)
        for w in warnings:
            print "\nwarning:", w
            if re.search(x,w) and not allowFailWarning: 
                if re.search(c,w):
                    # ignore the fail to converge warning now
                    pass
                else: 
                    # stop on other 'fail' warnings (are there any? fail to solve?
                    raise Exception(w)

    print "GLMModel execution time (milliseconds):", GLMModel['time']

    # FIX! don't get GLMParams if it can't solve?
    GLMParams = GLMModel["GLMParams"]
    family = GLMParams["family"]

    iterations = GLMModel['iterations']
    print "GLMModel/iterations:", iterations

    # pop the first validation from the list
    validationsList = GLMModel['validations']
    # don't want to modify validationsList in case someone else looks at it
    validations = validationsList[0]
    print "GLMModel/validations"
    print "%15s %s" % ("err:\t", validations['err'])
    print "%15s %s" % ("nullDev:\t", validations['nullDev'])
    print "%15s %s" % ("resDev:\t", validations['resDev'])

    # threshold only there if binomial?
    # auc only for binomial
    if family=="binomial":
        print "%15s %s" % ("auc:\t", validations['auc'])
        print "%15s %s" % ("threshold:\t", validations['threshold'])

    if family=="poisson" or family=="gaussian" or family=="gamma":
        print "%15s %s" % ("aic:\t", validations['aic'])

    # get a copy, so we don't destroy the original when we pop the intercept
    coefficients = GLMModel['coefficients'].copy()
    column_names = GLMModel['column_names']
    # get the intercept out of there into it's own dictionary
    intercept = coefficients.pop('Intercept', None)
    # have to skip the output col! get it from kwargs
    # better always be there!
    y = kwargs['y']


    # the dict keys are column headers if they exist...how to order those? new: use the 'column_names'
    # from the response
    # Tomas created 'column_names which is the coefficient list in order.
    # Just use it to index coefficients! works for header or no-header cases
    # I guess now we won't print the "None" cases for dropped columns (constant columns!)
    # Because Tomas doesn't get everything in 'column_names' if dropped by GLMQuery before
    # he gets it? 
    def add_to_coefficient_list_and_string(c,cList,cString):
        if c in coefficients:
            cValue = coefficients[c]
            cValueString = "%s: %.5e   " % (c, cValue)
        else:
            print "Warning: didn't see '" + c + "' in json coefficient response.",\
                  "Inserting 'None' with assumption it was dropped due to constant column)"
            cValue = None
            cValueString = "%s: %s   " % (c, cValue)

        cList.append(cValue)
        # we put each on newline for easy comparison to R..otherwise keep condensed
        if prettyPrint: 
            cValueString = "H2O coefficient " + cValueString + "\n"
        # not mutable?
        return cString + cValueString

    # creating both a string for printing and a list of values
    cString = ""
    cList = []
    # print in order using col_names
    # column_names is input only now..same for header or no header, or expanded enums
    for c in column_names:
        cString = add_to_coefficient_list_and_string(c,cList,cString)

    if prettyPrint: 
        print "\nH2O intercept:\t\t%.5e" % intercept
        print cString
    else:
        print "\nintercept:", intercept, cString

    print "\nTotal # of coefficients:", len(column_names)

    # pick out the coefficent for the column we enabled for enhanced checking. Can be None.
    # FIX! temporary hack to deal with disappearing/renaming columns in GLM
    if (not allowZeroCoeff) and (colX is not None):
        absXCoeff = abs(float(coefficients[str(colX)]))
        self.assertGreater(absXCoeff, 1e-26, (
            "abs. value of GLM coefficients['" + str(colX) + "'] is " +
            str(absXCoeff) + ", not >= 1e-26 for X=" + str(colX)
            ))

    # intercept is buried in there too
    absIntercept = abs(float(intercept))
    self.assertGreater(absIntercept, 1e-26, (
        "abs. value of GLM coefficients['Intercept'] is " +
        str(absIntercept) + ", not >= 1e-26 for Intercept"
                ))

    # this is good if we just want min or max
    # maxCoeff = max(coefficients, key=coefficients.get)
    # for more, just invert the dictionary and ...
    maxKey = max([(abs(coefficients[x]),x) for x in coefficients])[1]
    print "H2O Largest abs. coefficient value:", maxKey, coefficients[maxKey]
    minKey = min([(abs(coefficients[x]),x) for x in coefficients])[1]
    print "H2O Smallest abs. coefficient value:", minKey, coefficients[minKey]

    # many of the GLM tests aren't single column though.
    # quick and dirty check: if all the coefficients are zero, 
    # something is broken
    # intercept is in there too, but this will get it okay
    # just sum the abs value  up..look for greater than 0

    # skip this test if there is just one coefficient. Maybe pointing to a non-important coeff?
    if (not allowZeroCoeff) and (len(coefficients)>1):
        s = 0.0
        for c in coefficients:
            v = coefficients[c]
            s += abs(float(v))

        self.assertGreater(s, 1e-26, (
            "sum of abs. value of GLM coefficients/intercept is " + str(s) + ", not >= 1e-26"
            ))

    return (warnings, cList, intercept)


# compare this glm to last one. since the files are concatenations, 
# the results should be similar? 10% of first is allowed delta
def compareToFirstGlm(self, key, glm, firstglm):
    # if isinstance(firstglm[key], list):
    # in case it's not a list allready (err is a list)
    h2o.verboseprint("compareToFirstGlm key:", key)
    h2o.verboseprint("compareToFirstGlm glm[key]:", glm[key])
    # key could be a list or not. if a list, don't want to create list of that list
    # so use extend on an empty list. covers all cases?
    if type(glm[key]) is list:
        kList  = glm[key]
        firstkList = firstglm[key]
    elif type(glm[key]) is dict:
        raise Exception("compareToFirstGLm: Not expecting dict for " + key)
    else:
        kList  = [glm[key]]
        firstkList = [firstglm[key]]

    for k, firstk in zip(kList, firstkList):
        # delta must be a positive number ?
        delta = .1 * abs(float(firstk))
        msg = "Too large a delta (" + str(delta) + ") comparing current and first for: " + key
        self.assertAlmostEqual(float(k), float(firstk), delta=delta, msg=msg)
        self.assertGreaterEqual(abs(float(k)), 0.0, str(k) + " abs not >= 0.0 in current")


def simpleCheckGLMGrid(self, glmGridResult, colX=None, allowFailWarning=False, **kwargs):
    destination_key = glmGridResult['destination_key']
    inspectGG = h2o_cmd.runInspect(None, destination_key)
    h2o.verboseprint("Inspect of destination_key", destination_key,":\n", h2o.dump_json(inspectGG))

    # FIX! currently this is all unparsed!
    #type = inspectGG['type']
    #if 'unparsed' in type:
    #    print "Warning: GLM Grid result destination_key is unparsed, can't interpret. Ignoring for now"
    #    print "Run with -b arg to look at the browser output, for minimal checking of result"

    ### cols = inspectGG['cols']
    response = inspectGG['response'] # dict
    ### rows = inspectGG['rows']
    #value_size_bytes = inspectGG['value_size_bytes']

    model0 = glmGridResult['models'][0]
    alpha = model0['alpha']
    area_under_curve = model0['area_under_curve']
    error_0 = model0['error_0']
    error_1 = model0['error_1']
    key = model0['key']
    print "best GLM model key:", key

    glm_lambda = model0['lambda']

    # now indirect to the GLM result/model that's first in the list (best)
    inspectGLM = h2o_cmd.runInspect(None, key)
    h2o.verboseprint("GLMGrid inspectGLM:", h2o.dump_json(inspectGLM))
    simpleCheckGLM(self, inspectGLM, colX, allowFailWarning=allowFailWarning, **kwargs)

