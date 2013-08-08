import h2o_cmd, h2o
import re, random, math

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

        if 'family' in params and 'link' in params: 
            # don't allow logit for poisson
            if params['family'] is not None and params['family'] == 'poisson':
                if params['link'] is not None and params['link'] in ('logit'):
                    params['link'] = None # use default link for poisson always

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


def simpleCheckGLMScore(self, glmScore, family='gaussian', allowFailWarning=False, **kwargs):
    warnings = None
    if 'warnings' in glmScore:
        warnings = glmScore['warnings']
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

    validation = glmScore['validation']
    print "%15s %s" % ("err:\t", validation['err'])
    print "%15s %s" % ("nullDev:\t", validation['nullDev'])
    print "%15s %s" % ("resDev:\t", validation['resDev'])

    # threshold only there if binomial?
    # auc only for binomial
    if family=="binomial":
        print "%15s %s" % ("auc:\t", validation['auc'])
        print "%15s %s" % ("threshold:\t", validation['threshold'])

    if family=="poisson" or family=="gaussian":
        print "%15s %s" % ("aic:\t", validation['aic'])

    if math.isnan(validation['err']):
        emsg = "Why is this err = 'nan'?? %6s %s" % ("err:\t", validation['err'])
        raise Exception(emsg)

    if math.isnan(validation['resDev']):
        emsg = "Why is this resDev = 'nan'?? %6s %s" % ("resDev:\t", validation['resDev'])
        raise Exception(emsg)

    if math.isnan(validation['nullDev']):
        emsg = "Why is this nullDev = 'nan'?? %6s %s" % ("nullDev:\t", validation['nullDev'])
        raise Exception(emsg)

def simpleCheckGLM(self, glm, colX, allowFailWarning=False, allowZeroCoeff=False,
    prettyPrint=False, noPrint=False, maxExpectedIterations=None, **kwargs):
    # if we hit the max_iter, that means it probably didn't converge. should be 1-maxExpectedIter

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

    # for key, value in glm.iteritems(): print key
    # not in GLMGrid?

    # FIX! don't get GLMParams if it can't solve?
    GLMParams = GLMModel["GLMParams"]
    family = GLMParams["family"]

    iterations = GLMModel['iterations']
    print "GLMModel/iterations:", iterations

            # if we hit the max_iter, that means it probably didn't converge. should be 1-maxExpectedIter
    if maxExpectedIterations is not None and iterations  > maxExpectedIterations:
            raise Exception("Convergence issue? GLM did iterations: %d which is greater than expected: %d" % (iterations, maxExpectedIterations) )

    # pop the first validation from the list
    validationsList = GLMModel['validations']
    # don't want to modify validationsList in case someone else looks at it
    validations = validationsList[0]

    # xval. compare what we asked for and what we got.
    n_folds = kwargs.setdefault('n_folds', None)
    if not 'xval_models' in validations:
        if n_folds > 1:
            raise Exception("No cross validation models returned. Asked for "+n_folds)
    else:
        xval_models = validations['xval_models']
        if n_folds and n_folds > 1:
            if len(xval_models) != n_folds:
                raise Exception(len(xval_models)+" cross validation models returned. Asked for "+n_folds)
        else:
            # should be default 10?
            if len(xval_models) != 10:
                raise Exception(str(len(xval_models))+" cross validation models returned. Default should be 10")

    print "GLMModel/validations"
    print "%15s %s" % ("err:\t", validations['err'])
    print "%15s %s" % ("nullDev:\t", validations['nullDev'])
    print "%15s %s" % ("resDev:\t", validations['resDev'])

    # threshold only there if binomial?
    # auc only for binomial
    if family=="binomial":
        print "%15s %s" % ("auc:\t", validations['auc'])
        print "%15s %s" % ("threshold:\t", validations['threshold'])

    if family=="poisson" or family=="gaussian":
        print "%15s %s" % ("aic:\t", validations['aic'])

    if math.isnan(validations['err']):
        emsg = "Why is this err = 'nan'?? %6s %s" % ("err:\t", validations['err'])
        raise Exception(emsg)

    if math.isnan(validations['resDev']):
        emsg = "Why is this resDev = 'nan'?? %6s %s" % ("resDev:\t", validations['resDev'])
        raise Exception(emsg)

    if math.isnan(validations['nullDev']):
        emsg = "Why is this nullDev = 'nan'?? %6s %s" % ("nullDev:\t", validations['nullDev'])
        raise Exception(emsg)

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
        if not noPrint:
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
    if (len(coefficients)>0):
        maxKey = max([(abs(coefficients[x]),x) for x in coefficients])[1]
        print "H2O Largest abs. coefficient value:", maxKey, coefficients[maxKey]
        minKey = min([(abs(coefficients[x]),x) for x in coefficients])[1]
        print "H2O Smallest abs. coefficient value:", minKey, coefficients[minKey]
    else: 
        print "Warning, no coefficients returned. Must be intercept only?"

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

    print "GLMModel model time (milliseconds):", GLMModel['model_time']
    print "GLMModel validation time (milliseconds):", validations['val_time']
    print "GLMModel lsm time (milliseconds):", GLMModel['lsm_time']

    # shouldn't have any errors
    h2o.check_sandbox_for_errors()

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
    model_key = model0['key']
    print "best GLM model key:", model_key

    glm_lambda = model0['lambda']

    # now indirect to the GLM result/model that's first in the list (best)
    inspectGLM = h2o_cmd.runInspect(None, model_key)
    h2o.verboseprint("GLMGrid inspectGLM:", h2o.dump_json(inspectGLM))
    simpleCheckGLM(self, inspectGLM, colX, allowFailWarning=allowFailWarning, **kwargs)

# This gives me a comma separated x string, for all the columns, with cols with
# missing values, enums, and optionally matching a pattern, removed. useful for GLM
# since it removes rows with any col with NA

# get input from this.
#   (missingValuesDict, constantValuesDict, enumSizeDict, colTypeDict, colNameDict) = \
#                h2o_cmd.columnInfoFromInspect(parseKey['destination_key', 
#                exceptionOnMissingValues=False, timeoutSecs=300)

def goodXFromColumnInfo(y, 
    num_cols=None, missingValuesDict=None, constantValuesDict=None, enumSizeDict=None, 
    colTypeDict=None, colNameDict=None, keepPattern=None, key=None, 
    timeoutSecs=120, forRF=False, noPrint=False):

    y = str(y)

    # if we pass a key, means we want to get the info ourselves here
    if key is not None:
        (missingValuesDict, constantValuesDict, enumSizeDict, colTypeDict, colNameDict) = \
            h2o_cmd.columnInfoFromInspect(key, exceptionOnMissingValues=False, 
            timeoutSecs=timeoutSecs)
        num_cols = len(colNameDict)

    # now remove any whose names don't match the required keepPattern
    if keepPattern is not None:
        keepX = re.compile(keepPattern)
    else:
        keepX = None

    x = range(num_cols)
    # need to walk over a copy, cause we change x
    xOrig = x[:]
    ignore_x = [] # for use by RF
    for k in xOrig:
        name = colNameDict[k]
        # remove it if it has the same name as the y output
        if str(k)== y: # if they pass the col index as y
            if not noPrint:
                print "Removing %d because name: %s matches output %s" % (k, str(k), y)
            x.remove(k)
            # rf doesn't want it in ignore list
            # ignore_x.append(k)
        elif name == y: # if they pass the name as y 
            if not noPrint:
                print "Removing %d because name: %s matches output %s" % (k, name, y)
            x.remove(k)
            # rf doesn't want it in ignore list
            # ignore_x.append(k)

        elif keepX is not None and not keepX.match(name):
            if not noPrint:
                print "Removing %d because name: %s doesn't match desired keepPattern %s" % (k, name, keepPattern)
            x.remove(k)
            ignore_x.append(k)

        # missing values reports as constant also. so do missing first.
        # remove all cols with missing values
        # could change it against num_rows for a ratio
        elif k in missingValuesDict:
            value = missingValuesDict[k]
            if not noPrint:
                print "Removing %d with name: %s because it has %d missing values" % (k, name, value)
            x.remove(k)
            ignore_x.append(k)

        elif k in constantValuesDict:
            value = constantValuesDict[k]
            if not noPrint:
                print "Removing %d with name: %s because it has constant value: %s " % (k, name, str(value))
            x.remove(k)
            ignore_x.append(k)

        # this is extra pruning..
        # remove all cols with enums, if not already removed
        elif k in enumSizeDict:
            value = enumSizeDict[k]
            if not noPrint:
                print "Removing %d %s because it has enums of size: %d" % (k, name, value)
            x.remove(k)
            ignore_x.append(k)

    if not noPrint:
        print "x has", len(x), "cols"
        print "ignore_x has", len(ignore_x), "cols"
    x = ",".join(map(str,x))
    ignore_x = ",".join(map(str,ignore_x))

    if not noPrint:
        print "\nx:", x
        print "\nignore_x:", ignore_x

    if forRF:
        return ignore_x
    else:
        return x


