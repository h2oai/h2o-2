import h2o_cmd, h2o_util
import re, random, math
from h2o_test import verboseprint, dump_json, check_sandbox_for_errors

def pickRandDeepLearningParams(paramDict, params):
    randomGroupSize = random.randint(1,len(paramDict))
    for i in range(randomGroupSize):
        randomKey = random.choice(paramDict.keys())
        randomV = paramDict[randomKey]
        randomValue = random.choice(randomV)
        params[randomKey] = randomValue

    return


## Check that the last scored validation error is within a certain relative error of the expected result
def checkLastValidationError(self, model, rows, expectedErr, relTol, **kwargs):
    errsLast = model['validation_errors'][-1] # last scoring result
    verboseprint("Deep Learning 'Last scoring on test set:'", dump_json(errsLast))
    expectedSamples = rows * kwargs['epochs']
    print 'Expecting ' + format(expectedSamples) + ' training samples'
    if errsLast['training_samples'] != expectedSamples:
        raise Exception("Number of training samples should be equal to %s" % expectedSamples)

    print "Expected test set error: " + format(expectedErr)
    print "Actual   test set error: " + format(errsLast['classification'])
    if errsLast['classification'] != expectedErr and abs((expectedErr - errsLast['classification'])/expectedErr) > relTol:
        raise Exception("Test set classification error of %s is not within %s %% relative error of %s" % (errsLast['classification'], float(relTol)*100, expectedErr))

    warnings = None

    # shouldn't have any errors
    check_sandbox_for_errors()

    return (warnings)


## Check that the scored validation error is within a certain relative error of the expected result
def checkScoreResult(self, result, expectedErr, relTol, **kwargs):
    print "Expected score error: " + format(expectedErr)
    print "Actual   score error: " + format(result['classification_error'])
    if result['classification_error'] != expectedErr and abs((expectedErr - result['classification_error'])/expectedErr) > relTol:
        raise Exception("Scored classification error of %s is not within %s %% relative error of %s" % (result['classification_error'], float(relTol)*100, expectedErr))

    warnings = None

    # shouldn't have any errors
    check_sandbox_for_errors()

    return (warnings)
