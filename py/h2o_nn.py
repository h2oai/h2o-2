import h2o_cmd, h2o, h2o_util
import re, random, math

## Check that the last scored validation error is within a certain relative error of the expected result
def simpleCheckValidationError(self, modelView, rows, expectedErr, relTol, **kwargs):

    errsLast = modelView['validation_errors'][-1] # last scoring result
    h2o.verboseprint("NN 'Last scoring on test set:'", h2o.dump_json(errsLast))
    expectedSamples = rows * kwargs['epochs']
    print 'Expecting ' + format(expectedSamples) + ' training samples'
    if errsLast['training_samples'] != expectedSamples:
        raise Exception("Training samples should be equal to %s" % expectedSamples)

    print "Expected test set error: " + format(expectedErr)
    print "Actual   test set error: " + format(errsLast['classification'])
    if errsLast['classification'] != expectedErr and abs((expectedErr - errsLast['classification'])/expectedErr) > relTol:
        raise Exception("Test set classification error of %s is not within %s %% relative error of %s" % (errsLast['classification'], float(relTol)*100, expectedErr))

    warnings = None

    # shouldn't have any errors
    h2o.check_sandbox_for_errors()

    return (warnings)
