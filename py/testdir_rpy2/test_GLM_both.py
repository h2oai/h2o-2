import unittest, time, sys
sys.path.extend(['.','..','py'])
import copy

print "Needs numpy, rpy2, and R installed. Run on 192.168.171-175"
# FIX! maybe should update to build_cloud_with_hosts to run on 171-175?

import h2o, h2o_cmd, h2o_glm, h2o_util
import numpy as np
from rpy2 import robjects as ro


# y is h2o style. start at 0
def glm_R_and_compare(csvPathname, family, formula, y, header=False, h2oResults=None):
    # df = ro.DataFrame.from_csvfile(csvPathname, col_names=col_names, header=False)
    df = ro.DataFrame.from_csvfile(csvPathname, header=False)
    cn = ro.r.colnames(df)
    ### print df
    fit = ro.r.glm(formula=ro.r(formula), data=df, family=ro.r(family + '(link="logit")'))
    gsummary = ro.r.summary(fit)

    # print ro.r.summary(fit)
    coef = ro.r.coef(fit)

    # FIX! where do the GLM warnings come from
    warningsR = []
    interceptR = coef[0]
    cListR = coef[1:]

    if h2oResults is not None: # create delta list
        (warningsH2o, cListH2o, interceptH2o) = h2oResults
        interceptDelta = abs(abs(interceptH2o) - abs(interceptR))
        cDelta = [abs(abs(a) - abs(b)) for a,b in zip(cListH2o, cListR)]
    else:
        (warningsH2o, cListH2o, interceptH2o) = (None, None, None)
        interceptDelta = None
        cDelta = [None for a in cListR]

    def printit(a,b,c,d):
        print "%-20s %-20.5e %8s %5.2f%% %10s %-20.5e" % \
            ("R " + a + " " + b + ":", c, "diff:", abs(d/c)*100, "abs diff:", d)

    print
    printit("intercept", "", interceptR, interceptDelta)
    for i,cValue in enumerate(cListR):
        # skip over the output col name
        if i>=y: 
            cnIndex = i+1
        else:
            cnIndex = i
        printit("coefficient", cn[cnIndex], cValue, cDelta[i])

    ### print "\nDumping some raw R results (info already printed above)"
    ### print "coef:", ro.r.coef(fit)
    
    # what each index gives
    gsummaryIndexDesc = [
        'call',
        'terms',
        'family',
        'deviance',
        'aic',
        'contrasts',
        'df.residual',
        'null.deviance',
        'df.null',
        'iter',
        'deviance.resid',
        'coefficients',
        'aliased',
        'dispersion',
        'df',
        'cov.unscaled',
        'cov.scaled',
        ]

    whatIwant = [
        'family',
        'deviance',
        'aic',
        'df.residual',
        'null.deviance',
        'df.null',
        'iter',
        ]

    for i,v in enumerate(gsummary):
        d = gsummaryIndexDesc[i]
        if d in whatIwant:
            print "%s %s %15s %s" % ("R gsummary", i, d + ":\t", gsummary[i][0])

    # to get possiblities from gsummary above, do this
    # print "summary", ro.r.summary(gsummary)

    # print "residuals", ro.r.residuals(fit)
    # print "predict", ro.r.predict(fit)
    # print "fitted", ro.r.fitted(fit)

    # HMM! what about if H2O has dropped coefficients but R doesn't ...oh well!
    # not sure how to get from the R vector to python list..try this
    # theoretically we shouldn't have to worry about it? . But we strip the intercept out too.

    return (warningsR, cListR, interceptR)

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(1)
        global SYNDATASETS_DIR
        SYNDATASETS_DIR = h2o.make_syn_dir()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_umass(self):
        if (1==1):
            csvFilenameList = [
                # col is zero based
                ('uis.dat', 'binomial', 8, 5, False),
                # ('cgd.dat', 'gaussian', 12, 5, False),
                ('pros.dat', 'binomial', 1, 10, False),
                ('chdage.dat', 'binomial', 2, 5, True),
                ('icu.dat', 'binomial', 1, 10, False),
                # how to ignore 6? '1,2,3,4,5', False),
                # ('clslowbwt.dat', 'binomial', 7, 10, False),
            ]
        else:
            csvFilenameList = [

                # leave out ID and birth weight
                ('icu.dat', 'binomial', 1, 10, None),
                # need to exclude col 0 (ID) and col 10 (bwt)
                # but -x doesn't work..so do 2:9...range doesn't work? FIX!
                ('nhanes3.dat', 'binomial', 15, 10),
                ('lowbwt.dat', 'binomial', 1, 10, '2,3,4,5,6,7,8,9'),
                ('lowbwtm11.dat', 'binomial', 1, 10, None),
                ('meexp.dat', 'gaussian', 3, 10, None),
                ('nhanes3.dat', 'binomial', 15, 10, None),
                ('pbc.dat', 'gaussian', 1, 10, None),
                ('pharynx.dat', 'gaussian', 12, 10, None),
                ('uis.dat', 'binomial', 8, 10, None),
            ]

        trial = 0
        for (csvFilename, family, y, timeoutSecs, header) in csvFilenameList:
            # FIX! do something about this file munging
            csvPathname1 = h2o.find_file("smalldata/logreg/umass_statdata/" + csvFilename)
            csvPathname2 = SYNDATASETS_DIR + '/' + csvFilename + '_2.csv'
            h2o_util.file_clean_for_R(csvPathname1, csvPathname2)

            # we can inspect this to get the number of cols in the dataset (trust H2O here)
            parseKey = h2o_cmd.parseFile(None, csvPathname2, key=csvFilename, timeoutSecs=10)
            # we could specify key2 above but this is fine
            destination_key = parseKey['destination_key']
            inspect = h2o_cmd.runInspect(None, destination_key)
            num_cols = inspect['num_cols']
            num_rows = inspect['num_rows']
            print "num_cols", num_cols, "num_rows", num_rows
            ##  print h2o.dump_json(inspect)

            # create formula and the x for H2O GLM
            formula = "V" + str(y+1) + " ~ "
            x = None
            col_names = ""
            for c in range(0,num_cols):
                # don't add the output col to the RHS of formula
                if x is None: 
                    col_names += "V" + str(c+1)
                else: 
                    col_names += ",V" + str(c+1)

                if c!=y:
                    if x is None: 
                        x = str(c)
                        formula += "V" + str(c+1)
                    else: 
                        x += "," + str(c)
                        formula += "+V" + str(c+1)

            print 'formula:', formula
            print 'col_names:', col_names
            print 'x:', x

            kwargs = { 'num_cross_validation_folds': 0, 'y': y, 'x': x,
                'family': family, 'link': 'familyDefault',
                'alpha': 0, 'lambda': 0, 'case_mode': '=', 'case': 1,
                'beta_epsilon': 1.0E-4, 'max_iter': 50 }

            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)

            print "glm end (w/check) on ", csvPathname2, 'took', time.time()-start, 'seconds'
            h2oResults = h2o_glm.simpleCheckGLM(self, glm, None, prettyPrint=True, **kwargs)
            # now do it thru R and compare
            (warningsR, cListR, interceptR) = glm_R_and_compare(csvPathname2, family, formula, y, 
                header=header, h2oResults=h2oResults)

            trial += 1
            print "\nTrial #", trial

if __name__ == '__main__':
    h2o.unit_main()




