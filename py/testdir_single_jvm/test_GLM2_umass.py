import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_util, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1)
        global SYNDATASETS_DIR
        SYNDATASETS_DIR = h2o.make_syn_dir()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_umass(self):
        csvFilenameList = [
            ('cgd.dat', 'gaussian', 12, 30, None),
            ('chdage.dat', 'binomial', 2, 30, None),
    
            # leave out ID and birth weight
            ('clslowbwt.dat', 'binomial', 7, 60, [1,2,3,4,5]),
            ('icu.dat', 'binomial', 1, 60, None),
            # need to exclude col 0 (ID) and col 10 (bwt)
            # but -x doesn't work..so do 2:9...range doesn't work? FIX!
            ('lowbwt.dat', 'binomial', 1, 60, [2,3,4,5,6,7,8,9]),
            ('lowbwtm11.dat', 'binomial', 1, 60, None),
            ('meexp.dat', 'gaussian', 3, 60, None),
            ('nhanes3.dat', 'binomial', 15, 60, None),
            ('pbc.dat', 'gaussian', 1, 60, None),
            ('pharynx.dat', 'gaussian', 12, 60, None),
            ('pros.dat', 'binomial', 1, 60, None),
            ('uis.dat', 'binomial', 8, 60, None),
            ]

        trial = 0
        for i in range(3):
            for (csvFilename, family, y, timeoutSecs, x) in csvFilenameList:
                csvPathname = "logreg/umass_statdata/" + csvFilename
                kwargs = {'n_folds': 3, 'response': y, 'family': family, 'alpha': 1, 'lambda': 1e-4}


                parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', 
                    timeoutSecs=timeoutSecs)
                if x is not None:
                    ignored_cols = h2o_cmd.createIgnoredCols(key=parseResult['destination_key'], 
                        cols=x, response=y)
                    kwargs['ignored_cols'] = ignored_cols


                start = time.time()
                glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
                h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
                print "glm end (w/check) on ", csvPathname, 'took', time.time() - start, 'seconds'
                trial += 1
                print "\nTrial #", trial

if __name__ == '__main__':
    h2o.unit_main()
