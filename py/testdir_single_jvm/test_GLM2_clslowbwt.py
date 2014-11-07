import unittest, time, sys
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_glm, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_clslowbwt(self):
        # filename, y, timeoutSecs
        # this hangs during parse for some reason
        csvFilenameList = [
            ('clslowbwt.dat', 7, 10),
            ]

        trial = 0
        for (csvFilename, y, timeoutSecs) in csvFilenameList:
            print "\n" + csvFilename
            kwargs = {'n_folds': 0, 'family': 'binomial', 'response': y}
            start = time.time()
            parseResult = h2i.import_parse(bucket='smalldata', path='logreg/umass_statdata/' + csvFilename, 
                schema='put', timeoutSecs=timeoutSecs)
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "glm end (w/check) on ", csvFilename, 'took', time.time() - start, 'seconds'
            trial += 1
            print "\nTrial #", trial

if __name__ == '__main__':
    h2o.unit_main()
