import unittest, time, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_glm, h2o_util, h2o_hosts

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts(1)
        global SYNDATASETS_DIR
        SYNDATASETS_DIR = h2o.make_syn_dir()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_umass(self):
        # filename, Y, timeoutSecs
        # fix. the ones with comments may want to be a gaussian?
        csvFilenameList = [
            ('cgd.dat', 'gaussian', 12, 5, None),
            ('chdage.dat', 'binomial', 2, 5, None),
    
            # leave out ID and birth weight
            ('clslowbwt.dat', 'binomial', 7, 10, '1,2,3,4,5'),
            ('icu.dat', 'binomial', 1, 10, None),
            # need to exclude col 0 (ID) and col 10 (bwt)
            # but -x doesn't work..so do 2:9...range doesn't work? FIX!
            ('lowbwt.dat', 'binomial', 1, 10, '2,3,4,5,6,7,8,9'),
            ('lowbwtm11.dat', 'binomial', 1, 10, None),
            ('meexp.dat', 'gaussian', 3, 10, None),
            ('nhanes3.dat', 'binomial', 15, 10, None),
            ('pbc.dat', 'gaussian', 1, 10, None),
            ('pharynx.dat', 'gaussian', 12, 10, None),
            ('pros.dat', 'binomial', 1, 10, None),
            ('uis.dat', 'binomial', 8, 10, None),
            ]

        trial = 0
        for i in range(3):
            for (csvFilename, family, y, timeoutSecs, x) in csvFilenameList:
                csvPathname = h2o.find_file("smalldata/logreg/umass_statdata/" + csvFilename)
                kwargs = {'num_cross_validation_folds': 2, 'y': y, 'family': family, 'alpha': 1, 'lambda': 1e-4, 'link': 'familyDefault'}
                if x is not None:
                    kwargs['x'] = x

                start = time.time()
                glm = h2o_cmd.runGLM(csvPathname=csvPathname, key=csvFilename, timeoutSecs=timeoutSecs, **kwargs)
                h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
                print "glm end (w/check) on ", csvPathname, 'took', time.time() - start, 'seconds'
                trial += 1
                print "\nTrial #", trial

if __name__ == '__main__':
    h2o.unit_main()
