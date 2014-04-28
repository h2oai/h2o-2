import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_glm, h2o_import as h2i
import h2o_browse as h2b

argcaseList = [
    {   
        'x': '0,1,2,3,4,5,6,7,8,10,11',
        'y': 106,
        'family': 'gaussian',
        'lambda': 1.0E-5,
        'max_iter': 50,
        'thresholds': 0.5,
        'link': 'familyDefault',
        'n_folds': 0,
        'alpha': 1,
        'beta_epsilon': 1.0E-4 
    },
]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_hhp_107_01_browse(self):
        csvPathname = 'hhp_107_01.data.gz'
        print "\n" + csvPathname
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put',
            hex_key="hhp_107_01.data.hex", timeoutSecs=15)

        # pop open a browser on the cloud
        # h2b.browseTheCloud()
        trial = 0
        for argcase in argcaseList:
            print "\nTrial #", trial, "start"
            kwargs = argcase
            print 'y:', kwargs['y']
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, browseAlso=True, timeoutSecs=200, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "\nTrial #", trial


if __name__ == '__main__':
    h2o.unit_main()
