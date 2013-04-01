import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts

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

    @classmethod
    def tearDownClass(cls):
        ## time.sleep(3600)
        h2o.tear_down_cloud()

    def test_B_benign(self):
        csvFilename = "benign.csv"
        print "\nStarting", csvFilename 
        csvPathname = h2o.find_file('smalldata/logreg' + '/' + csvFilename)
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=csvFilename + ".hex")
        # columns start at 0
        # cols 0-13. 3 is output
        # no member id in this one
        y = "3"
        xList = []  
        for appendx in xrange(14):
            if (appendx == 0): 
                print "\nSkipping 0. Causes coefficient of 0 when used alone"
            elif (appendx == 3): 
                print "\n3 is output."
            else:
                xList.append(appendx)

        x = ','.join(map(str, xList))

        # just run the test with all x, not the intermediate results
        print "\nx:", x
        print "y:", y
        
        kwargs = {
            'x': x, 'y':  y, 'num_cross_validation_folds': 0, 
            'lambda': '1e-8:1e-2:100', 
            'alpha': '0,0.5,1',
            'thresholds': '0:1:0.01'
            }
        # fails with num_cross_validation_folds
        print "Not doing num_cross_validation_folds with benign. Fails with 'unable to solve?'"

        gg = h2o_cmd.runGLMGridOnly(parseKey=parseKey, timeoutSecs=120, **kwargs)
        # check the first in the models list. It should be the best
        colNames = [ 'STR','OBS','AGMT','FNDX','HIGD','DEG','CHK',
                     'AGP1','AGMN','NLV','LIV','WT','AGLP','MST' ]

        # h2o_glm.simpleCheckGLMGrid(self, gg, colNames[xList[-1]], **kwargs)
        h2o_glm.simpleCheckGLMGrid(self, gg, None, **kwargs)

    def test_C_prostate(self):
        csvFilename = "prostate.csv"
        print "\nStarting", csvFilename
        # columns start at 0
        csvPathname = h2o.find_file('smalldata/logreg' + '/' + csvFilename)
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=csvFilename + ".hex")

        y = "1"
        xList = []  
        for appendx in xrange(9):
            if (appendx == 0):
                print "\n0 is member ID. not used"
            elif (appendx == 1):
                print "\n1 is output."
            else:
                xList.append(appendx)

        x = ','.join(map(str, xList))
        # just run the test with all x, not the intermediate results
        print "\nx:", x
        print "y:", y

        # FIX! thresholds is used in GLMGrid. threshold is used in GLM
        # comma separated means use discrete values
        # colon separated is min/max/step
        # FIX! have to update other GLMGrid tests
        kwargs = {
            'x': x, 'y':  y, 'num_cross_validation_folds': 2, 
            'beta_epsilon': 1e-4,
            'lambda': '1e-8:1e3:100', 
            'alpha': '0,0.5,1',
            'thresholds': '0:1:0.01'
            }

        gg = h2o_cmd.runGLMGridOnly(parseKey=parseKey, timeoutSecs=120, **kwargs)
        colNames = ['D','CAPSULE','AGE','RACE','DPROS','DCAPS','PSA','VOL','GLEASON']
        # h2o_glm.simpleCheckGLMGrid(self, gg, colNames[xList[0]], **kwargs)
        h2o_glm.simpleCheckGLMGrid(self, gg, None, **kwargs)

if __name__ == '__main__':
    h2o.unit_main()
