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
        h2o.tear_down_cloud()

    def test_B_benign(self):
        print "\nStarting benign.csv"
        csvFilename = "benign.csv"
        csvPathname = h2o.find_file('smalldata/logreg' + '/' + csvFilename)
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=csvFilename + ".hex")
        # columns start at 0
        y = "3"
        # cols 0-13. 3 is output
        # no member id in this one
        for maxx in range(4,14):
            x = range(maxx)
            x.remove(3) # 3 is output
            x = ",".join(map(str,x))
            print "\nx:", x
            print "y:", y
            
            # solver can be ADMM
            kwargs = {'x': x, 'y':  y,\
                 'expert': 1, 'lsm_solver': 'GenGradient', 'standardize': 1, 'n_folds': 1}
            # fails with n_folds
            print "Not doing n_folds with benign. Fails with 'unable to solve?'"
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=30, **kwargs)
            # no longer look at STR?
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            h2o.check_sandbox_for_errors()
            sys.stdout.write('.')
            sys.stdout.flush() 

    def test_C_prostate(self):
        print "\nStarting prostate.csv"
        # columns start at 0
        y = "1"
        csvFilename = "prostate.csv"
        csvPathname = h2o.find_file('smalldata/logreg' + '/' + csvFilename)
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=csvFilename + ".hex")

        for maxx in range(2,9):
            x = range(maxx)
            x.remove(0) # 0 is member ID. not used
            x.remove(1) # 1 is output
            x = ",".join(map(str,x))
            print "\nx:", x
            print "y:", y

            # solver can be ADMM. standardize normalizes the data.
            kwargs = {'x': x, 'y':  y, 'n_folds': 5,\
                'expert': 1, 'lsm_solver': 'GenGradient', 'standardize':1}
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=30, **kwargs)
            # ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON
            h2o_glm.simpleCheckGLM(self, glm, 'AGE', **kwargs)
            h2o.check_sandbox_for_errors()
            sys.stdout.write('.')
            sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
