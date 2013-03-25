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

    def test_C_hhp_107_01(self):
        csvPathname = h2o.find_file("smalldata/hhp_107_01.data.gz")
        print "\n" + csvPathname

        y = "106"
        x = ""
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, timeoutSecs=15)

        # create the X that excludes some columns
        trial = 0
        # don't need to exclude specifically any more...algo handles it
        if (1==0):
            for appendx in xrange(107):
                if (1==0 and appendx == 9):
                    print "9 causes singularity. not used"
                elif (1==0 and appendx == 12): 
                    print "12 causes singularity. not used"
                elif (1==0 and appendx == 25): 
                    print "25 causes singularity. not used"
                elif (1==0 and appendx == 53): 
                    print "53 causes singularity. not used"
                elif (1==0 and appendx == 54): 
                    print "54 causes singularity. not used"
                elif (1==0 and appendx == 76): 
                    print "76 causes singularity. not used"
                elif (1==0 and appendx == 91): 
                    print "91 causes singularity. not used"
                elif (1==0 and appendx == 103): 
                    print "103 causes singularity. not used"
                elif (1==0 and appendx == 106):
                    print "106 is output."
                else:
                    if x == "": 
                        x = str(appendx)
                    else:
                        x = x + "," + str(appendx)

        for trial in xrange(3):
            sys.stdout.write('.')
            sys.stdout.flush() 
            print "\nx:", x
            print "y:", y

            start = time.time()
            kwargs = {'x': x, 'y': y, 'num_cross_validation_folds': 6}
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=300, **kwargs)

            # pass the kwargs with all the params, so we know what we asked for!
            h2o_glm.simpleCheckGLM(self, glm, 57, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "\nTrial #", trial

if __name__ == '__main__':
    h2o.unit_main()
