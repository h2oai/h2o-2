import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_import as h2i

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


    def test_C_prostate(self):
        print "\nStarting prostate.csv"
        # columns start at 0
        y = "1"
        x = ""
        csvFilename = "prostate.csv"
        csvPathname = 'logreg' + '/' + csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=csvFilename + ".hex", schema='put')

        for maxx in [6]:
            x = range(maxx)
            x.remove(0) # 0 is member ID. not used
            x.remove(1) # 1 is output
            x = ",".join(map(str,x))
            print "\nx:", x
            print "y:", y

            destination_key='GLM_model_python_0_default_0'

            kwargs = {
                'x': x, 
                'y':  y, 
                'n_folds': 5, 
                'destination_key': destination_key,
            }
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=15, **kwargs)
            h2o_destination_key = glm['destination_key']
            print 'h2o_destination_key:', h2o_destination_key

            self.assertEqual(h2o_destination_key, destination_key, msg='I said to name the key %s, h2o used %s' % 
                (destination_key, h2o_destination_key))

            # ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON
            h2o_glm.simpleCheckGLM(self, glm, 'AGE', **kwargs)


if __name__ == '__main__':
    h2o.unit_main()
