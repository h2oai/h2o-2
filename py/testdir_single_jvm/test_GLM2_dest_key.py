import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()


    def test_GLM2_dest_key(self):
        print "\nStarting prostate.csv"
        # columns start at 0
        y = "1"
        csvFilename = "prostate.csv"
        csvPathname = 'logreg' + '/' + csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=csvFilename + ".hex", schema='put')

        for maxx in [6]:
            destination_key='GLM_model_python_0_default_0'
            # illegal to have output col in the ignored_cols!
            kwargs = {
                'ignored_cols': '0',
                'response':  y, 
                'n_folds': 5, 
                'destination_key': destination_key,
            }
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=15, **kwargs)
            h2o_destination_key = glm['glm_model']['_key']
            print 'h2o_destination_key:', h2o_destination_key

            self.assertEqual(h2o_destination_key, destination_key, msg='I said to name the key %s, h2o used %s' % 
                (destination_key, h2o_destination_key))

            # ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON
            h2o_glm.simpleCheckGLM(self, glm, 'AGE', **kwargs)


if __name__ == '__main__':
    h2o.unit_main()
