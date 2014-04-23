import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_import as h2i
from pprint import pprint

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
        h2o.nodes[0].log_view()
        namelist = h2o.nodes[0].log_download()

        print "\nStarting benign.csv"
        csvFilename = "benign.csv"
        csvPathname = 'logreg' + '/' + csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=csvFilename + ".hex", schema='put')
        # columns start at 0
        y = "4"
        # cols 0-13. 3 is output
        # no member id in this one
        for maxx in range(11,14):
            x = range(maxx)
            x.remove(4) # 4 is output
            x = ",".join(map(str,x))

            print "\nx:", x
            print "y:", y

        # Adding values for lambda and max_iter

            kwargs = {
                'x': x, 
                'y': y, 
                'alpha': 0.5, 
                'lambda': 0.001, 
                'max_iter': 30,
                'standardize': 0,
            }
           
            startime = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=15, **kwargs)
            elapsedtime = time.time() - startime
            print("ELAPSED TIME ",elapsedtime)
            pprint(glm['GLMModel']['coefficients'])
            # pprint(glm['GLMModel']['normalized_coefficients'])
            pprint(glm['GLMModel']['nCols'])
            pprint(glm['GLMModel']['nLines'])
            pprint(glm['GLMModel']['iterations'])
            pprint(glm['GLMModel']['coefficients']['Intercept'])
            sys.stdout.write('.')
            sys.stdout.flush()

if __name__ == '__main__':
    h2o.unit_main()
