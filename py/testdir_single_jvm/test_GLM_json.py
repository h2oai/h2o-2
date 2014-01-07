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

    def test_B_benign(self):
        h2o.nodes[0].log_view()
        namelist = h2o.nodes[0].log_download()

        print "\nStarting benign.csv"
        csvFilename = "benign.csv"
        csvPathname = 'logreg' + '/' + csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=csvFilename + ".hex", schema='put')
        # columns start at 0
        y = "3"
        # cols 0-13. 3 is output
        # no member id in this one
        for maxx in range(11,14):
            x = range(maxx)
            x.remove(3) # 3 is output
            x = ",".join(map(str,x))
            print "\nx:", x
            print "y:", y

            kwargs = {'x': x, 'y':  y}
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=15, **kwargs)
            # no longer look at STR?
            print (h2o.dump_json(glm)) 
            glm_c = h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
          
            print (h2o.dump_json(glm_c)) 
            sys.stdout.write('.')
            sys.stdout.flush() 
        y = "4"
        # cols 0-13. 3 is output
        # no member id in this one
        for maxx in range(11,14):
            x = range(maxx)
            x.remove(4) # 3 is output
            x = ",".join(map(str,x))
            print "\nx:", x
            print "y:", y

            kwargs = {'x': x, 'y':  y}
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=15, **kwargs)
            # no longer look at STR?
            print (h2o.dump_json(glm))
            glm_c = h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
 
            print (h2o.dump_json(glm_c))
            sys.stdout.write('.')
            sys.stdout.flush()

if __name__ == '__main__':
    h2o.unit_main()
