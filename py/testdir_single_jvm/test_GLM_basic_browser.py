import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_import as h2i, h2o_browse as h2b

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
        h2b.browseTheCloud()

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
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            sys.stdout.write('.')
            sys.stdout.flush() 

        # now redo it all thru the browser
        h2b.browseJsonHistoryAsUrl()


    def test_C_prostate(self):
        h2o.nodes[0].log_view()
        namelist = h2o.nodes[0].log_download()
        print "\nStarting prostate.csv"
        # columns start at 0
        y = "1"
        x = ""
        csvFilename = "prostate.csv"
        csvPathname = 'logreg' + '/' + csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=csvFilename + ".hex", schema='put')

        for maxx in range(2,6):
            x = range(maxx)
            x.remove(0) # 0 is member ID. not used
            x.remove(1) # 1 is output
            x = ",".join(map(str,x))
            print "\nx:", x
            print "y:", y

            kwargs = {'x': x, 'y':  y, 'n_folds': 5}
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=15, **kwargs)
            # ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON
            h2o_glm.simpleCheckGLM(self, glm, 'AGE', **kwargs)
            sys.stdout.write('.')
            sys.stdout.flush() 

        # now redo it all thru the browser
        # three times!
        for i in range(3):
            h2b.browseJsonHistoryAsUrl()

        h2o.nodes[0].log_view()
        namelist = h2o.nodes[0].log_download()


if __name__ == '__main__':
    h2o.unit_main()
