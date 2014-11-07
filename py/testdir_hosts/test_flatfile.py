import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o_cmd, h2o, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(2,use_flatfile=True)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_Basic(self):
        h2o.verify_cloud_size()

    def test_B_RF_iris2(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='iris/iris2.csv', schema='put')
        h2o_cmd.runRF(parseResult=parseResult, trees=6, timeoutSecs=10)

    def test_C_RF_poker100(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='poker/poker100', schema='put')
        h2o_cmd.runRF(parseResult=parseResult, trees=6, timeoutSecs=10)

        SYNDATASETS_DIR = h2o.make_syn_dir()
        # always match the run below!
        for x in xrange (11,100,10):
            # Have to split the string out to list for pipe
            shCmdString = "perl " + h2o.find_file("syn_scripts/parity.pl") + " 128 4 "+ str(x) + " quad " + SYNDATASETS_DIR
            h2o.spawn_cmd_and_wait('parity.pl', shCmdString.split(),timeout=30)
            # the algorithm for creating the path and filename is hardwired in parity.pl..i.e
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  

        trees = 6
        timeoutSecs = 60
        # always match the gen above!
        # reduce to get intermittent failures to lessen, for now
        for x in xrange (11,60,10):
            sys.stdout.write('.')
            sys.stdout.flush()
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            parseResult = h2i.import_parse(path=csvPathname, schema='put')
            h2o_cmd.runRF(parseResult=parseResult, ntrees=trees, timeoutSecs=timeoutSecs)
            trees += 10

if __name__ == '__main__':
    h2o.unit_main()
