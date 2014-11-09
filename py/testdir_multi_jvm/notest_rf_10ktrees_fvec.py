import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(2)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_10ktrees_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        # always match the run below!
        # just using one file for now
        for x in [1000]:
            shCmdString = "perl " + h2o.find_file("syn_scripts/parity.pl") + " 128 4 "+ str(x) + " quad " + SYNDATASETS_DIR
            h2o.spawn_cmd_and_wait('parity.pl', shCmdString.split(),4)
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  

        # always match the gen above!
        for trial in range (1,3):
            sys.stdout.write('.')
            sys.stdout.flush()

            csvFilename = "parity_128_4_" + str(1000) + "_quad.data"  
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            hex_key = csvFilename + "_" + str(trial) + ".hex"
            parseResult = h2o_cmd.parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=30)

            h2o.verboseprint("Trial", trial)
            start = time.time()
            h2o_cmd.runRF(parseResult=parseResult, trees=10000, max_depth=2, timeoutSecs=900, retryDelaySecs=3)
            print "RF #", trial,  "end on ", csvFilename, 'took', time.time() - start, 'seconds'

        print "Waiting 60 secs for TIME_WAIT sockets to go away"
        time.sleep(60)

if __name__ == '__main__':
    h2o.unit_main()

