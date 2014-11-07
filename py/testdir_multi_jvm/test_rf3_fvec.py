import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(3, java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf3_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        # always match the run below!
        for x in [10000]:
            # Have to split the string out to list for pipe
            shCmdString = "perl " + h2o.find_file("syn_scripts/parity.pl") + " 128 4 "+ str(x) + " quad " + SYNDATASETS_DIR
            h2o.spawn_cmd_and_wait('parity.pl', shCmdString.split(),4)
            # the algorithm for creating the path and filename is hardwired in parity.pl..i.e
            csvFilename = "parity_128_4_" + str(x) + "_quad.data"  

        # always match the gen above!
        trial = 1
        for x in range (1):
            sys.stdout.write('.')
            sys.stdout.flush()

            # just use one file for now
            csvFilename = "parity_128_4_" + str(10000) + "_quad.data"  
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            # broke out the put separately so we can iterate a test just on the RF
            parseResult = h2i.import_parse(path=csvPathname, schema='put', pollTimeoutSecs=60, timeoutSecs=60)

            h2o.verboseprint("Trial", trial)
            h2o_cmd.runRF(parseResult=parseResult, trees=237, max_depth=45, timeoutSecs=480)

            # don't change tree count yet
            ## trees += 10
            ### timeoutSecs += 2
            trial += 1

if __name__ == '__main__':
    h2o.unit_main()
