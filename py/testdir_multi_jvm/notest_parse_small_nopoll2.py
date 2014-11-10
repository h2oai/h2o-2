import unittest, sys, random, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i

def writeRows(csvPathname,row,eol,repeat):
    f = open(csvPathname, 'w')
    for r in range(repeat):
        f.write(row + eol)

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(3)

    @classmethod 
    def tearDownClass(cls): 
        h2o.tear_down_cloud()

    def test_parse_small_nopoll2(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        # can try the other two possibilities also
        eol = "\n"
        row = "a,b,c,d,e,f,g"

        # need unique key name for upload and for parse, each time
        # maybe just upload it once?
        timeoutSecs = 10
        node = h2o.nodes[0]

        # fail rate is one in 200?
        # need at least two rows (parser)
        for sizeTrial in range(25):
            size = random.randint(2,129)
            print "\nparsing with rows:", size
            csvFilename = "p" + "_" + str(size)
            csvPathname = SYNDATASETS_DIR + "/" + csvFilename
            writeRows(csvPathname,row,eol,size)
            
            trialMax = 100
            for trial in range(trialMax):
                # have to repeat the put because h2o deletes the source key now after parse
                hex_key = csvFilename + "_" + str(trial) + ".hex"
                # just parse, without polling, except for last one..will that make prior ones complete too?
                noPoll = trial!=(trialMax-1)
                parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=30)

                if not trial%10:
                    sys.stdout.write('.')
                    sys.stdout.flush()

                # do a storeview to each node
                for node in h2o.nodes:
                    storeView = node.store_view()

        # and wait a minute to make sure all tcp_wait ports clear out
        print "Sleeping for 120 secs so the next jenkins job doesn't see all our tcp_wait ports"
        time.sleep(120)

if __name__ == '__main__':
    h2o.unit_main()
