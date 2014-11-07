import unittest, re, sys, random
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
        # SEED = h2o.setup_random_seed()
        SEED = 6204672511291494176
        h2o.init(1)

    @classmethod 
    def tearDownClass(cls): 
        h2o.tear_down_cloud()

    def test_parse_small_many_fvec(self):
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
        for sizeTrial in range(10):
            size = random.randint(2,129)
            print "\nparsing with rows:", size
            csvFilename = "p" + "_" + str(size)
            csvPathname = SYNDATASETS_DIR + "/" + csvFilename
            writeRows(csvPathname,row,eol,size)
            src_key = csvFilename
            for trial in range(5):
                hex_key = csvFilename + "_" + str(trial) + ".hex"
                parseResult = h2i.import_parse(path=csvPathname, schema='put', src_key=src_key, hex_key=hex_key)

                sys.stdout.write('.')
                sys.stdout.flush()

if __name__ == '__main__':
    h2o.unit_main()
