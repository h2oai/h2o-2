import unittest
import re, os, shutil, sys, random
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts

def writeRows(csvPathname,row,eol,repeat):
    f = open(csvPathname, 'w')
    for r in range(repeat):
        f.write(row + eol)

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

    def test_A_parse_small_many(self):
        SEED = 6204672511291494176
        random.seed(SEED)
        print "\nUsing random seed:", SEED

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
        for sizeTrial in range(7):
            size = random.randint(2,129)
            print "\nparsing with rows:", size
            csvFilename = "p" + "_" + str(size)
            csvPathname = SYNDATASETS_DIR + "/" + csvFilename
            writeRows(csvPathname,row,eol,size)
            key  = csvFilename
            pkey = node.put_file(csvPathname, key=key, timeoutSecs=timeoutSecs)
            print h2o.dump_json(pkey)
            for trial in range(5):
                key2 = csvFilename + "_" + str(trial) + ".hex"
                # just parse
                node.parse(pkey, key2, timeoutSecs=timeoutSecs, retryDelaySecs=0.00)
                sys.stdout.write('.')
                sys.stdout.flush()

if __name__ == '__main__':
    h2o.unit_main()
