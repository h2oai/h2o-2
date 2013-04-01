import unittest
import re, os, shutil, sys, random, time
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
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3)
        else:
            h2o_hosts.build_cloud_with_hosts()

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
        for sizeTrial in range(25):
            size = random.randint(2,129)
            print "\nparsing with rows:", size
            csvFilename = "p" + "_" + str(size)
            csvPathname = SYNDATASETS_DIR + "/" + csvFilename
            writeRows(csvPathname,row,eol,size)
            key  = csvFilename
            pkey = node.put_file(csvPathname, key=key, timeoutSecs=timeoutSecs)
            print h2o.dump_json(pkey)
            
            trialMax = 100
            for trial in range(trialMax):
                key2 = csvFilename + "_" + str(trial) + ".hex"
                # just parse, without polling, except for last one..will that make prior ones complete too?
                noPoll = trial==(trialMax-1)
                node.parse(pkey, key2, timeoutSecs=timeoutSecs, retryDelaySecs=0.00, noPoll=noPoll)
                if not trial%10:
                    sys.stdout.write('.')
                    sys.stdout.flush()

            # do a storeview ..was causing npe while parsing?
            # maybe fire to each node?
            if 1==0:
                for node in h2o.nodes:
                    storeView = node.store_view()

        # and wait a minute to make sure all tcp_wait ports clear out
        print "Sleeping for 120 secs so the next jenkins job doesn't see all our tcp_wait ports"
        time.sleep(120)




if __name__ == '__main__':
    h2o.unit_main()
