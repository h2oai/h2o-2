import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o_cmd, h2o, h2o_hosts

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o_hosts.build_cloud_with_hosts(use_hdfs=False)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_file_with_nul_chars_inserted(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        # we're going to insert <NUL> (0x0) in between every byte!
        # and then use it. move to a large file. I suppose
        # we could compare the results to a non-munged file with the same algo
        # I suppose the <NUL> are thrown away by parse, so doesn't change
        # chunk boundary stuff. (i.e. not interesting test for RF)
        csvFilename = 'poker1000'
        csvPathname = h2o.find_file('smalldata/poker/' + csvFilename)
        nulFilename = "syn_nul.data"
        nulPathname = SYNDATASETS_DIR + '/' + nulFilename

        piece_size = 4096 # 4 KiB

        with open(csvPathname, "rb") as in_file:
            with open(nulPathname, "wb") as out_file:
                while True:
                    piece = in_file.read(103)
                    if piece == "":
                        break # end of file

                    # we could just extend piece?
                    # start with a null
                    withNuls = bytearray(piece)
                    # FIX! we'll eventually stick a <NUL> after every byte!
                    withNuls.extend(bytearray.fromhex('00'))
                    out_file.write(withNuls)


        for trials in xrange(1,2):
            trees = 6
            for x in xrange (161,240,40):
                y = 10000 * x
                print "\nTrial:", trials, ", y:", y

                timeoutSecs = 20 + 5*(len(h2o.nodes))
                
                model_key = csvFilename + "_" + str(trials)
                h2o_cmd.runRF(trees=trees, model_key=model_key, csvPathname=nulPathname,
                    timeoutSecs=timeoutSecs, retryDelaySecs=1)
                sys.stdout.write('.')
                sys.stdout.flush()

                # partial clean, so we can look at tree builds from this run if hang
                h2o.clean_sandbox_stdout_stderr()
                
if __name__ == '__main__':
    h2o.unit_main()

