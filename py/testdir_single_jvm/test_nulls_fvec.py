import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o_cmd, h2o, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init()


    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_nulls_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        # we're going to insert <NUL> (0x0) in between every byte!
        # and then use it. move to a large file. I suppose
        # we could compare the results to a non-munged file with the same algo
        # I suppose the <NUL> are thrown away by parse, so doesn't change
        # chunk boundary stuff. (i.e. not interesting test for RF)
        csvFilename = 'poker1000'
        csvPathname = 'poker/' + csvFilename
        fullPathname = h2i.find_folder_and_filename('smalldata', csvPathname, returnFullPath=True)

        nulFilename = "syn_nul.data"
        nulPathname = SYNDATASETS_DIR + '/' + nulFilename

        piece_size = 4096 # 4 KiB

        with open(fullPathname, "rb") as in_file:
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

                parseResult = h2i.import_parse(path=nulPathname, schema='put')
                h2o_cmd.runRF(parseResult=parseResult, trees=trees, destination_key=model_key, timeoutSecs=timeoutSecs, retryDelaySecs=1)
                sys.stdout.write('.')
                sys.stdout.flush()

                # partial clean, so we can look at tree builds from this run if hang
                h2o.clean_sandbox_stdout_stderr()
                
if __name__ == '__main__':
    h2o.unit_main()

