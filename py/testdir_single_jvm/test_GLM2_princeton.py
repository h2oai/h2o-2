import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_util, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1)
        global SYNDATASETS_DIR
        SYNDATASETS_DIR = h2o.make_syn_dir()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_princeton(self):
        # filename, y, timeoutSecs
        # these are all counts? using gaussian?
        csvFilenameList = [
            ('cuse.dat', 'gaussian', 3, 10), # notUsing
            ('cuse.dat', 'gaussian', 4, 10), # using
            ('copen.dat', 'gaussian', 4, 10),
            ('housing.raw', 'gaussian', 4, 10),
            ]

        trial = 0
        for (csvFilename, family, y, timeoutSecs) in csvFilenameList:
            csvPathname1 = 'logreg/princeton/' + csvFilename
            fullPathname1 = h2i.find_folder_and_filename('smalldata', csvPathname1, returnFullPath=True)
            csvPathname2 = SYNDATASETS_DIR + '/' + csvFilename + '_stripped.csv'
            h2o_util.file_strip_trailing_spaces(fullPathname1, csvPathname2)

            parseResult = h2i.import_parse(path=csvPathname2, schema='put', timeoutSecs=timeoutSecs)
            start = time.time()
            kwargs = {'n_folds': 0, 'family': family, 'response': y}
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "glm end (w/check) on ", csvPathname2, 'took', time.time() - start, 'seconds'
            trial += 1
            print "\nTrial #", trial

if __name__ == '__main__':
    h2o.unit_main()
