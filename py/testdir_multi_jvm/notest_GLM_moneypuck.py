import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_glm, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(2,java_heap_GB=5)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_moneypuck(self):
        if 1==1:
            # None is okay for hex_key
            csvFilenameList = [
                # ('hdb-2007-02-05/Goalies.csv',240,'Goalies'),
                # ('hdb-2007-02-05/GoaliesSC.csv',240,'GoaliesSC'),
                # ('hdb-2007-02-05/Master.csv',240,'Master'),
                ('hdb-2007-02-05/Scoring.csv',240,'Scoring'),
                ('hdb-2007-02-05/ScoringSC.csv',240,'ScoringSC'),
                ('hdb-2007-02-05/Teams.csv',240,'Teams'),
                ('hdb-2007-02-05/TeamsHalf.csv',240,'TeamsHalf'),
                ('hdb-2007-02-05/TeamsPost.csv',240,'TeamsPost'),
                ('hdb-2007-02-05/TeamsSC.csv',240,'TeamsSC'),
                ('tricks-2012-06-23/HatTricks.csv',240,'HatTricks'),
                ('bkb090621/abbrev.csv',240,'abbrev'),
                ('bkb090621/AwardsCoaches.csv',240,'AwardsCoaches'),
                ('bkb090621/AwardsPlayers.csv',240,'AwardsPlayers'),
                ('bkb090621/Coaches.csv',240,'Coaches'),
                # never finishes?
                # ('bkb090621/Draft.csv',240,'Draft'),
                # ('bkb090621/Master.csv',240,'Master'),
                ('bkb090621/PlayersAllstar.csv',240,'PlayersAllstar'),
                ('bkb090621/Players.csv',240,'Players'),
                ('bkb090621/PlayersPlayoffs.csv',240,'PlayersPlayoffs'),
                ('bkb090621/Teams.csv',240,'Teams'),
                ('hdb-2007-02-05/abbrev.csv',240,'abbrev'),
                # SPD without regularization
                # can't solve, when regularization added
                # ('hdb-2007-02-05/AwardsCoaches.csv',240,'AwardsCoaches'),
                # ('hdb-2007-02-05/AwardsMisc.csv',240,'AwardsMisc'),
                ('hdb-2007-02-05/AwardsPlayers.csv',240,'AwardsPlayers'),
                # can't solve, when regularization added
                # ('hdb-2007-02-05/Coaches.csv',240,'Coaches'),
                ]

        # a browser window too, just because we can
        h2b.browseTheCloud()

        importFolderPath = "hockey"
        for csvFilename, timeoutSecs, hex_key in csvFilenameList:
            # creates csvFilename.hex from file in importFolder dir 
            csvPathname = importFolderPath + "/" + csvFilename
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, timeoutSecs=2000, hex_key=hex_key)
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            numRows = inspect['numRows']
            numCols = inspect['numCols']

            print "\n" + csvPathname, \
                "    numRows:", "{:,}".format(numRows), \
                "    numCols:", "{:,}".format(numCols)

            max_iter = 9
            # assume the last col is the output!
            y = numCols-1
            kwargs = {
                'y': y, 
                'family': 'poisson',
                'link': 'log',
                'n_folds': 0, 
                'max_iter': max_iter, 
                'beta_epsilon': 1e-3}

            # L2 
            if 1==0:
                kwargs.update({'alpha': 0, 'lambda': 0})
                start = time.time()
                glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
                print "glm (L2) end on ", csvPathname, 'took', time.time() - start, 'seconds'
                # assume each one has a header and you have to indirect thru 'column_names'
                column_names = glm['GLMModel']['column_names']
                print "column_names[0]:", column_names[0]
                h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
                h2b.browseJsonHistoryAsUrlLastMatch("GLM")


            # Elastic
            kwargs.update({'alpha': 0.5, 'lambda': 1e-4})
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            print "glm (Elastic) end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

            # L1
            kwargs.update({'alpha': 1.0, 'lambda': 1e-4})
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            print "glm (L1) end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)


if __name__ == '__main__':
    h2o.unit_main()
