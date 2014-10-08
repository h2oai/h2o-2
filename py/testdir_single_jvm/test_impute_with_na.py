import unittest, time, sys, random 
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_import as h2i, h2o_jobs, h2o_exec as h2e, h2o_util

print "Put some NAs in covtype then impute with the 3 methods"
print "Don't really understand the group_by. Randomly put some columns in there"

DO_POLL = False
AVOID_BUG = True
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        global SEED
        SEED = h2o.setup_random_seed()

        if (localhost):
            h2o.build_cloud(java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_impute_with_na(self):
        csvFilename = 'covtype.data'
        csvPathname = 'standard/' + csvFilename
        hex_key = "covtype.hex"
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, hex_key=hex_key, schema='local', timeoutSecs=20)

        print "Just insert some NAs and see what happens"
        inspect = h2o_cmd.runInspect(key=hex_key)
        origNumRows = inspect['numRows']
        origNumCols = inspect['numCols']
        missing_fraction = 0.1



        # NOT ALLOWED TO SET AN ENUM COL?
        if 1==0:
            # since insert missing values (below) doesn't insert NA into enum rows, make it NA with exec?
            # just one in row 1
            for enumCol in enumColList:
                print "hack: Putting NA in row 0 of col %s" % enumCol
                execExpr = '%s[1, %s+1] = NA' % (hex_key, enumCol)
                h2e.exec_expr(execExpr=execExpr, timeoutSecs=10)

            inspect = h2o_cmd.runInspect(key=hex_key)
            missingValuesList = h2o_cmd.infoFromInspect(inspect)
            print "missingValuesList after exec:", missingValuesList
            if len(missingValuesList) != len(enumColList):
                raise Exception ("Didn't get missing values in expected number of cols: %s %s" % (enumColList, missingValuesList))


        for trial in range(5):
            # copy the dataset
            hex_key2 = 'c.hex'
            execExpr = '%s = %s' % (hex_key2, hex_key)
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=10)

            imvResult = h2o.nodes[0].insert_missing_values(key=hex_key2, missing_fraction=missing_fraction, seed=SEED)
            print "imvResult", h2o.dump_json(imvResult)
            # maybe make the output col a factor column
            # maybe one of the 0,1 cols too? 
            # java.lang.IllegalArgumentException: Method `mode` only applicable to factor columns.
            # ugh. ToEnum2 and ToInt2 take 1-based column indexing. This should really change back to 0 based for h2o-dev? (like Exec3)

            print "Doing the ToEnum2 AFTER the NA injection, because h2o doesn't work right if we do it before"
            expectedMissing = missing_fraction * origNumRows # per col
            enumColList = [49, 50, 51, 52, 53, 54] 
            enumResult = h2o.nodes[0].to_enum(src_key=hex_key2, column_index=(54+1))
            enumResult = h2o.nodes[0].to_enum(src_key=hex_key2, column_index=(53+1))
            enumResult = h2o.nodes[0].to_enum(src_key=hex_key2, column_index=(52+1))
            enumResult = h2o.nodes[0].to_enum(src_key=hex_key2, column_index=(51+1))
            enumResult = h2o.nodes[0].to_enum(src_key=hex_key2, column_index=(50+1))
            enumResult = h2o.nodes[0].to_enum(src_key=hex_key2, column_index=(49+1))

            inspect = h2o_cmd.runInspect(key=hex_key2)
            numRows = inspect['numRows']
            numCols = inspect['numCols']
            self.assertEqual(origNumRows, numRows)
            self.assertEqual(origNumCols, numCols)

            missingValuesList = h2o_cmd.infoFromInspect(inspect)
            print "missingValuesList", missingValuesList
            if len(missingValuesList) != numCols:
                raise Exception ("Why is missingValuesList not right afer ToEnum2?: %s %s" % (enumColList, missingValuesList))

            expected = .1 * numRows
            for mv in missingValuesList:
                self.assertAlmostEqual(mv, expectedMissing, delta=0.1 * mv, msg='mv %s is not approx. expected %s' % (mv, expectedMissing))

            summaryResult = h2o_cmd.runSummary(key=hex_key2)
            h2o_cmd.infoFromSummary(summaryResult)
            # h2o_cmd.infoFromSummary(summaryResult)

            print "I don't understand why the values don't increase every iteration. It seems to stay stuck with the first effect"
            print "trial", trial
            print "expectedMissing:", expectedMissing

            print "Now get rid of all the missing values, but imputing means. We know all columns should have NAs from above"
            print "Do the columns in random order"


            shuffledColList = range(0,55)
            random.shuffle(shuffledColList)

            for column in shuffledColList: 
                # get a random set of column. no duplicate. random order? 0 is okay? will be []
                groupBy = random.sample(range(55), random.randint(0, 54))
                # header names start with 1, not 0. Empty string if []
                groupByNames = ",".join(map(lambda x: "C" + str(x+1), groupBy))

                # what happens if column and groupByNames overlap?? Do we loop here and choose until no overlap
                columnName = "C%s" % (column + 1) 
                print "don't use mode if col isn't enum"
                badChoices = True
                while badChoices:
                    method = random.choice(["mean", "median", "mode"])
                    badChoices = column not in enumColList and method=="mode"

                NEWSEED = random.randint(0, sys.maxint)
                print "does impute modify the source key?"
                # we get h2o error (argument exception) if no NAs
                impResult = h2o.nodes[0].impute(source=hex_key2, column=column, method=method)

            print "Now check that there are no missing values"
            print "FIX! broken..insert missing values doesn't insert NAs in enum cols"

            inspect = h2o_cmd.runInspect(key=hex_key2)
            numRows2 = inspect['numRows']
            numCols2 = inspect['numCols']
            self.assertEqual(numRows, numRows2, "imput shouldn't have changed frame numRows: %s %s" % (numRows, numRows2))
            self.assertEqual(numCols, numCols2, "imput shouldn't have changed frame numCols: %s %s" % (numCols, numCols2))

            missingValuesList = h2o_cmd.infoFromInspect(inspect)
            print "missingValuesList after impute:", missingValuesList
            if missingValuesList:
                raise Exception ("Not expecting any missing values after imputing all cols: %s" % missingValuesList)

if __name__ == '__main__':
    h2o.unit_main()
