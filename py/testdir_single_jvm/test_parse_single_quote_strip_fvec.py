import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_browse as h2b

print "h2o should automatically strip the single quotes if we say single_quotes=1"
print "should probably check the names too ..right now failing on rows"
# failure: the single quotes are in the Inspect json, as \u0027
# like this:
# 
# so that can't be right?
# 
#       "row": 0,
#       "C1": "\u0027 FirstName \u0027",
#       "C2": "\u0027 Middle Initials \u0027",
#       "C3": "\u0027 LastName \u0027",
#       "C4": "\u0027 Date of Birth  \u0027"

# 'comment, is okay
# "this comment, is okay too
# 'this' comment, is okay too
datalines = """' FirstName '' Middle Initials '' LastName '' Date of Birth  '
'0''0.5''1''0'
'3''0''4''1'
'0.6''0.7''0.8''1'
'+0.6''+0.7''+0.8''0'
'-0.6''-0.7''-0.8''1'
'.6''.7''.8''0'
'+.6''+.7''+.8''1'
'-.6''-.7''-.8''0'
'+0.6e0''+0.7e0''+0.8e0''1'
'-0.6e0''-0.7e0''-0.8e0''0'
'.6e0''.7e0''.8e0''1'
'+.6e0''+.7e0''+.8e0''0'
'-.6e0''-.7e0''-.8e0''1'
'+0.6e00''+0.7e00''+0.8e00''0'
'-0.6e00''-0.7e00''-0.8e00''1'
'.6e00''.7e00''.8e00''0'
'+.6e00''+.7e00''+.8e00''1'
'-.6e00''-.7e00''-.8e00''0'
'+0.6e-01''+0.7e-01''+0.8e-01''1'
'-0.6e-01''-0.7e-01''-0.8e-01''0'
'.6e-01''.7e-01''.8e-01''1'
'+.6e-01''+.7e-01''+.8e-01''0'
'-.6e-01''-.7e-01''-.8e-01''1'
'+0.6e+01''+0.7e+01''+0.8e+01''0'
'-0.6e+01''-0.7e+01''-0.8e+01''1'
'.6e+01''.7e+01''.8e+01''0'
'+.6e+01''+.7e+01''+.8e+01''1'
'-.6e+01''-.7e+01''-.8e+01''0'
'6''''8''0'
"""


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_single_quote_strip_fvec(self):
        # h2b.browseTheCloud()
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvPathname = SYNDATASETS_DIR + '/bad.data'
        dsf = open(csvPathname, "w+")
        dsf.write(datalines)
        dsf.close()

        for i in range(5):
            parseResult = h2i.import_parse(path=csvPathname, schema='put', single_quotes=1,
                hex_key="trial" + str(i) + ".hex")
            inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
            print "\n" + csvPathname, \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])
            numRows = inspect['numRows']
            numCols = inspect['numCols']

            self.assertEqual(numCols, 4, "Parsed wrong number of cols: %s" % numCols)
            self.assertNotEqual(numRows, 30, "Parsed wrong number of rows. Should be 29.\
                 Didn't deduce header?: %s" % numRows)
            self.assertEqual(numRows, 29, "Parsed wrong number of rows: %s" % numRows)


if __name__ == '__main__':
    h2o.unit_main()
                        
