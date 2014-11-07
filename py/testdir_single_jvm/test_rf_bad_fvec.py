import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i

datalines = """
# 'comment, is okay
# "this comment, is okay too
# 'this' comment, is okay too
@FirstName@ @Middle@Initials@ @LastName@ @Date@of@Birth@
0 0.5 1 0
3 NaN 4 1
6  8 0
0.6 0.7 0.8 1
+0.6 +0.7 +0.8 0
-0.6 -0.7 -0.8 1
.6 .7 .8 0
+.6 +.7 +.8 1
-.6 -.7 -.8 0
+0.6e0 +0.7e0 +0.8e0 1
-0.6e0 -0.7e0 -0.8e0 0
.6e0 .7e0 .8e0 1
+.6e0 +.7e0 +.8e0 0
-.6e0 -.7e0 -.8e0 1
+0.6e00 +0.7e00 +0.8e00 0
-0.6e00 -0.7e00 -0.8e00 1
.6e00 .7e00 .8e00 0
+.6e00 +.7e00 +.8e00 1
-.6e00 -.7e00 -.8e00 0
+0.6e-01 +0.7e-01 +0.8e-01 1
-0.6e-01 -0.7e-01 -0.8e-01 0
.6e-01 .7e-01 .8e-01 1
+.6e-01 +.7e-01 +.8e-01 0
-.6e-01 -.7e-01 -.8e-01 1
+0.6e+01 +0.7e+01 +0.8e+01 0
-0.6e+01 -0.7e+01 -0.8e+01 1
.6e+01 .7e+01 .8e+01 0
+.6e+01 +.7e+01 +.8e+01 1
-.6e+01 -.7e+01 -.8e+01 0
"""

datalines2 = """ FirstName , Middle Initials , LastName , Date of Birth   
0,0.5,1,0
3,0,4,1
6,,8,0
6,,8,0
6,,8,0
6,,8,0
6,,8,0
6,,8,0
6,,8,0
0.6,0.7,0.8,1
+0.6,+0,7,+0.8,0
-0.6,-0.7,-0.8,1
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

    def test_badrf(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvPathname = SYNDATASETS_DIR + '/badrf.data'
        dsf = open(csvPathname, "w+")
        dsf.write(datalines)
        dsf.close()

        for i in range(5):
            parseResult = h2i.import_parse(path=csvPathname, schema='put')
            h2o_cmd.runRF(parseResult=parseResult, trees=1,
                timeoutSecs=10, retryDelaySecs=0.1, noPrint=True)

if __name__ == '__main__':
    h2o.unit_main()
                        
