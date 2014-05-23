import unittest, re, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1) 
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1) 
        global SYNDATASETS_DIR
        SYNDATASETS_DIR = h2o.make_syn_dir()

    @classmethod 
    def tearDownClass(cls): 
        h2o.tear_down_cloud()

    def test_many_parse1(self):
        rows = self.genrows1()
        set = 1
        self.tryThemAll(set,rows)

    def test_many_parse2(self):
        rows = self.genrows2()
        set = 2
        self.tryThemAll(set,rows)

    # this one has problems with blank lines
    def test_many_parse3(self):
        rows = self.genrows3()
        set = 3
        self.tryThemAll(set,rows)

    def genrows1(self):
        # comment has to have # in first column? (no leading whitespace)
        # FIX! what about blank fields and spaces as sep
        # FIX! temporary need more lines to avoid sample error in H2O
        # throw in some variants for leading 0 on the decimal, and scientific notation
        rows = [
        "# 'comment, is okay",
        '# "this comment, is okay too',
        "# 'this' comment, is okay too",
        "FirstName|MiddleInitials|LastName|DateofBirth",
        "0|0.5|1|0",
        "3|NaN|4|1",
        "6||8|0",
        "0.6|0.7|0.8|1",
        "+0.6|+0.7|+0.8|0",
        "-0.6|-0.7|-0.8|1",
        ".6|.7|.8|0",
        "+.6|+.7|+.8|1",
        "-.6|-.7|-.8|0",
        "+0.6e0|+0.7e0|+0.8e0|1",
        "-0.6e0|-0.7e0|-0.8e0|0",
        ".6e0|.7e0|.8e0|1",
        "+.6e0|+.7e0|+.8e0|0",
        "-.6e0|-.7e0|-.8e0|1",
        "+0.6e00|+0.7e00|+0.8e00|0",
        "-0.6e00|-0.7e00|-0.8e00|1",
        ".6e00|.7e00|.8e00|0",
        "+.6e00|+.7e00|+.8e00|1",
        "-.6e00|-.7e00|-.8e00|0",
        "+0.6e-01|+0.7e-01|+0.8e-01|1",
        "-0.6e-01|-0.7e-01|-0.8e-01|0",
        ".6e-01|.7e-01|.8e-01|1",
        "+.6e-01|+.7e-01|+.8e-01|0",
        "-.6e-01|-.7e-01|-.8e-01|1",
        "+0.6e+01|+0.7e+01|+0.8e+01|0",
        "-0.6e+01|-0.7e+01|-0.8e+01|1",
        ".6e+01|.7e+01|.8e+01|0",
        "+.6e+01|+.7e+01|+.8e+01|1",
        "-.6e+01|-.7e+01|-.8e+01|0",
        "+0.6e102|+0.7e102|+0.8e102|1",
        "-0.6e102|-0.7e102|-0.8e102|0",
        ".6e102|.7e102|.8e102|1",
        "+.6e102|+.7e102|+.8e102|0",
        "-.6e102|-.7e102|-.8e102|1",
        ]
        return rows
    
    #     "# comment here is okay",
    #     "# comment here is okay too",
    # FIX! needed an extra line to avoid bug on default 67+ sample?
    def genrows2(self):
        rows = [
        "FirstName|MiddleInitials|LastName|DateofBirth",
        "Kalyn|A.|Dalton|1967-04-01",
        "Gwendolyn|B.|Burton|1947-10-26",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31"
        ]
        return rows
    
    # update spec
    # intermixing blank lines in the first two lines breaks things
    # blank lines cause all columns except the first to get NA (red)
    # first may get a blank string? (not ignored)
    def genrows3(self):
        rows = [
        "# comment here is okay",
        "# comment here is okay too",
        "FirstName|MiddleInitials|LastName|DateofBirth",
        "Kalyn|A.|Dalton|1967-04-01",
        "",
        "Gwendolyn||Burton|1947-10-26",
        "",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        ]
        return rows


    # The 3 supported line-ends
    # FIX! should test them within quoted tokens
    eolDict = {
        0:"\n",
        1:"\r\n",
        2:"\r"
        }
    
    tokenChangeDict = {
        0:['  ','  '], # double space
        1:['  ',' '], # unbalanced space
        2:[' ','  '], # unblanced space3
        }
    
    def changeTokens(self,rows,tokenCase):
        [cOpen,cClose] = self.tokenChangeDict[tokenCase]
        newRows = []
        for r in rows:
            # don't quote lines that start with #
            # can quote lines start with some spaces or tabs? maybe
            comment = re.match(r'^[ \t]*#', r)
            empty = re.match(r'^$',r)
            if not (comment or empty):
                r = re.sub('^',cOpen,r)
                r = re.sub('\|',cClose + '|' + cOpen,r)
                r = re.sub('$',cClose,r)
            h2o.verboseprint(r)
            newRows.append(r)
        return newRows
    
    
    def writeRows(self,csvPathname,rows,eol):
        f = open(csvPathname, 'w')
        for r in rows:
            f.write(r + eol)
        # what about case of missing eoll at end of file?
    
    sepChangeDict = {
        # NEW: Hive datasets use 0x01 hex char as SEP, so now legal in our parser spec
        0:"",
        1:"  ", # double space
        2:" ",
        3:",",
        4:"\t",
        }
    
    def changeSep(self,rows,sepCase):
        # do a trial replace, to see if we get a <tab><sp> problem
        # comments at the beginning..get a good row
        r = rows[-1]
        tabseptab = re.search(r'\t|\t', r)
        spsepsp  = re.search(r' | ', r)

        # NOTE: we don't care about this because we don't have quoted strings
        # in this test. we do care in the other parse_many_cases test
        if 1==1:
            newSep = self.sepChangeDict[sepCase]
        else:
            if tabseptab or spsepsp:
                # use comma instead. always works
                # print "Avoided"
                newSep = ","
            else:
                newSep = self.sepChangeDict[sepCase]

        newRows = [r.replace('|',newSep) for r in rows]
        return newRows
    
    def tryThemAll(self,set,rows):
        for eolCase in range(len(self.eolDict)):
            eol = self.eolDict[eolCase]
            # change tokens must be first
            for tokenCase in range(len(self.tokenChangeDict)):
                newRows1 = self.changeTokens(rows,tokenCase)
                for sepCase in range(len(self.sepChangeDict)):
                    newRows2 = self.changeSep(newRows1,sepCase)
                    csvPathname = SYNDATASETS_DIR + '/parsetmp_' + \
                        str(set) + "_" + \
                        str(eolCase) + "_" + \
                        str(tokenCase) + "_" + \
                        str(sepCase) + \
                        '.data'
                    self.writeRows(csvPathname,newRows2,eol)
                    if "'" in self.tokenChangeDict[tokenCase]:
                        single_quotes = 1
                    else:
                        single_quotes = 0
                    parseResult = h2i.import_parse(path=csvPathname, schema='put', single_quotes=single_quotes,
                        noPrint=not h2o.verbose)

                    h2o_cmd.runRF(parseResult=parseResult, trees=1, timeoutSecs=30, retryDelaySecs=0.1)
                    h2o.verboseprint("Set", set)
                    sys.stdout.write('.')
                    sys.stdout.flush()
    
if __name__ == '__main__':
    h2o.unit_main()
