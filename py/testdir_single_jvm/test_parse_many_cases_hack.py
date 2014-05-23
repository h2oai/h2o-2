import unittest, re, sys, random, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i

print "the last line with the blank field (NA) seems to be causing the problem"
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

    def test_A_many_parse1(self):
        rows = self.genrows1()
        tset = 1
        self.tryThemAll(tset,rows)

    def test_B_many_parse2(self):
        rows = self.genrows2()
        tset = 2
        self.tryThemAll(tset,rows)

    # this one has problems with blank lines
    def test_C_many_parse3(self):
        rows = self.genrows3()
        tset = 3
        self.tryThemAll(tset,rows)

    def genrows1(self):
        # comment has to have # in first column? (no leading whitespace)
        # FIX! what about blank fields and spaces as sep
        # FIX! temporary need more lines to avoid sample error in H2O
        # throw in some variants for leading 0 on the decimal, and scientific notation
        # new: change the @ to an alternate legal SEP if the special HIVE SEP is in play
        rows = [
        "# 'comment, is okay",
        '# "this comment, is okay too',
        "# 'this' comment, is okay too",
        "@FirstName@|@Middle@Initials@|@LastName@|@Date@of@Birth@ ",
        "0|0.5|1|0",
        # "3|NaN|4|1",
        "3|0|4|1",
        # "6||8|0",
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
         "6||8|0",
        ]
        return rows
    
    #     "# comment here is okay",
    #     "# comment here is okay too",
    # FIX! needed an extra line to avoid bug on default 67+ sample?
    def genrows2(self):
        rows = [
        "First@Name|@MiddleInitials|LastName@|Date@ofBirth",
        "Kalyn|A.|Dalton|1967-04-01",
        "Gwendolyn|B.|Burton|1947-10-26",
        "Elodia|G.|Ali|1983-10-31",
        "Elo@dia|@G.|Ali@|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31"
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31"
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31"
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31"
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
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
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
        "Elodia|G.|Ali|1983-10-31",
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
    
    # tab here will cause problems too?
    #    5:['"\t','\t"'],
    #    8:["'\t","\t'"]
    tokenChangeDict = {
        0:['',''],
        1:['\t','\t'],
        2:[' ',' '],
        3:['"','"'],
        4:['" ',' "'],
        5:["'","'"],
        6:["' "," '"],
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
        # NEW: 0x01 can be SEP character for Hive datasets
        0:"",
        1:",",
        2:" ",
        3:"\t",
        }
    
    def changeSep(self,rows,sepCase):
        # do a trial replace, to see if we get a <tab><sp> problem
        # comments at the beginning..get a good row
        r = rows[-1]
        tabseptab = re.search(r'\t|\t', r)
        spsepsp  = re.search(r' | ', r)

        if tabseptab or spsepsp:
            # use comma instead. always works
            # print "Avoided"
            newSep = ","
        else:
            newSep = self.sepChangeDict[sepCase]

        newRows = [r.replace('|',newSep) for r in rows]

        # special case, if using the HIVE sep, substitute randomly
        # one of the other SEPs into the "@" in the template
        # FIX! we need to add HIVE lineends into lineend choices.
        # assuming that lineend
        if newSep == "":
            # don't use the same SEP to swap in.
            randomOtherSep = random.choice(self.sepChangeDict.values())
            while (randomOtherSep==newSep):
                randomOtherSep = random.choice(self.sepChangeDict.values())
            newRows = [r.replace('@',randomOtherSep) for r in newRows]

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

                    # use the single_quotes param if single quote in the tokenCase (creates token wrapper)
                    if "'" in self.tokenChangeDict[tokenCase]:
                        single_quotes = 1
                    else:
                        single_quotes = 0
                    parseResult = h2i.import_parse(path=csvPathname, schema='local', single_quotes=single_quotes,
                        noPrint=not h2o.verbose)
                    inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
                    print "\n" + csvPathname, \
                        "    num_rows:", "{:,}".format(inspect['num_rows']), \
                        "    num_cols:", "{:,}".format(inspect['num_cols'])
                    num_rows = inspect['num_rows']
                    num_cols = inspect['num_cols']
                    self.assertEqual(num_cols, 4, "Parsed wrong number of cols: %s" % num_cols)
                    self.assertEqual(num_rows, 29, "Parsed wrong number of rows: %s" % num_rows)

                    h2o_cmd.runRF(parseResult=parseResult, trees=1, 
                        timeoutSecs=10, retryDelaySecs=1.0, noPrint=True)
                    h2o.verboseprint("Set", set)
                    h2o.check_sandbox_for_errors()
                    sys.stdout.write('.')
                    sys.stdout.flush()
    
if __name__ == '__main__':
    h2o.unit_main()
