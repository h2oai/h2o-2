import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts
import h2o_browse as h2b

# ord('a') gives 97. Use that when you pass it as url param to h2o
# str(unichr(97)) gives 'a'

print "Maybe a rows count problem if using tabs? temporary only tabs"
print "passes if only , is used"
print "just do one file with tab"
paramsDict = {
    # don't worry about these variants in this test (just parse normal)
    # 'parser_type': [None, 'AUTO', 'XLS', 'XLSX', 'SVMLight'],
    # I suppose, libsvm could have strangeness!..but there is no header with libsvm?
    'parser_type': [None, 'AUTO'],
    # gets used for separator=
    # 'separator': [",", " ", "\t"],
    # 'separator': [",", " "],
    'separator': ["\t"],
    'header': [None, 0,1],
    # we can point to the 'wrong' file!
    # assume this is always used, otherwise we sum data rows without knowing if we'll use the header file?
    # always point to the header file..again, if we switch it around, the counts are off
    'header_from_file': ['syn_header'],
    # 'header_from_file': [None],
}

# ability to selectively comment the first line (which may be data or header)
# Does not write an extra line as the comment first in that case (header can be picked from the comment line)
# H2O always just strips the comment from the first line? and continues parsing?

# Don't write headerString if None (for non-header files)
# Don't write data if rowCount is None
def write_syn_dataset(csvPathname, rowCount, headerString, rList, commentFirst=False, sepChar=','):
    commentDone = False
    dsf = open(csvPathname, "w+")

    # this should never add to the actual count of data rows
    # UPDATE: comments are always ignored. So a commented header, commented data, is ignored.
    headerRowsDone = 0
    if headerString is not None:
        if commentFirst and not commentDone:
            h = "# " + headerString
            commentDone = True
        # FIX: for now, always put the header in even if we're told to!
        h = headerString
        headerRowsDone += 1
        dsf.write(h + "\n")
    
    if rowCount is not None:        
        for i in range(rowCount):
            # two choices on the input. Make output choices random
            r = rList[random.randint(0,1)] + sepChar + str(random.randint(0,7))
            if commentFirst and not commentDone:
                # if commented, then write it twice, so the count of rows done is what we expect
                # the commented row will be ignored 
                dsf.write("# " + r + "\n")
                commentDone = True
            dsf.write(r + "\n")
        dsf.close()
        return (headerRowsDone, rowCount) # rows done
    else:
        return (headerRowsDone, 0) # rows done

def rand_rowData(colCount, sepChar):
    rowData = [random.randint(0,7) for i in range(colCount)]
    rowData1= sepChar.join(map(str,rowData))
    rowData = [random.randint(0,7) for i in range(colCount)]
    rowData2= sepChar.join(map(str,rowData))
    # RF will complain if all inputs are the same
    r = [rowData1, rowData2]
    return r

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        # SEED = h2o.setup_random_seed(8968685305521902318)

        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2,java_heap_MB=1300,use_flatfile=True)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
    
    def test_parse_multi_header_rand(self):
        h2b.browseTheCloud()
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn_ints.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        headerList = [
            # consistent with A, so we can decide when A should have been used (see below)
            ['aA','aB','aC','aD','aE','aF','aG','aH','aI','output'],
            # FIX! don't mess with the header size to data size now (stack trace
            # ['A','B2','C2','D2','E2','F2','G2','H2','I2','output2'],
            # ['A','B','C','D','E','F','G','H','I'],
            # ['A','B','C','D','E','F','G','H','I','output','junk'],
        ]

        # cols must be 9 to match the header above, otherwise a different bug is hit
        # extra output is added, so it's 10 total
        tryList = [
            # FIX! one fails count for now
            # (1, 5, 9, 'cA', 60, 0),
            (1, 5, 9, 'cA', 60, 0),

            # try with col mismatch on header. 
            # FIX! causes exception? don't test for now
            # (7, 300, 10, 'cA', 60, 0),
            # (7, 300, 10, 'cB', 60, 1),
            # (7, 300, 10, 'cC', 60, 2),
            # (7, 300, 10, 'cD', 60, 3),

            # (7, 300, 8, 'cA', 60, 0),
            # (7, 300, 8, 'cB', 60, 1),
            # (7, 300, 8, 'cC', 60, 2),
            # (7, 300, 8, 'cD', 60, 3),
            ]

        # so many random combos..rather than walk tryList, just do random for some amount of time
        for trial in range(20):
            (fileNum, rowCount, colCount, key2, timeoutSecs, dataRowsWithHeader) = random.choice(tryList)
            print fileNum, rowCount, colCount, key2, timeoutSecs, dataRowsWithHeader
            # FIX! should we add a header to them randomly???
            print "Wait while", fileNum, "synthetic files are created in", SYNDATASETS_DIR
            rowxcol = str(rowCount) + 'x' + str(colCount)
            totalCols = colCount + 1 # 1 extra for output
            totalDataRows = 0
            totalHeaderRows = 0

            HEADER_HAS_HEADER = random.randint(0,1)
            DATA_HAS_HEADER = random.randint(0,1)
            ## DATA_FIRST_IS_COMMENT = random.randint(0,1)
            ## HEADER_FIRST_IS_COMMENT = random.randint(0,1)
            print "TEMPORARY: don't put any comments in"
            DATA_FIRST_IS_COMMENT = 0
            HEADER_FIRST_IS_COMMENT = 0
            # none is not legal
            SEP_CHAR_GEN = random.choice(paramsDict['separator'])
            
            print '\nHEADER_HAS_HEADER:', HEADER_HAS_HEADER
            print 'DATA_HAS_HEADER:', DATA_HAS_HEADER
            print 'DATA_FIRST_IS_COMMENT:', DATA_FIRST_IS_COMMENT
            print 'HEADER_FIRST_IS_COMMENT:', HEADER_FIRST_IS_COMMENT
            print 'SEP_CHAR_GEN:', SEP_CHAR_GEN

            # they need to both use the same separator (h2o rule)
            headerForHeader = SEP_CHAR_GEN.join(random.choice(headerList))
            headerForData = SEP_CHAR_GEN.join(random.choice(headerList))

            # random selection of parse param choices
            kwargs = {}
            for k,v in paramsDict.items():
                aChoice = random.choice(v)
                # can tell h2o something different compared to what we actually used!
                if k == 'separator':
                    if aChoice: 
                        sepChar = aChoice
                        sepCharInt = ord(aChoice) # make it an integer for h2o
                    else:
                        sepChar = ',' # default char for None, need it for header/data file creation
                        sepCharInt = None
                    aChoice = sepCharInt

                kwargs[k] = aChoice

            # FOR NOW: ..override the rand choice if it exists, so we can parse and expect 'A' to be found
            # match what was gen'ed if choice is not None
            if kwargs['separator']:
                kwargs['separator'] = ord(SEP_CHAR_GEN)
        
            # create data files
            for fileN in range(fileNum):
                csvFilename = 'syn_data_' + str(fileN) + "_" + str(SEED) + "_" + str(trial) + "_" + rowxcol + '.csv'
                csvPathname = SYNDATASETS_DIR + '/' + csvFilename
                rList = rand_rowData(colCount, sepChar=SEP_CHAR_GEN)
                (headerRowsDone, dataRowsDone) = write_syn_dataset(csvPathname, rowCount, 
                    headerString=(headerForData if DATA_HAS_HEADER else None), rList=rList,
                    commentFirst=DATA_FIRST_IS_COMMENT, sepChar=SEP_CHAR_GEN)
                totalDataRows += dataRowsDone
                totalHeaderRows += headerRowsDone

            # create the header file
            hdrFilename = 'syn_header_' + str(SEED) + "_" + str(trial) + "_" + rowxcol + '.csv'
            hdrPathname = SYNDATASETS_DIR + '/' + hdrFilename
            dataRowsWithHeader = 0 # temp hack
            (headerRowsDone, dataRowsDone) = write_syn_dataset(hdrPathname, dataRowsWithHeader, 
                headerString=(headerForHeader if HEADER_HAS_HEADER else None), rList=rList,
                commentFirst=HEADER_FIRST_IS_COMMENT, sepChar=SEP_CHAR_GEN)
            totalDataRows += dataRowsDone
            totalHeaderRows += headerRowsDone

            # make sure all key names are unique, when we re-put and re-parse (h2o caching issues)
            key = "syn_" + str(trial)
            key2 = "syn_" + str(trial) + ".hex"

            # DON"T get redirected to S3! (EC2 hack in config, remember!)
            # use it at the node level directly (because we gen'ed the files.
            # I suppose we could force the redirect state bits in h2o.nodes[0] to False, instead?:w
            xs = h2o.nodes[0].import_files(SYNDATASETS_DIR)['keys']
            headerKey = [x for x in xs if hdrFilename in x][0]
            dataKey = [x for x in xs if csvFilename not in x][0]

            # use regex. the only files in the dir will be the ones we just created with  *fileN* match
            print "Header Key =", headerKey

            # put the right name in
            if kwargs['header_from_file'] == 'syn_header':
                kwargs['header_from_file'] = headerKey
            # use one of the data files?
            elif kwargs['header_from_file'] == 'syn_data':
                kwargs['header_from_file'] = dataKey

            print "If header_from_file= is used, we are currently required to force header=1 for h2o"
            if kwargs['header_from_file']:
                kwargs['header'] =  1
            # if we have a header in a data file, tell h2o (for now)
            elif DATA_HAS_HEADER:
                kwargs['header'] =  1
            else:
                kwargs['header'] =  0

            # may have error if h2o doesn't get anything!
            start = time.time()
            parseKey = h2o.nodes[0].parse('*syn_data_*'+str(trial)+"_"+rowxcol+'*', key2=key2, timeoutSecs=timeoutSecs, **kwargs)

            print "parseKey['destination_key']: " + parseKey['destination_key']
            print 'parse time:', parseKey['response']['time']

            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            h2o_cmd.infoFromInspect(inspect, csvPathname)
            print "\n" + csvPathname, \
                "    num_rows:", "{:,}".format(inspect['num_rows']), \
                "    num_cols:", "{:,}".format(inspect['num_cols'])

            # more reporting: (we can error here if extra col in header, causes all NA for missing col of data)
            h2o_cmd.columnInfoFromInspect(parseKey['destination_key'], exceptionOnMissingValues=False)

            # should match # of cols in header or ??
            self.assertEqual(inspect['num_cols'], totalCols, \
                "parse created result with the wrong number of cols %s %s" % (inspect['num_cols'], totalCols))

            # do we end up parsing one data rows as a header because of mismatch in gen/param
            h2oLosesOneData = (headerRowsDone==0) and (kwargs['header']==1) and not DATA_HAS_HEADER
            print "h2oLosesOneData:", h2oLosesOneData
            if h2oLosesOneData:
                totalDataRows -= 1
                
            self.assertEqual(inspect['num_rows'], totalDataRows,
                "parse created result with the wrong number of rows (header rows don't count) h2o: %s gen'ed: %s" % \
                (inspect['num_rows'], totalDataRows))

            # put in an ignore param, that will fail unless headers were parsed correctly
            # doesn't matter if the header got a comment, should see it
            h2oShouldSeeHeader = (HEADER_HAS_HEADER and (kwargs['header_from_file']==1)) or DATA_HAS_HEADER
            if h2oShouldSeeHeader:
                kwargs = {'sample': 75, 'depth': 25, 'ntree': 1, 'ignore': 'A'}
            else:
                kwargs = {'sample': 75, 'depth': 25, 'ntree': 1}

            start = time.time()
            elapsed = time.time() - start
            print "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            print "trial #", trial, "totalDataRows:", totalDataRows, "parse end on ", csvFilename, \
                'took', time.time() - start, 'seconds'

            h2o.check_sandbox_for_errors()

if __name__ == '__main__':
    h2o.unit_main()
