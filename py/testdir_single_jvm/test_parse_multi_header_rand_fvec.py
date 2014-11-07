import unittest, time, sys, random, os
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_browse as h2b, h2o_util

# ord('a') gives 97. Use that when you pass it as url param to h2o
# str(unichr(97)) gives 'a'

print "Data rows in header_from_file are ignored unless that file was part of the parse pattern"
print "Tab in header is not auto-detected separator. comma and space are"
print "Hmmm..if there's a header in the data files, it needs to have the same separator as the data?? otherwise parse chaos"
print "Maybe only if header_from_file is also used"
paramsDict = {
    # don't worry about these variants in this test (just parse normal)
    # 'parser_type': [None, 'AUTO', 'XLS', 'XLSX', 'SVMLight'],
    # I suppose, libsvm could have strangeness!..but there is no header with libsvm?
    # 'parser_type': [None, 'AUTO'],
    # add the hive 1 separator a choice
    'separator': ['\t', ',', ' ', str(unichr(1))], 
    'preview': [1],
    # 'separator': ['\t', ',', ' '],
    'hdr_separator': [' ', ',', 'same'],
    'header': [None, 0,1],
    # we can point to the 'wrong' file!
    # assume this is always used, otherwise we sum data rows without knowing if we'll use the header file?
    # always point to the header file..again, if we switch it around, the counts are off
    # 'header_from_file': [None, 'header', 'data'],
    # FIX! always specify it from 'header' for now
    'header_from_file': ['header'],
}

# ability to selectively comment the first line (which may be data or header)
# Does not write an extra line as the comment first in that case
# (header can be picked from the comment line)
# H2O always just strips the comment from the first line? and continues parsing?

# Don't write headerString if None (for non-header files)
# Don't write data if rowCount is None
def write_syn_dataset(csvPathname, rowCount, headerString, rList, commentFirst=False, sepChar=','):
    dsf = open(csvPathname, "w+")

    if commentFirst:
        h = "# random comment junk because we don't support commented headers yet"
        dsf.write(h + "\n")

    # UPDATE: comments are always ignored. So a commented header, commented data, is ignored.
    headerRowsDone = 0
    if headerString is not None:
        # FIX: for now, always put the header in even if we're told to!
        h = headerString
        headerRowsDone += 1
        dsf.write(h + "\n")
    
    if rowCount is not None:        
        for i in range(rowCount):
            # two choices on the input. Make output choices random
            # do the two choices, in order to be able to do RF (cols have to have varying values)
            r = rList[random.randint(0,1)] + sepChar + str(random.randint(0,7))
            dsf.write(r + "\n")
        dsf.close()
        return (headerRowsDone, rowCount) # rows done
    else:
        return (headerRowsDone, 0) # rows done

def rand_rowData(colCount, sepChar):
    # do one fp, one enum, the rest int
    # assume colCount is at least 3
    if colCount<=3:
        raise Exception("Test expects desired colCount to be 3 or more", colCount)
    
    randomReal = "%0.2f" % (random.random() * float(random.randint(1,1000)))
    randomEnum = random.choice(['hello', 'there', 'how', 'are', 'you'])
    rowData = [randomReal, randomEnum] +  [random.randint(0,7) for i in range(colCount-2)]
    rowData1= sepChar.join(map(str,rowData))

    randomReal = "%0.2f" % (random.random() * float(random.randint(1,10000)))
    randomEnum = random.choice(['hello2', '2there', '2how', '2are', '2you'])
    rowData = [randomReal, randomEnum] +  [random.randint(0,7) for i in range(colCount-2)]
    rowData2= sepChar.join(map(str,rowData))
    # RF will complain if all inputs are the same
    r = [rowData1, rowData2]
    return r

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(java_heap_GB=4,use_flatfile=True)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
    
    def test_parse_multi_header_rand_fvec(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn_ints.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        allowedLetters = 'abcdeABCDE01234[]'
        headerChoices = []
        for n in range(500): # max # of cols below is 500
            done = False
            while not done:
                l = random.randint(1,64) # random length headers
                headerName = ''.join([random.choice(allowedLetters) for _ in range(l)])
                # we keep trying if we already have that header name. Has to be unique.
                done = headerName not in headerChoices
            headerChoices.append(headerName)

        tryList = [
            (3, 5, 9, 'cA', 60, 0),
            # (3, 5, 25, 'cA', 60, 0),
            # (10, 100, 500, 'cA', 60, 0),
            ]

        for trial in range(20):
            (fileNum, rowCount, colCount, hex_key, timeoutSecs, dataRowsWithHeader) = random.choice(tryList)
            print fileNum, rowCount, colCount, hex_key, timeoutSecs, dataRowsWithHeader
            # FIX! should we add a header to them randomly???
            print "Wait while", fileNum, "synthetic files are created in", SYNDATASETS_DIR
            rowxcol = str(rowCount) + 'x' + str(colCount)
            totalCols = colCount + 1 # 1 extra for output
            totalDataRows = 0
            totalHeaderRows = 0
            # random selection of parse param choices

            # HEADER_HAS_HDR_ROW = random.randint(0,1)
            HEADER_HAS_HDR_ROW = 1
            
            DATA_HAS_HDR_ROW = random.randint(0,1)
            PARSE_PATTERN_INCLUDES_HEADER = random.randint(0,1)
            # DATA_FIRST_IS_COMMENT = random.randint(0,1)
            # HEADER_FIRST_IS_COMMENT = random.randint(0,1)
            # FIX! doesn't seem to like just comment in the header file
            DATA_FIRST_IS_COMMENT = 0
            HEADER_FIRST_IS_COMMENT = 0
            
            GZIP_DATA = random.randint(0,1)
            GZIP_HEADER = random.randint(0,1)
            SEP_CHAR_GEN = random.choice(paramsDict['separator'])

            HEADER_SEP_CHAR_GEN = random.choice(paramsDict['hdr_separator'])
            if HEADER_SEP_CHAR_GEN == 'same':
                HEADER_SEP_CHAR_GEN = SEP_CHAR_GEN

            # don't put a header in a data file with a different separator?
            if DATA_HAS_HDR_ROW and HEADER_HAS_HDR_ROW:
                HEADER_SEP_CHAR_GEN = SEP_CHAR_GEN

            # Hack: if both data and header files have a header, then, just in case
            # the header and data files should have the same separator
            # if they don't, make header match data
            if DATA_HAS_HDR_ROW and HEADER_HAS_HDR_ROW:
                HEADER_SEP_CHAR_GEN = SEP_CHAR_GEN

            # New for fvec? if separators are not the same, then the header separator needs to be comma
            if HEADER_SEP_CHAR_GEN != SEP_CHAR_GEN:
                HEADER_SEP_CHAR_GEN = ','


            # screw it. make them always match
            HEADER_SEP_CHAR_GEN = SEP_CHAR_GEN

            if HEADER_SEP_CHAR_GEN in (',', ' '):
                pass
                # extra spaces? Don't add any
                # if random.randint(0,1):
                #    HEADER_SEP_CHAR_GEN = " " + HEADER_SEP_CHAR_GEN
                # if random.randint(0,1):
                #    HEADER_SEP_CHAR_GEN = HEADER_SEP_CHAR_GEN + " "

            kwargs = {}
            for k,v in paramsDict.items():
                kwargs[k] = random.choice(v)

            kwargs['separator'] = SEP_CHAR_GEN
            # parse doesn't auto-detect tab. will autodetect space and comma
            if SEP_CHAR_GEN==" "  or SEP_CHAR_GEN==",": 
                del kwargs['separator']
            else:
                kwargs['separator'] = ord(SEP_CHAR_GEN)
            
            # randomly add leading and trailing white space
            # we have to do this after we save the single char HEADER_SEP_CHAR_GEN
            if SEP_CHAR_GEN in (',', ' '):
                if random.randint(0,1):
                    SEP_CHAR_GEN = " " + SEP_CHAR_GEN
                if random.randint(0,1):
                    SEP_CHAR_GEN = SEP_CHAR_GEN + " "


            print '\nHEADER_HAS_HDR_ROW:', HEADER_HAS_HDR_ROW
            print 'DATA_HAS_HDR_ROW:', DATA_HAS_HDR_ROW
            print 'PARSE_PATTERN_INCLUDES_HEADER', PARSE_PATTERN_INCLUDES_HEADER
            print 'DATA_FIRST_IS_COMMENT:', DATA_FIRST_IS_COMMENT
            print 'HEADER_FIRST_IS_COMMENT:', HEADER_FIRST_IS_COMMENT
            print 'SEP_CHAR_GEN:', "->" + SEP_CHAR_GEN + "<-"
            print 'HEADER_SEP_CHAR_GEN:', "->" + HEADER_SEP_CHAR_GEN + "<-"
            print 'GZIP_DATA:', GZIP_DATA
            print 'GZIP_HEADER:', GZIP_HEADER 

            # they need to both use the same separator (h2o rule)
# can't have duplicates
            hfhList = random.sample(headerChoices, colCount) + ["output"]
            # UPDATE: always use comma or space for header separator?? it should work no matter what 
            # separator the data uses?

            headerForHeader = HEADER_SEP_CHAR_GEN.join(hfhList)
            print "headerForHeader:", headerForHeader

            
            # make these different
            # hfdList = [random.choice(headerChoices) for h in range(colCount)] + ["output"]
            # FIX! keep them the same for now to avoid some odd cases on what header gets used to RF
            hfdList = hfhList

            headerForData   = SEP_CHAR_GEN.join(hfdList)

        
            # create data files
            for fileN in range(fileNum):
                csvFilenameSuffix = str(fileN) + "_" + str(SEED) + "_" + str(trial) + "_" + rowxcol + '_csv'
                csvFilename = 'syn_data_' + csvFilenameSuffix
                csvPathname = SYNDATASETS_DIR + '/' + csvFilename
                rList = rand_rowData(colCount, sepChar=SEP_CHAR_GEN)
                (headerRowsDone, dataRowsDone) = write_syn_dataset(csvPathname, rowCount, 
                    headerString=(headerForData if DATA_HAS_HDR_ROW else None), rList=rList,
                    commentFirst=DATA_FIRST_IS_COMMENT, sepChar=SEP_CHAR_GEN)
                totalDataRows += dataRowsDone
                totalHeaderRows += headerRowsDone
                if GZIP_DATA:
                    csvPathnamegz = csvPathname + ".gz"
                    print "gzipping to", csvPathnamegz
                    h2o_util.file_gzip(csvPathname, csvPathnamegz)
                    os.rename(csvPathname, SYNDATASETS_DIR + "/not_used_data_" + csvFilenameSuffix)
                    # pattern match should find the right key with csvPathname


            # create the header file
            hdrFilenameSuffix = str(SEED) + "_" + str(trial) + "_" + rowxcol + '_csv'
            hdrFilename = 'syn_header_' + hdrFilenameSuffix
            hdrPathname = SYNDATASETS_DIR + '/' + hdrFilename
            # dataRowsWithHeader = 0 # temp hack
            (headerRowsDone, dataRowsDone) = write_syn_dataset(hdrPathname, dataRowsWithHeader, 
                headerString=(headerForHeader if HEADER_HAS_HDR_ROW else None), rList=rList,
                commentFirst=HEADER_FIRST_IS_COMMENT, sepChar=SEP_CHAR_GEN)
            # only include header file data rows if the parse pattern includes it
            if PARSE_PATTERN_INCLUDES_HEADER: 
                totalDataRows += dataRowsDone
            totalHeaderRows += headerRowsDone
            if GZIP_HEADER:
                hdrPathnamegz = hdrPathname + ".gz"
                print "gzipping to", hdrPathnamegz
                h2o_util.file_gzip(hdrPathname, hdrPathnamegz)
                os.rename(hdrPathname, SYNDATASETS_DIR + "/not_used_header_" + hdrFilenameSuffix)
                # pattern match should find the right key with hdrPathnameh

            # make sure all key names are unique, when we re-put and re-parse (h2o caching issues)
            hex_key = "syn_dst" + str(trial) + ".hex"

            # DON"T get redirected to S3! (EC2 hack in config, remember!)
            # use it at the node level directly (because we gen'ed the files.
            # I suppose we could force the redirect state bits in h2o.nodes[0] to False, instead?:w

            # put them, rather than using import files, so this works if remote h2o is used
            # and python creates the files locally
            fileList = os.listdir(SYNDATASETS_DIR)
            for f in fileList:
                h2i.import_only(path=SYNDATASETS_DIR + "/" + f, schema='put', noPrint=True)

            h2o_cmd.runStoreView()
            headerKey = h2i.find_key(hdrFilename)
            dataKey = h2i.find_key(csvFilename)

            # use regex. the only files in the dir will be the ones we just created 
            # with  *fileN* match
            print "Header Key =", headerKey

            # put the right name in
            if kwargs['header_from_file'] == 'header':
                # do we need to add the .hex suffix we know h2o will append
                kwargs['header_from_file'] = headerKey
            # use one of the data files?
            elif kwargs['header_from_file'] == 'data':
                # do we need to add the .hex suffix we know h2o will append
                kwargs['header_from_file'] = dataKey

            # if there's no header in the header file, turn off the header_from_file
            if not HEADER_HAS_HDR_ROW:
                kwargs['header_from_file'] = None

            if HEADER_HAS_HDR_ROW and (kwargs['header_from_file'] == headerKey):
                ignoreForRf = hfhList[0]
            elif DATA_HAS_HDR_ROW:
                ignoreForRf = hfdList[0]
            else:
                ignoreForRf = None

            print "If header_from_file= , required to force header=1 for h2o"
            if kwargs['header_from_file']:
                kwargs['header'] =  1
            # if we have a header in a data file, tell h2o (for now)
            elif DATA_HAS_HDR_ROW:
                kwargs['header'] =  1
            else:
                kwargs['header'] =  0

            # may have error if h2o doesn't get anything!
            start = time.time()
            if PARSE_PATTERN_INCLUDES_HEADER and HEADER_HAS_HDR_ROW:
                pattern = 'syn_*'+str(trial)+"_"+rowxcol+'*'
            else:
                pattern = 'syn_data_*'+str(trial)+"_"+rowxcol+'*'

            # don't pass to parse
            kwargs.pop('hdr_separator', None)
            parseResult = h2i.parse_only(pattern=pattern, hex_key=hex_key, timeoutSecs=timeoutSecs, **kwargs)
            print "parseResult['destination_key']: " + parseResult['destination_key']

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            h2o_cmd.infoFromInspect(inspect, csvPathname)
            print "\n" + csvPathname, \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])

            # more reporting: (we can error here if extra col in header, 
            # causes all NA for missing col of data)
            h2o_cmd.columnInfoFromInspect(parseResult['destination_key'], exceptionOnMissingValues=False)

            # should match # of cols in header or ??
            self.assertEqual(inspect['numCols'], totalCols, \
                "parse created result with the wrong number of cols %s %s" % (inspect['numCols'], totalCols))

            # do we end up parsing one data rows as a header because of mismatch in gen/param
            h2oLosesOneData = (headerRowsDone==0) and (kwargs['header']==1) and not DATA_HAS_HDR_ROW
            # header in data file gets treated as data
            h2oGainsOneData = (headerRowsDone!=0) and (kwargs['header']==1) and \
                DATA_HAS_HDR_ROW and (kwargs['header_from_file'] is not None)
            h2oGainsOneData = False
            print "h2oLosesOneData:", h2oLosesOneData
            print "h2oGainsOneData:", h2oGainsOneData
            if h2oLosesOneData:
                totalDataRows -= 1
            if h2oGainsOneData:
                totalDataRows += 1
                
            if 1==0: # FIX! don't check for now
                self.assertEqual(inspect['numRows'], totalDataRows,
                    "parse created result with the wrong number of rows h2o %s gen'ed: %s" % \
                    (inspect['numRows'], totalDataRows))

            # put in an ignore param, that will fail unless headers were parsed correctly
            # doesn't matter if the header got a comment, should see it

            kwargs = {'sample': 100, 'depth': 25, 'ntree': 2, 'ignore': ignoreForRf}
            start = time.time()
            # h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=10, **kwargs)
            elapsed = time.time() - start
            print "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            print "trial #", trial, "totalDataRows:", totalDataRows, "parse end on ", csvFilename, \
                'took', time.time() - start, 'seconds'

            h2o.check_sandbox_for_errors()
            h2i.delete_keys_at_all_nodes(pattern='syn_datasets')


if __name__ == '__main__':
    h2o.unit_main()
