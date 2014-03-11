import unittest, random, sys, time, re
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util
import h2o_gbm
import getpass

# details:
# we want to seed a random dictionary for our enums
# string.ascii_uppercase string.printable string.letters string.digits string.punctuation string.whitespace
# restricting the choices makes it easier to find the bad cases
randChars = "abeE01" + "$%+-.;|\t "
randChars = "abeE01" # bad..causes NAification. probably 1E0e is causing a problem
# randChars = "abfF01" # try this.. fails
# randChars = "abcdef" #
quoteChars = "\'\""
# don't use any quote characters. We'd have to protect combinations
quoteChars = ""
MIN_ENUM_WIDTH = 2
MAX_ENUM_WIDTH = 8
RAND_ENUM_LENGTH = True

DO_PLOT = getpass.getuser()=='kevin'

DO_MEDIAN = True
MAX_QBINS = 1000
MULTI_PASS = 1

def random_enum(n, randChars=randChars, quoteChars=quoteChars):
    # randomly return None 10% of the time
    if random.randint(0,9)==0:
        return None

    choiceStr = randChars + quoteChars
    mightBeNumberOrWhite = True
    while mightBeNumberOrWhite:
        # H2O doesn't seem to tolerate random single or double quote in the first two rows.
        # disallow that by not passing quoteChars for the first two rows (in call to here)
        r = ''.join(random.choice(choiceStr) for x in range(n))
        mightBeNumberOrWhite = h2o_util.might_h2o_think_number_or_whitespace(r)

    return r

def create_enum_list(n=4, **kwargs):
    # Allowing length one, we sometimes form single digit numbers that cause the whole column to NA
    # see DparseTask.java for this effect
    # FIX! if we allow 0, then we allow NA?. I guess we check for no missing, so can't allow NA
    # too many retries allowing 1. try 2 min.
    if RAND_ENUM_LENGTH:
        enumList = [random_enum(n=random.randint(MIN_ENUM_WIDTH, MAX_ENUM_WIDTH), **kwargs) for i in range(n)]
    else:
        # a fixed width is sometimes good for finding badness
        enumList = [random_enum(n=MAX_ENUM_WIDTH, **kwargs) for i in range(n)]
    return enumList

def write_syn_dataset(csvPathname, rowCount, inCount=1, outCount=1, SEED='12345678', 
        colSepChar=",", rowSepChar="\n", quoteChars=""):
    r1 = random.Random(SEED)

    # create the per-column choice lists
    colEnumList = []
    for col in range(inCount):
        enumList = create_enum_list(n=random.randint(1,4), quoteChars=quoteChars)
        colEnumList.append(enumList)

    dsf = open(csvPathname, "w+")
    for row in range(rowCount):
        # doesn't guarantee that 10000 rows have 10000 unique enums in a column
        # essentially sampling with replacement
        rowData = []
        for iCol in range(inCount):
            # FIX! we should add some random NA?
            ri = random.choice(colEnumList[iCol])
            rowData.append(ri)

        # output columns. always 0-10e6 with 2 digits of fp precision
        for oCol in range(outCount):
            ri = "%.2f" % random.uniform(0, 10e6)
            rowData.append(ri)

        # use the new Hive separator
        rowDataCsv = colSepChar.join(map(str,rowData)) + rowSepChar
        sys.stdout.write(rowDataCsv)
        dsf.write(rowDataCsv)
    dsf.close()


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.sleep(3600)
        h2o.tear_down_cloud()

    def test_exec_enums_rand_cut(self):
        h2o.beta_features = True
        SYNDATASETS_DIR = h2o.make_syn_dir()

        n = 10
        tryList = [
            (n, 1, 1, 'cD', 300), 
            (n, 2, 3, 'cE', 300), 
            (n, 10, 9, 'cE', 300), 
            ]

        # create key names to use for exec
        eKeys = ['e%s' % i for i in range(10)]

        h2b.browseTheCloud()
        trial = 0
        xList = []
        eList = []
        fList = []
        for repeat in range(10):
            for (rowCount, iColCount, oColCount, hex_key, timeoutSecs) in tryList:

                # CREATE DATASET*******************************************
                colCount = iColCount + oColCount
                SEEDPERFILE = random.randint(0, sys.maxint)
                csvFilename = 'syn_enums_' + str(rowCount) + 'x' + str(colCount) + '.csv'
                csvPathname = SYNDATASETS_DIR + '/' + csvFilename

                print "Creating random", csvPathname
                write_syn_dataset(csvPathname, rowCount, iColCount, oColCount, SEEDPERFILE)

                # PARSE*******************************************************
                # should be two different keys in the sample
                e = random.sample(eKeys,2)
                fKey = e[0]
                eKey = e[1]

                parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=eKey, timeoutSecs=30)
                print "Parse result['destination_key']:", parseResult['destination_key']
                inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
                print h2o.dump_json(inspect)

                (missingValuesDict, constantValuesDict, enumSizeDict, colTypeDict, colNameDict) = \
                    h2o_cmd.columnInfoFromInspect(parseResult['destination_key'], exceptionOnMissingValues=False)

                # error if any col has constant values
                if len(constantValuesDict) != 0:
                    raise Exception("Probably got a col NA'ed and constant values as a result %s" % constantValuesDict)

                # EXEC*******************************************************
                # don't use exec_expr to avoid issues with Inspect following etc.
                randICol = random.randint(0,iColCount-1)
                randOCol = random.randint(iColCount, iColCount+oColCount-1)

                start = time.time()
                h2o.nodes[0].exec_query(str='%s=%s[%s,]' % (fKey, eKey, randOCol))
                elapsed = time.time() - start
                print "exec1 end on ", csvFilename, 'took', elapsed, 'seconds.'
                execTime = elapsed
                
                gKey = random.choice(eKeys)

                start = time.time()
                h2o.nodes[0].exec_query(str='%s=%s' % (gKey, fKey))
                print "exec2 end on ", csvFilename, 'took', elapsed, 'seconds.'
                elapsed = time.time() - start
                execTime = elapsed

                # QUANTILE*******************************************************
                quantile = 0.5 if DO_MEDIAN else .999
                # first output col
                column = iColCount
                start = time.time()
                q = h2o.nodes[0].quantiles(source_key=fKey, column=column, quantile=quantile, max_qbins=MAX_QBINS, multiple_pass=MULTI_PASS)
                elapsed = time.time() - start
                print "quantile end on ", csvFilename, 'took', elapsed, 'seconds.'
                quantileTime = elapsed


                # remove all keys*******************************************************
                start = time.time()
                h2o.nodes[0].remove_all_keys()
                elapsed = time.time() - start
                print "remove all keys end on ", csvFilename, 'took', elapsed, 'seconds.'

                trial += 1
                xList.append(trial)
                eList.append(execTime)
                fList.append(quantileTime)

        if DO_PLOT:
            xLabel = 'trial'
            eLabel = 'exec cut time'
            fLabel = 'quantile time'
            eListTitle = ""
            fListTitle = ""
            h2o_gbm.plotLists(xList, xLabel, eListTitle, eList, eLabel, fListTitle, fList, fLabel)



if __name__ == '__main__':
    h2o.unit_main()
