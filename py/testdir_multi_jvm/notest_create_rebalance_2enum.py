import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_util, h2o_import as h2i

REBALANCE_CHUNKS = 100

print "Keeping the # of rows > the number of chunks I ask for. Is that a requirement?"
paramDict = {
    'rows': [100,1000],
    'cols': [1,10,100], # Number of data columns (in addition to the first response column)
    'seed': [None, 1234],
    'randomize': [None, 0, 1],
    'value': [None, 0, 1234567890, 1e6, -1e6], # Constant value (for randomize=false)
    'real_range': [None, 0, 1234567890, 1e6, -1e6], # -range to range
    'categorical_fraction': [None, 0.1, 1.0], # Fraction of integer columns (for randomize=true)
    'factors': [None, 5, 17], # Factor levels for categorical variables
    'integer_fraction': [None, 0.1, 1.0], # Fraction of integer columns (for randomize=true)
    'integer_range': [None, 0, 1, 1234567890], # -range to range
    'missing_fraction': [None, 0.1, 1.0],
    'response_factors': [None, 1, 2, 10], # Number of factor levels of the first column (1=real, 2=binomial, N=multinomial)
}

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(3)
        global SYNDATASETS_DIR
        SYNDATASETS_DIR = h2o.make_syn_dir()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_create_rebalance_2enum(self):
        # default
        params = {
            'rows': 100, 
            'cols': 1
        }
        for trial in range(20):
            # CREATE FRAME params################################################################
            h2o_util.pickRandParams(paramDict, params)
            i = params.get('integer_fraction', None)
            c = params.get('categorical_fraction', None)
            r = params.get('randomize', None)
            v = params.get('value', None)
            # h2o does some strict checking on the combinations of these things
            # fractions have to add up to <= 1 and only be used if randomize
            # h2o default randomize=1?
            if r:
                if not i:   
                    i = 0
                if not c:
                    c = 0
                if (i and c) and (i + c) >= 1.0:
                    c = 1.0 - i
                params['integer_fraction'] = i
                params['categorical_fraction'] = c
                params['value'] = None
                
            else:
                params['randomize'] = 0
                params['integer_fraction'] = 0
                params['categorical_fraction'] = 0


            # CREATE FRAME*****************************************************
            kwargs = params.copy()
            print kwargs
            timeoutSecs = 300
            hex_key='temp1000.hex'
            cfResult = h2o.nodes[0].create_frame(key=hex_key, timeoutSecs=timeoutSecs, **kwargs)

            # REBALANCE*****************************************************
            print "Rebalancing it to create an artificially large # of chunks"
            rb_key = "rb_%s" % (hex_key)
            start = time.time()
            print "Rebalancing %s to %s with %s chunks" % (hex_key, rb_key, REBALANCE_CHUNKS)
            SEEDPERFILE = random.randint(0, sys.maxint)
            rebalanceResult = h2o.nodes[0].rebalance(source=hex_key, after=rb_key, chunks=REBALANCE_CHUNKS)
            elapsed = time.time() - start
            print "rebalance end on ", hex_key, 'to', rb_key, 'took', elapsed, 'seconds',\

            # TO ENUM*****************************************************
            print "Now doing to_enum across all columns of %s" % rb_key
            for column_index in range(params['cols']):
                # is the column index 1-base in to_enum
                result = h2o.nodes[0].to_enum(None, src_key=rb_key, column_index=column_index+1)
                # print "\nto_enum result:", h2o.dump_json(result)
                summaryResult = h2o_cmd.runSummary(key=hex_key)
                # check that it at least is an enum column now, with no na's
                # just look at the column we touched
                column = summaryResult['summaries'][column_index]
                colname = column['colname']
                coltype = column['type']
                nacnt = column['nacnt']
                stats = column['stats']
                stattype = stats['type']

                # we have some # of na's in the columns...but there should not be 100% NA
                if nacnt>=params['rows']:
                    raise Exception("column %s, which has name '%s', somehow too many NAs after convert to Enum  %s %s" % 
                        (column_index, colname, nacnt, params['rows']))

                print "I suspect that columns that are constant, maybe with NAs also, don't convert to Enum"
                if stattype != 'Enum':
                    raise Exception("column %s, which has name '%s', didn't convert to Enum, is %s %s %s" %  
                        (column_index, colname, stattype, coltype, h2o.dump_json(column)))

                cardinality = stats['cardinality']
                # don't know the cardinality expected
                # if cardinality!=4:
                #     raise Exception("column %s, which has name '%s',  should have cardinality 4, got: %s" % 
                #         (column_index, colname, cardinality))

                h2o_cmd.infoFromSummary(summaryResult)
    
            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
