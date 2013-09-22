import unittest, time, sys, csv
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_glm

# translate provides the mapping between original and predicted
# since GLM is binomial, We predict 0 for 0 and 1 for > 0
def compare_csv_last_col(csvPathname, msg, translate=None, skipHeader=False):
    predictOutput = []
    with open(csvPathname, 'rb') as f:
        reader = csv.reader(f)
        print "csv read of", csvPathname
        rowNum = 0
        for row in reader:
            # print the last col
            # ignore the first row ..header
            if skipHeader and rowNum==0:
                print "Skipping header in this csv"
            else:
                output = row[-1]
                if translate:
                    output = translate[int(output)]
                # only print first 10 for seeing
                if rowNum<10: print msg, row[-1], output
                predictOutput.append(output)
            rowNum += 1
    return (rowNum, predictOutput)

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

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_glm_predict3(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()

        trees = 15
        timeoutSecs = 120
        if 1==0:
            csvPathname = 'standard/covtype.data'
            hexKey = 'covtype.data.hex'
        else:
            # try smaller data set
            csvPathname = 'standard/covtype.shuffled.10pct.data'
            hexKey = 'covtype.shuffled.10pct.data.hex'

        predictHexKey = 'predict.hex'
        predictCsv = 'predict.csv'
        bucket = 'home-0xdiag-datasets'

        csvPredictPathname = SYNDATASETS_DIR + "/" + predictCsv
        # for using below in csv reader
        csvFullname = h2i.find_folder_and_filename(bucket, csvPathname, schema='put', returnFullPath=True)

        def predict_and_compare_csvs(model_key):
            start = time.time()
            predict = h2o.nodes[0].generate_predictions(model_key=model_key,
                data_key=hexKey, destination_key=predictHexKey)
            print "generate_predictions end on ", hexKey, " took", time.time() - start, 'seconds'
            h2o.check_sandbox_for_errors()
            inspect = h2o_cmd.runInspect(key=predictHexKey)
            h2o_cmd.infoFromInspect(inspect, 'predict.hex')

            h2o.nodes[0].csv_download(src_key=predictHexKey, csvPathname=csvPredictPathname)
            h2o.check_sandbox_for_errors()

            print "Do a check of the original output col against predicted output"
            translate = {1: 0.0, 2: 1.0, 3: 1.0, 4: 1.0, 5: 1.0, 6: 1.0, 7: 1.0}
            (rowNum1, originalOutput) = compare_csv_last_col(csvFullname, 
                msg="Original", translate=translate, skipHeader=False)
            (rowNum2, predictOutput)  = compare_csv_last_col(csvPredictPathname, 
                msg="Predicted", skipHeader=True)

            # no header on source
            if ((rowNum1+1) != rowNum2):
                raise Exception("original rowNum1: %s + 1 not same as downloaded predict (w/header) rowNum2: \
                    %s" % (rowNum1, rowNum2))

            wrong = 0
            wrong0 = 0
            wrong1 = 0
            for rowNum,(o,p) in enumerate(zip(originalOutput, predictOutput)):
                o = float(o)
                p = float(p)
                if o!=p:
                    msg = "Comparing original output col vs predicted. row %s differs. \
                        original: %s predicted: %s"  % (rowNum, o, p)
                    if p==0.0 and wrong0==10:
                        print "Not printing any more predicted=0 mismatches"
                    elif p==0.0 and wrong0<10:
                        print msg
                    if p==1.0 and wrong1==10:
                        print "Not printing any more predicted=1 mismatches"
                    elif p==1.0 and wrong1<10:
                        print msg

                    if p==0.0:
                        wrong0 += 1
                    elif p==1.0:
                        wrong1 += 1

                    wrong += 1

            print "wrong0:", wrong0
            print "wrong1:", wrong1
            print "\nTotal wrong:", wrong
            print "Total:", len(originalOutput)
            pctWrong = (100.0 * wrong)/len(originalOutput)
            print "wrong/Total * 100 ", pctWrong
            # I looked at what h2o can do for modelling with binomial and it should get better than 25% error?
            if pctWrong > 28.0:
                raise Exception("pct wrong too high. Expect < 28% error")

        #*************************************************************************
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)

        print "Use H2O GeneratePredictionsPage with a H2O generated model and the same data key." 
        print "Does this work? (feeding in same data key)if you're predicting, "
        print "don't you need one less column (the last is output?)"
        print "WARNING: max_iter set to 8 for benchmark comparisons"
        max_iter = 8

        y = "54"
        kwargs = {
            'x': "",
            'y': y,
            'family': 'binomial',
            'link': 'logit',
            'n_folds': 1,
            'case_mode': '>',
            'case': 1, # zero should predict to 0, 2-7 should predict to 1
            'max_iter': max_iter,
            'beta_epsilon': 1e-3}

        timeoutSecs = 120
        # L2 
        start = time.time()
        kwargs.update({'alpha': 0, 'lambda': 0})
        glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
        print "glm (L2) end on ", csvPathname, 'took', time.time() - start, 'seconds'
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
        predict_and_compare_csvs(model_key=glm['destination_key'])

        # Elastic
        kwargs.update({'alpha': 0.5, 'lambda': 1e-4})
        start = time.time()
        glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
        print "glm (Elastic) end on ", csvPathname, 'took', time.time() - start, 'seconds'
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
        predict_and_compare_csvs(model_key=glm['destination_key'])

        # L1
        kwargs.update({'alpha': 1, 'lambda': 1e-4})
        start = time.time()
        glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
        print "glm (L1) end on ", csvPathname, 'took', time.time() - start, 'seconds'
        h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
        predict_and_compare_csvs(model_key=glm['destination_key'])

if __name__ == '__main__':
    h2o.unit_main()
