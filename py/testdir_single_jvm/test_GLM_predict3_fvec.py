import unittest, time, sys, csv
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_glm, h2o_exec as h2e

DO_BUG=False
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
        h2o.beta_features = True
        SYNDATASETS_DIR = h2o.make_syn_dir()

        trees = 15
        timeoutSecs = 120
        csvPathname = 'standard/covtype.data'
        hexKey = 'covtype.data.hex'

        predictHexKey = 'predict.hex'
        predictCsv = 'predict.csv'

        execHexKey = 'A.hex'
        execCsv = 'exec.csv'

        bucket = 'home-0xdiag-datasets'

        csvPredictPathname = SYNDATASETS_DIR + "/" + predictCsv
        csvExecPathname = SYNDATASETS_DIR + "/" + execCsv
        # for using below in csv reader
        csvFullname = h2i.find_folder_and_filename(bucket, csvPathname, schema='put', returnFullPath=True)

        def predict_and_compare_csvs(model_key):
            start = time.time()
            predict = h2o_cmd.runPredict(model_key=model_key, data_key=hexKey, destination_key=predictHexKey)
            print "runPredict end on ", hexKey, " took", time.time() - start, 'seconds'
            h2o.check_sandbox_for_errors()
            inspect = h2o_cmd.runInspect(key=predictHexKey)
            h2o_cmd.infoFromInspect(inspect, 'predict.hex')

            h2o.nodes[0].csv_download(src_key=predictHexKey, csvPathname=csvPredictPathname)
            h2o.nodes[0].csv_download(src_key=execHexKey, csvPathname=csvExecPathname)
            h2o.check_sandbox_for_errors()

            print "Do a check of the original output col against predicted output"
            translate = {1: 0.0, 2: 1.0, 3: 1.0, 4: 1.0, 5: 1.0, 6: 1.0, 7: 1.0}
            (rowNum1, originalOutput) = compare_csv_last_col(csvExecPathname,
                msg="Original, after being exec'ed", skipHeader=True)
            (rowNum2, predictOutput)  = compare_csv_last_col(csvPredictPathname, 
                msg="Predicted", skipHeader=True)

            # no header on source
            if (rowNum1 != rowNum2):
                raise Exception("original rowNum1: %s not same as downloaded predict (w/header) rowNum2: \
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
            if pctWrong > 10.0:
                raise Exception("pct wrong too high. Expect < 10% error")

        #*************************************************************************
        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)
        # do the binomial conversion with Exec2, for both training and test (h2o won't work otherwise)
        trainKey = parseResult['destination_key']
        y = 54
        if DO_BUG:
            # class 4=0, all else 1
            execExpr="A.hex=%s;A.hex[,%s]=(A.hex[,%s]!=%s)" % (trainKey, y+1, y+1, 4)
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)
        else:
            execExpr="A.hex=%s" % trainKey
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)

            # class 4=0, all else 1
            execExpr="A.hex[,%s]=(A.hex[,%s]!=%s)" % (y+1, y+1, 4)
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)

        max_iter = 8
        kwargs = {
            'classification': 1,
            'response': 'C' + str(y),
            'family': 'binomial',
            'n_folds': 1,
            # 'case_mode': '>',
            # 'case_val': 1, # zero should predict to 0, 2-7 should predict to 1
            'max_iter': max_iter,
            'beta_epsilon': 1e-3}

        timeoutSecs = 120

        aHack = {'destination_key': 'A.hex'}
        if 1==0:
            start = time.time()
            kwargs.update({'alpha': 0, 'lambda': 0})
            glm = h2o_cmd.runGLM(parseResult=aHack, timeoutSecs=timeoutSecs, **kwargs)
            print "glm (L2) end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, 'C13', **kwargs)
            modelKey = glm['glm_model']['_selfKey']
            predict_and_compare_csvs(model_key=modelKey)

            # Elastic
            kwargs.update({'alpha': 0.5, 'lambda': 1e-4})
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=aHack, timeoutSecs=timeoutSecs, **kwargs)
            print "glm (Elastic) end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o_glm.simpleCheckGLM(self, glm, 'C13', **kwargs)
            modelKey = glm['glm_model']['_selfKey']
            predict_and_compare_csvs(model_key=modelKey)

        # L1
        kwargs.update({'alpha': 1, 'lambda': 1e-4})
        start = time.time()
        glm = h2o_cmd.runGLM(parseResult=aHack, timeoutSecs=timeoutSecs, **kwargs)
        print "glm (L1) end on ", csvPathname, 'took', time.time() - start, 'seconds'
        h2o_glm.simpleCheckGLM(self, glm, 'C13', **kwargs)
        modelKey = glm['glm_model']['_selfKey']
        predict_and_compare_csvs(model_key=modelKey)

if __name__ == '__main__':
    h2o.unit_main()
