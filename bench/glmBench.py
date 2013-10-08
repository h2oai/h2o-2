#GLM bench
import os, sys, time, csv
sys.path.append('../py/')
sys.path.extend(['.','..'])
import h2o_cmd, h2o, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_rf

csv_header = ('java_heap_GB','dataset','nTrainRows','nTestRows','nCols','trainParseWallTime','nfolds','glmBuildTime','testParseWallTime','scoreTime','AUC','error','AverageAccuracy')

files      = {'Airlines'    : {'train': ('AirlinesTrain1x', 'AirlinesTrain10x', 'AirlinesTrain100x'),          'test' : 'AirlinesTest'},
              'AllBedrooms' : {'train': ('AllBedroomsTrain1x', 'AllBedroomsTrain10x', 'AllBedroomsTrain100x'), 'test' : 'AllBedroomsTest'},
             }
def doGLM(fs, folderPath, family, link, lambda_, alpha, nfolds, y, x, testFilehex, row):
    for (f in fs['train']):
        overallWallStart = time.time()
        if not os.path.exists('glmbench.csv'):
            output = open('glmbench.csv','w')
            output.write(','.join(csv_header)+'\n')
        else:
            output = open('glmbench.csv','a')
        csvWrt = csv.DictWriter(output, fieldnames=csv_header, restval=None, 
                        dialect='excel', extrasaction='ignore',delimiter=',')
        try:
            java_heap_GB = h2o.nodes[0].java_heap_GB
            #Train File Parsing#
            importFolderPath = "bench-test/" + folderPath
            if (f in ['AirlinesTrain1x','AllBedroomsTrain1x', 'AllBedroomsTrain10x', 'AllBedroomsTrain100x']): csvPathname = importFolderPath + "/" + f + '.csv'
            else: csvPathname = importFolderPath + "/*linked*"
            hex_key = f + '.hex'
            trainParseWallStart = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', hex_key=hex_key,
                timeoutSecs=3600,retryDelaySecs=5,pollTimeoutSecs=3600)
            parseWallTime = time.time() - trainParseWallStart
            #End Train File Parse#
            print "Parsing training file took ", trainParseWallTime ," seconds." 
            
            inspect  = h2o.nodes[0].inspect(parseResult['destination_key'])
            
            row.update( {'java_heap_GB'       : java_heap_GB,
                         'dataset'            : f,
                         'nRows'              : inspect['num_rows'],
                         'nCols'              : inspect['num_cols'],
                         'ParseWallTime'      : parseWallTime,
                        })
        
            params   =  {'key'                : hex_key,
                         'y'                  : y,
                         'x'                  : x,
                         'family'             : family,
                         'link'               : link,
                         'lambda'             : lambda_,
                         'alpha'              : alpha,
                         'n_folds'            : nfolds,
                         'destination_key'    : "python_GLM_key",
                        }

            kwargs    = params.copy()
            glmStart  = time.time()
            glm       = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            glmTime   = time.time() - glmStart
            row.update( {'glmBuildTime'       : glmTime,
                         'AverageAccuracy'    : glm['validations']['err'],
                        })
            
            glmScoreStart = time.time()
            glmScore = h2o_cmd.runGLMScore(key=testFilehex,model_key=glm['GLMModel'])
            scoreTime = time.time() - glmScoreStart

            row.update( {'scoreTime'          : scoreTime,
                         'AUC'                : glmScore['validation']['auc'],
                         'error'              : glmScore['validation']['err'],
                        })
            csvWrt.writerow(row)
        finally:
            output.close()

if __name__ == '__main__':
    h2o_hosts.build_cloud_with_hosts()
    #Test File parse#
    airlinesTestParseStart      = time.time()
    testFile                    = h2o.import_parse(bucket='home-0xdiag-datasets', path='bench/Airlines/AirlinesTest.csv', schema-'local', hex_key="atest.hex",timeoutSecs=3600,retryDelaySecs=5, pollTimeoutSecs=3600)
    elapsedAirlinesTestParse    = time.time() - airlinesTestParseStart
    
    row = {'testParseWallTime' : elapsedAirlinesTestParse}
    doGLM(files['Airlines'], 'Airlines', 'binomial', 'logit', 1E-5, 0.5, 10, 'IsDepDelayed', 'Year,Month,DayofMonth,DayOfWeek,DepTime,ArrTime,UniqueCarrier,FlightNum,TailNum,Origin,Dest,Distance',testFile['destination_key'],row)

    allBedroomsTestParseStart   = time/time()
    testFile                    = h2o.import_parse(bucket='home-0xdiag-datasets', path='bench/AllBedrooms/AllBedroomsTest.csv', schema-'local', hex_key="allBtest.hex",timeoutSecs=3600,retryDelaySecs=5, pollTimeoutSecs=3600)
    elapsedAllBedroomsTestParse = time.time() - allBedroomsTestParseStart
    
    row = {'testParseWallTime' : elapsedAllBedroomsTestParse}
    doGLM(files['AllBedrooms'], 'AllBedrooms', 'gaussian', 'identity', 1E-5, 0.5, 10, 'medrent',x, testFile['destination_key'],row)

    h2o.tear_down_cloud()
