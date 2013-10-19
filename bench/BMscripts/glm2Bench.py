#GLM2 bench
import os, sys, time, csv
sys.path.append('../py/')
sys.path.extend(['.','..'])
import h2o_cmd, h2o, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_rf, h2o_jobs

csv_header = ('h2o_build','java_heap_GB','dataset','nTrainRows','nTestRows','nCols','trainParseWallTime','nfolds','glmBuildTime','testParseWallTime','scoreTime','AUC','AIC','error','AverageErrorOver10Folds')

files      = {'Airlines'    : {'train': ('AirlinesTrain1x', 'AirlinesTrain10x', 'AirlinesTrain100x'),          'test' : 'AirlinesTest'},
              'AllBedrooms' : {'train': ('AllBedroomsTrain1x', 'AllBedroomsTrain10x', 'AllBedroomsTrain100x'), 'test' : 'AllBedroomsTest'},
             }
build = ""
debug = False
def doGLM2(fs, folderPath, family, lambda_, alpha, nfolds, y, x, testFilehex, row, case_mode, case_val):
    bench = "bench"
    if debug:
        print "DOING GLM2 DEBUG"
        bench = "bench/debug"
    date = '-'.join([str(z) for z in list(time.localtime())][0:3])
    for f in fs['train']:
        overallWallStart  = time.time()
        pre               = ""
        if debug: pre     = "DEBUG"
        glm2benchcsv      = 'benchmarks/'+build+'/'+date+'/'+pre+'glm2bench.csv'
        if not os.path.exists(glm2benchcsv):
            output = open(glm2benchcsv,'w')
            output.write(','.join(csv_header)+'\n')
        else:
            output = open(glm2benchcsv,'a')
        csvWrt = csv.DictWriter(output, fieldnames=csv_header, restval=None, 
                        dialect='excel', extrasaction='ignore',delimiter=',')
        try:
            java_heap_GB     = h2o.nodes[0].java_heap_GB
            importFolderPath = "bench/" + folderPath
            if (f in ['AirlinesTrain1x','AllBedroomsTrain1x', 'AllBedroomsTrain10x', 'AllBedroomsTrain100x']): 
                csvPathname = importFolderPath + "/" + f + '.csv'
            else: 
                csvPathname = importFolderPath + "/" + f + "/*linked*"
            hex_key         = f + '.hex'
            hK              = folderPath + "Header.csv"    
            headerPathname  = importFolderPath + "/" + hK
            h2i.import_only(bucket='home-0xdiag-datasets', path=headerPathname)
            headerKey       = h2i.find_key(hK)
            trainParseWallStart = time.time()
            parseResult = h2i.import_parse(bucket           = 'home-0xdiag-datasets',
                                           path             = csvPathname,
                                           schema           = 'local',
                                           hex_key          = hex_key,
                                           header           = 1,
                                           header_from_file = headerKey,
                                           separator        = 44,
                                           timeoutSecs      = 7200,
                                           retryDelaySecs   = 5,
                                           pollTimeoutSecs  = 7200
                                          )
            parseWallTime = time.time() - trainParseWallStart
            print "Parsing training file took ", parseWallTime ," seconds." 
            
            inspect_train  = h2o.nodes[0].inspect(parseResult['destination_key'])
            inspect_test   = h2o.nodes[0].inspect(testFilehex)
            
            row.update( {'h2o_build'          : build,  
                         'java_heap_GB'       : java_heap_GB,
                         'dataset'            : f,
                         'nTrainRows'         : inspect_train['numRows'],
                         'nTestRows'          : inspect_test['numRows'],
                         'nCols'              : inspect_train['numCols'],
                         'trainParseWallTime' : parseWallTime,
                         'nfolds'             : nfolds,
                        })
        
            params   =  {'vresponse'          : y,
                         'ignored_cols'       : x,
                         'family'             : family,
                         'lambda'             : lambda_,
                         'alpha'              : alpha,
                         'n_folds'            : nfolds,
                         #'case_mode'          : case_mode,
                         #'case_val'           : case_val, 
                         'destination_key'    : "GLM("+f+")",
                        }
            h2o.beta_features = True
            kwargs    = params.copy()
            glmStart  = time.time()
            glm       = h2o_cmd.runGLM(parseResult = parseResult, timeoutSecs=7200, **kwargs)
            glmTime   = time.time() - glmStart
            row.update( {'glmBuildTime'       : glmTime,
                         'AverageErrorOver10Folds'    : glm['glm_model']['validations'][0]['err'],
                        })
            
            glmScoreStart = time.time()
            glmScore      = h2o_cmd.runGLMScore(key=testFilehex,model_key=params['destination_key'])
            scoreTime     = time.time() - glmScoreStart
            if family == "binomial":
                row.update( {'scoreTime'          : scoreTime,
                             'AUC'                : glmScore['validation']['auc'],
                             'AIC'                : glmScore['validation']['aic'],
                             'error'              : glmScore['validation']['err'],
                            })
            else:
                row.update( {'scoreTime'          : scoreTime,
                             'AIC'                : glmScore['validation']['aic'],
                             'AUC'                : 'NA',
                             'error'              : glmScore['validation']['err'],
                            })
            csvWrt.writerow(row)
        finally:
            output.close()

if __name__ == '__main__':
    build = sys.argv.pop(-1)
    debug = sys.argv.pop(-1)
    h2o.beta_features = True
    h2o.parse_our_args()
    h2o_hosts.build_cloud_with_hosts()
    
    #AIRLINES
    airlinesTestParseStart      = time.time()
    hK                          =  "AirlinesHeader.csv"
    headerPathname              = "bench/Airlines" + "/" + hK
    h2i.import_only(bucket      = 'home-0xdiag-datasets', path=headerPathname)
    headerKey                   = h2i.find_key(hK)
    testFile                    = h2i.import_parse(bucket='home-0xdiag-datasets', path='bench/Airlines/AirlinesTest.csv', schema='local', hex_key="atest.hex", header=1, header_from_file=headerKey, separator=44,
                                  timeoutSecs=7200,retryDelaySecs=5, pollTimeoutSecs=7200)
    elapsedAirlinesTestParse    = time.time() - airlinesTestParseStart
    
    row = {'testParseWallTime' : elapsedAirlinesTestParse}
    x = "DepTime,ArrTime,FlightNum,TailNum,ActualElapsedTime,AirTime,ArrDelay,DepDelay,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,IsArrDelayed" #columns to be ignored
    doGLM2(files['Airlines'], 'Airlines', 
            family      = 'binomial',
            lambda_     = 1E-5, 
            alpha       = 0.5, 
            nfolds      = 10, 
            y           = 'IsDepDelayed',
            x           = x,  
            testFilehex = testFile['destination_key'],
            row         = row,
            case_mode   = "%3D",
            case_val    = 1.0 
          ) 
    
    #ALLBEDROOMS
    allBedroomsTestParseStart   = time.time()
    hK                          =  "AllBedroomsHeader.csv"
    headerPathname              = "bench/AllBedrooms" + "/" + hK
    h2i.import_only(bucket='home-0xdiag-datasets', path=headerPathname)
    headerKey                   = h2i.find_key(hK)

    testFile                    = h2i.import_parse(bucket='home-0xdiag-datasets', path='bench/AllBedrooms/AllBedroomsTest.csv', schema='local', hex_key="allBtest.hex", header=1, header_from_file=headerKey, separator=44,
                                  timeoutSecs=7200,retryDelaySecs=5, pollTimeoutSecs=7200)

    elapsedAllBedroomsTestParse = time.time() - allBedroomsTestParseStart
    
    row = {'testParseWallTime' : elapsedAllBedroomsTestParse}
    x = "county,place,Rent_Type,mcd" #columns to be ignored
    doGLM2(files['AllBedrooms'], 'AllBedrooms', 
            family      = 'gaussian',
            lambda_     = 1E-4, 
            alpha       = 0.75, 
            nfolds      = 10, 
            y           = 'medrent',
            x           = x, 
            testFilehex = testFile['destination_key'],
            row         = row,
            case_mode   = "n/a",
            case_val    = 0.0
          )

    h2o.tear_down_cloud()
