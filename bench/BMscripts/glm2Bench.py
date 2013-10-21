#GLM2 bench
import os, sys, time, csv
sys.path.append('../py/')
sys.path.extend(['.','..'])
import h2o_cmd, h2o, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_rf, h2o_jobs
from pprint import pprint

csv_header = ('h2o_build','java_heap_GB','dataset','nTrainRows','nTestRows','nCols','nPredictors','trainParseWallTime','nfolds','glmBuildTime','testParseWallTime','nIterations','AUC','AIC','AverageError')

files      = {'Airlines'    : {'train': ('AirlinesTrain1x', 'AirlinesTrain10x', 'AirlinesTrain100x'),          'test' : 'AirlinesTest'},
              'AllBedrooms' : {'train': ('AllBedroomsTrain1x', 'AllBedroomsTrain10x', 'AllBedroomsTrain100x'), 'test' : 'AllBedroomsTest'},
             }
build = ""
debug = False
def doGLM2(f, folderPath, family, lambda_, alpha, nfolds, y, x, testFilehex, row, case_mode, case_val):
    debug = False
    bench = "bench"
    if debug:
        print "DOING GLM2 DEBUG"
        bench = "bench/debug"
    date = '-'.join([str(z) for z in list(time.localtime())][0:3])
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
        importFolderPath = bench+"/" + folderPath
        if (f in ['AirlinesTrain1x','AllBedroomsTrain1x', 'AllBedroomsTrain10x', 'AllBedroomsTrain100x']): 
            csvPathname = importFolderPath + "/" + f + '.csv'
        else:
            #print "Not doing Airlines10x and 100x for Parse2, regex seems to be broken..." 
            #continue
            csvPathname = importFolderPath + "/" + f + "/*"
        hex_key         = f + '.hex'
        hK              = folderPath + "Header.csv"    
        headerPathname  = importFolderPath + "/" + hK
        h2i.import_only(bucket='home-0xdiag-datasets', path=headerPathname)
        headerKey       = h2i.find_key(hK)
        trainParseWallStart = time.time()
        if f in (['AirlinesTrain10x', 'AirlinesTrain100x']): h2o.beta_features = False #regex parsing acting weird when not using browser, use VA -> FVEC converter
        parseResult = h2i.import_parse(bucket           = 'home-0xdiag-datasets',
                                       path             = csvPathname,
                                       schema           = 'local',
                                       hex_key          = hex_key,
                                       header           = 1,
                                       header_from_file = headerKey,
                                       separator        = 44,
                                       timeoutSecs      = 7200,
                                       retryDelaySecs   = 5,
                                       pollTimeoutSecs  = 7200,
                                       noPoll           = True,
                                       doSummary        = False
                                      )
        h2o_jobs.pollWaitJobs(timeoutSecs=7200, pollTimeoutSecs=7200, retryDelaySecs=5)
        parseResult = {'destination_key':hex_key}
        parseWallTime = time.time() - trainParseWallStart
        print "Parsing training file took ", parseWallTime ," seconds." 
        h2o.beta_features = True
        inspect_train  = h2o.nodes[0].inspect(hex_key, timeoutSecs=7200)
        inspect_test   = h2o.nodes[0].inspect(testFilehex, timeoutSecs=7200)
        
        row.update( {'h2o_build'          : build,  
                     'java_heap_GB'       : java_heap_GB,
                     'dataset'            : f,
                     'nTrainRows'         : inspect_train['numRows'],
                     'nTestRows'          : inspect_test['numRows'],
                     'nCols'              : inspect_train['numCols'],
                     'trainParseWallTime' : parseWallTime,
                     'nfolds'             : nfolds,
                    })
    
        params   =  {'vresponse'       : y,
                     'ignored_cols'    : x,
                     'family'          : family,
                     'lambda'          : lambda_,
                     'alpha'           : alpha,
                     'n_folds'         : nfolds,
                     #'case_mode'          : case_mode,
                     #'case_val'           : case_val, 
                     'destination_key' : "GLM("+f+")",
                    }
        h2o.beta_features = True
        kwargs    = params.copy()
        glmStart  = time.time()
        glm       = h2o_cmd.runGLM(parseResult = parseResult, noPoll=True, **kwargs)
        h2o_jobs.pollWaitJobs(timeoutSecs=7200, pollTimeoutSecs=7200, retryDelaySecs=5)
        glmTime   = time.time() - glmStart
        #glm       = h2o.nodes[0].inspect("GLM("+f+")")
        row.update( {'glmBuildTime'       : glmTime,
                     #'AverageErrorOver10Folds'    : glm['glm_model']['validations'][0]['err'],
                    })
        #if "Bedrooms" in f: 
            #print "Sleeping 30"
            #time.sleep(30)
        glmView = h2o_cmd.runGLMView(modelKey = "GLM("+f+")", timeoutSecs=380)

        #glmScoreStart = time.time()
        #glmScore      = h2o_cmd.runGLMScore(key=testFilehex,model_key=params['destination_key'])
        #scoreTime     = time.time() - glmScoreStart
        row.update( {'AIC'          : glmView['glm_model']['validation']['aic'],
                     'nIterations'  : glmView['glm_model']['iteration'],
                     'nPredictors'  : len(glmView['glm_model']['beta']),
                     'AverageError' : glmView['glm_model']['validation']['avg_err'],
                    })
        if family == "binomial":
            row.update( {#'scoreTime'          : scoreTime,
                         'AUC'                : glmView['glm_model']['validation']['auc'],
                        })
        else:
            row.update( {#'scoreTime'          : scoreTime,
                         'AUC'                : 'NA',
                        })
        csvWrt.writerow(row)
    finally:
        output.close()

if __name__ == '__main__':
    debug = sys.argv.pop(-1)
    build = sys.argv.pop(-1)
    h2o.parse_our_args()
    h2o_hosts.build_cloud_with_hosts()
    h2o.beta_features = True
    bench = "bench"
    if debug:
        bench = "bench/debug"
    #AIRLINES
    airlinesTestParseStart      = time.time()
    hK                          =  "AirlinesHeader.csv"
    headerPathname              = bench+"/Airlines" + "/" + hK
    h2i.import_only(bucket      = 'home-0xdiag-datasets', path=headerPathname)
    headerKey                   = h2i.find_key(hK)
    testFile                    = h2i.import_parse(bucket='home-0xdiag-datasets', path=bench+'/Airlines/AirlinesTest.csv', schema='local', hex_key="atest.hex", header=1, header_from_file=headerKey, separator=44, noPoll = True, doSummary = False)
    h2o_jobs.pollWaitJobs(timeoutSecs=7200, pollTimeoutSecs=7200, retryDelaySecs=5)
    elapsedAirlinesTestParse    = time.time() - airlinesTestParseStart
    
    row = {'testParseWallTime' : elapsedAirlinesTestParse}
    x = None #"DepTime,ArrTime,FlightNum,TailNum,ActualElapsedTime,AirTime,ArrDelay,DepDelay,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,IsArrDelayed" #columns to be ignored
    doGLM2(files['Airlines'], 'Airlines', 
            family      = 'binomial',
            lambda_     = 1E-5, 
            alpha       = 0.5, 
            nfolds      = 10, 
            y           = 'IsDepDelayed',
            x           = x,  
            testFilehex = 'atest.hex',
            row         = row,
            case_mode   = "%3D",
            case_val    = 1.0 
          ) 
    
    #ALLBEDROOMS
    allBedroomsTestParseStart   = time.time()
    hK                          =  "AllBedroomsHeader.csv"
    headerPathname              = bench+"/AllBedrooms" + "/" + hK
    h2i.import_only(bucket='home-0xdiag-datasets', path=headerPathname)
    headerKey                   = h2i.find_key(hK)

    testFile                    = h2i.import_parse(bucket='home-0xdiag-datasets', path=bench+'/AllBedrooms/AllBedroomsTest.csv', schema='local', hex_key="allBtest.hex", header=1, header_from_file=headerKey, separator=44, noPoll = True, doSummary = False)
    h2o_jobs.pollWaitJobs(timeoutSecs=7200, pollTimeoutSecs=7200, retryDelaySecs=5)
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
            testFilehex = 'allBtest.hex', 
            row         = row,
            case_mode   = "n/a",
            case_val    = 0.0
          )
    h2o.tear_down_cloud()
