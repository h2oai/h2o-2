#GBM bench
import os, sys, time, csv
sys.path.append('../py/')
sys.path.extend(['.','..'])
import h2o_cmd, h2o, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_rf

csv_header = ('h2o_build','java_heap_GB','dataset','nTrainRows','nTestRows','nCols','trainParseWallTime','classification','gbmBuildTime')

files      = {'Airlines'    : {'train': ('AirlinesTrain1x', 'AirlinesTrain10x', 'AirlinesTrain100x'),         'test' : 'AirlinesTest'},
              'AllBedrooms': {'train': ('AllBedroomsTrain1x', 'AllBedroomsTrain10x', 'AllBedroomsTrain100x'), 'test' : 'AllBedroomsTest'},
              'Covtype'     : {'train': ('CovTypeTrain1x', 'CovTypeTrain10x', 'CovTypeTrain100x'),            'test' : 'CovTypeTest'},
             }
header = ""

def doGLM(fs, folderPath, ignored_cols, classification, testFilehex, ntrees, depth, minrows, nbins, learnRate, response):
    for f in fs['train']:
        overallWallStart = time.time()
        date = '-'.join([str(x) for x in list(time.localtime())][0:3])
        gbmbenchcsv = 'benchmarks/'+build+'/'+date+'/gbmbench.csv'
        if not os.path.exists(gbmbenchcsv):
            output = open(gbmbenchcsv,'w')
            output.write(','.join(csv_header)+'\n')
        else:
            output = open(gbmbenchcsv,'a')
        csvWrt = csv.DictWriter(output, fieldnames=csv_header, restval=None, 
                        dialect='excel', extrasaction='ignore',delimiter=',')
        try:
            java_heap_GB = h2o.nodes[0].java_heap_GB
            importFolderPath = "bench/" + folderPath
            if (f in ['AirlinesTrain1x','CovTypeTrain1x', 'CovTypeTrain10x', 'CovTypeTrain100x']): csvPathname = importFolderPath + "/" + f + '.csv'
            else: csvPathname = importFolderPath + "/" + f + "/*linked*"
            hex_key = f + '.hex'
            hK = folderPath + "Header.csv"    
            headerPathname = importFolderPath + "/" + hK
            h2i.import_only(bucket='home-0xdiag-datasets', path=headerPathname)
            headerKey =h2i.find_key(hK)
            trainParseWallStart = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', hex_key=hex_key, header=1, header_from_file=headerKey, separator=44,
                timeoutSecs=4800,retryDelaySecs=5,pollTimeoutSecs=4800)
            parseWallTime = time.time() - trainParseWallStart
            print "Parsing training file took ", parseWallTime ," seconds." 
            
            inspect_train  = h2o.nodes[0].inspect(parseResult['destination_key'])
            inspect_test   = h2o.nodes[0].inspect(testFilehex)

            row.update( {'h2o_build'          : build,  
                         'java_heap_GB'       : java_heap_GB,
                         'dataset'            : f,
                         'nTrainRows'         : inspect_train['num_rows'],
                         'nTestRows'          : inspect_test['num_rows'],
                         'nCols'              : inspect_train['num_cols'],
                         'trainParseWallTime' : parseWallTime,
                         'classification'     : classification,
                        })
        
            params   =  {'destination_key'      : 'GBM('+f+')',
                         'response'             : response,
                         'ignored_cols_by_name' : ignored_cols,
                         'classification'       : classification,
                         'validation'           : testFilehex,
                         'ntrees'               : ntrees,
                         'max_depth'            : depth,
                         'min_rows'             : minrows,
                         'nbins'                : nbins,
                         'learn_rate'           : learnRate,
                        }

            kwargs    = params.copy()
            gbmStart  = time.time()
            gbm       = h2o_cmd.runGBM(parseResult = parseResult, timeoutSecs=4800, **kwargs)
            gbmTime   = time.time() - gbmStart
            row.update( {'gbmBuildTime'       : gbmTime,
                        })
            #TODO(spencer): Add in gbm scoring
            #gbmScoreStart = time.time()
            #gbmScore      = h2o_cmd.runGLMScore(key=testFilehex,model_key=params['destination_key'])
            #scoreTime     = time.time() - gbmScoreStart
            #csvWrt.writerow(row)
        finally:
            output.close()

if __name__ == '__main__':
    build = sys.argv.pop(-1)
    h2o.parse_our_args()
    h2o_hosts.build_cloud_with_hosts()
    
    #AIRLINES
    airlinesTestParseStart      = time.time()
    hK                          =  "AirlinesHeader.csv"
    headerPathname              = "bench/Airlines" + "/" + hK
    h2i.import_only(bucket='home-0xdiag-datasets', path=headerPathname)
    headerKey                   = h2i.find_key(hK)
    testFile                    = h2i.import_parse(bucket='home-0xdiag-datasets', path='bench/Airlines/AirlinesTest.csv', schema='local', hex_key="atest.hex", header=1, header_from_file=headerKey, separator=44,
                                  timeoutSecs=4800,retryDelaySecs=5, pollTimeoutSecs=4800)
    elapsedAirlinesTestParse    = time.time() - airlinesTestParseStart
    row = {'testParseWallTime' : elapsedAirlinesTestParse}
    response = 'IsDepDelayed'
    ignored  = None
    doGBM(files['Airlines'], folderPath='Airlines', 
            ignored_cols    = ignored, 
            classificiation = 1,
            testFilehex     = testFile['destination_key'], 
            ntrees          = 100,
            depth           = 5,
            minrows         = 10,
            nbins           = 100,
            learnRate       = 0.01
         )
    
    #COVTYPE
    covTypeTestParseStart   = time.time()
    hK                          =  "CovTypeHeader.csv"
    headerPathname              = "bench/CovType" + "/" + hK
    h2i.import_only(bucket='home-0xdiag-datasets', path=headerPathname)
    headerKey                   = h2i.find_key(hK)
    testFile                    = h2i.import_parse(bucket='home-0xdiag-datasets', path='bench/CovType/CovTypeTest.csv', schema='local', hex_key="covTtest.hex", header=1, header_from_file=headerKey, separator=44,
                                  timeoutSecs=4800,retryDelaySecs=5, pollTimeoutSecs=4800)
    elapsedCovTypeTestParse = time.time() - covTypeTestParseStart
    row = {'testParseWallTime' : elapsedCovTypeTestParse}
    response = 'C55'
    ignored  = None
    doGBM(files['Covtype'], folderPath='CovType', 
            ignored_cols    = ignored, 
            classificiation = 1,
            testFilehex     = testFile['destination_key'], 
            ntrees          = 100,
            depth           = 5,
            minrows         = 10, 
            nbins           = 100,
            learnRate       = 0.01,
            response        = response
         ) 
    
    #ALLBEDROOMS
    allBedroomsTestParseStart   = time.time()
    hK                          =  "AllBedroomsHeader.csv"
    headerPathname              = "bench/AllBedrooms" + "/" + hK
    h2i.import_only(bucket='home-0xdiag-datasets', path=headerPathname)
    headerKey                   = h2i.find_key(hK)
    testFile                    = h2i.import_parse(bucket='home-0xdiag-datasets', path='bench/AllBedrooms/AllBedroomsTest.csv', schema='local', hex_key="allBTest.hex", header=1, header_from_file=headerKey, separator=44,
                                  timeoutSecs=4800,retryDelaySecs=5, pollTimeoutSecs=4800)
    elapsedAllBedroomsParse = time.time() - allBedroomsTestParseStart
    row = {'testParseWallTime' : elapsedAllBedroomsTestParse}
    response = 'medrent'
    ignored  = None
    doGBM(files['AllBedroom'], folderPath='AllBedrooms',
            ignored_cols    = ignored,
            classificiation = 0,
            testFilehex     = testFile['destination_key'],
            ntrees          = 100,
            depth           = 5,
            minrows         = 10,
            nbins           = 100,
            learnRate       = 0.01,
            response        = response
         )

    h2o.tear_down_cloud()
