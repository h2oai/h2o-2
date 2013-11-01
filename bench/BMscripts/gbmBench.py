#GBM bench
import os, sys, time, csv, string
sys.path.append('../py/')
sys.path.extend(['.','..'])
import h2o_cmd, h2o, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_rf, h2o_jobs

csv_header = ('h2o_build','nMachines','nJVMs','Xmx/JVM','dataset','nTrainRows','nTestRows','nCols','trainParseWallTime','nTrees','minRows','maxDepth','learnRate','classification','gbmBuildTime','Error')

files      = {'Airlines'    : {'train': ('AirlinesTrain1x', 'AirlinesTrain10x', 'AirlinesTrain100x'),         'test' : 'AirlinesTest'},
              'AllBedrooms': {'train': ('AllBedroomsTrain1x', 'AllBedroomsTrain10x', 'AllBedroomsTrain100x'), 'test' : 'AllBedroomsTest'},
              'Covtype'     : {'train': ('CovTypeTrain1x', 'CovTypeTrain10x', 'CovTypeTrain100x'),            'test' : 'CovTypeTest'},
             }
build = ""
debug = False
json  = ""
def doGBM(f, folderPath, ignored_cols, classification, testFilehex, ntrees, depth, minrows, nbins, learnRate, response, row):
    debug = False
    bench = "bench"
    if debug:
        print "Doing GBM DEBUG"
        bench = "bench/debug"
    #date = '-'.join([str(x) for x in list(time.localtime())][0:3])
    overallWallStart = time.time()
    pre = ""
    if debug: pre    = 'DEBUG'
    gbmbenchcsv = 'benchmarks/'+build+'/'+pre+'gbmbench.csv'
    if not os.path.exists(gbmbenchcsv):
        output = open(gbmbenchcsv,'w')
        output.write(','.join(csv_header)+'\n')
    else:
        output = open(gbmbenchcsv,'a')
    csvWrt = csv.DictWriter(output, fieldnames=csv_header, restval=None, 
                    dialect='excel', extrasaction='ignore',delimiter=',')
    try:
        java_heap_GB = h2o.nodes[0].java_heap_GB
        importFolderPath = bench + "/" + folderPath
        if (f in ['AirlinesTrain1x','AllBedroomsTrain1x', 'AllBedroomsTrain10x', 'AllBedroomsTrain100x','CovTypeTrain1x', 'CovTypeTrain10x', 'CovTypeTrain100x']): 
            csvPathname = importFolderPath + "/" + f + '.csv'
        else: 
            csvPathname = importFolderPath + "/" + f + "/*linked*"
        hex_key = f + '.hex'
        hK = folderPath + "Header.csv"    
        headerPathname = importFolderPath + "/" + hK
        h2i.import_only(bucket='home-0xdiag-datasets', path=headerPathname)
        headerKey = h2i.find_key(hK)
        trainParseWallStart = time.time()
        h2o.beta_features = False #ensure this is false! 
        if f in (['AirlinesTrain10x', 'AirlinesTrain100x']): h2o.beta_features = False #regex parsing acting weird when not using browser, use VA -> FVEC converter
        parseResult = h2i.import_parse(bucket           = 'home-0xdiag-datasets',
                                       path             = csvPathname,
                                       schema           = 'local',
                                       hex_key          = hex_key,
                                       header           = 1,
                                       header_from_file = headerKey,
                                       separator        = 44,
                                       timeoutSecs      = 16000,
                                       retryDelaySecs   = 5,
                                       pollTimeoutSecs  = 16000,
                                       noPoll           = True,
                                       doSummary        = False
                                      )
        h2o_jobs.pollWaitJobs(timeoutSecs=16000, pollTimeoutSecs=16000, retryDelaySecs=5)
        parseWallTime = time.time() - trainParseWallStart
        print "Parsing training file took ", parseWallTime ," seconds." 
        h2o.beta_features = False #make sure false for the inspect as well!
        inspect_train  = h2o.nodes[0].inspect(hex_key, timeoutSecs=16000)
        inspect_test   = h2o.nodes[0].inspect(testFilehex, timeoutSecs=16000)
        h2o.beta_features = True #ok, can be true again
        nMachines = 1 if len(h2o_hosts.hosts) is 0 else len(h2o_hosts.hosts)
        row.update( {'h2o_build'          : build,
                     'nMachines'          : nMachines,
                     'nJVMs'              : len(h2o.nodes),
                     'Xmx/JVM'            : java_heap_GB,
                     'dataset'            : f,
                     'nTrainRows'         : inspect_train['num_rows'],
                     'nTestRows'          : inspect_test['num_rows'],
                     'nCols'              : inspect_train['num_cols'],
                     'trainParseWallTime' : parseWallTime,
                     'nTrees'             : ntrees,
                     'minRows'            : minrows,
                     'maxDepth'           : depth,
                     'learnRate'          : learnRate,
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
    
        parseResult = {'destination_key' : hex_key}
        kwargs    = params.copy()
        gbmStart  = time.time()
        #TODO(spencer): Uses jobs to poll for gbm completion
        gbm       = h2o_cmd.runGBM(parseResult = parseResult, noPoll=True, timeoutSecs=4800, **kwargs)
        h2o_jobs.pollWaitJobs(timeoutSecs=16000, pollTimeoutSecs=120, retryDelaySecs=5)
        gbmTime   = time.time() - gbmStart
        cmd = 'bash startloggers.sh ' + json + ' stop_'
        os.system(cmd)
        row.update( {'gbmBuildTime'       : gbmTime,
                    })
        gbmTrainView = h2o_cmd.runGBMView(model_key='GBM('+f+')')
        if classification:
            cm = gbmTrainView['gbm_model']['cm']
            err = 1.0*(cm[0][1] + cm[1][0]) / (cm[0][0] + cm[0][1] + cm[1][0] + cm[1][1])
        else:
            err = gbmTrainView['gbm_model']['errs'][-1]
        row.update({'Error' : err})
        csvWrt.writerow(row)
    finally:
        output.close()

if __name__ == '__main__':
    dat   = sys.argv.pop(-1)
    debug = sys.argv.pop(-1)
    build = sys.argv.pop(-1)
    json  = sys.argv[-1].split('/')[-1]
    h2o.parse_our_args()
    h2o_hosts.build_cloud_with_hosts(enable_benchmark_log=False)
    fp    = 'Airlines' if 'Air' in dat else 'AllBedrooms'
    bench = "bench"
    h2o.beta_features = True
    debug = False
    if debug:
        bench = "bench/debug"

    if dat == 'Air1x'    : fs = files['Airlines']['train'][0]
    if dat == 'Air10x'   : fs = files['Airlines']['train'][1]
    if dat == 'Air100x'  : fs = files['Airlines']['train'][2]
    if dat == 'AllB1x'   : fs = files['AllBedrooms']['train'][0]
    if dat == 'AllB10x'  : fs = files['AllBedrooms']['train'][1]
    if dat == 'AllB100x' : fs = files['AllBedrooms']['train'][2]

    if fp == "Airlines":
        #AIRLINES
        airlinesTestParseStart      = time.time()
        hK                          =  "AirlinesHeader.csv"
        headerPathname              = bench+"/Airlines" + "/" + hK
        h2i.import_only(bucket='home-0xdiag-datasets', path=headerPathname)
        headerKey                   = h2i.find_key(hK)
        testFile                    = h2i.import_parse(bucket='home-0xdiag-datasets', path=bench+'/Airlines/AirlinesTest.csv', schema='local', hex_key="atest.hex", header=1, header_from_file=headerKey, separator=44, noPoll=True,doSummary=False)
        h2o_jobs.pollWaitJobs(timeoutSecs=16000, pollTimeoutSecs=16000, retryDelaySecs=5)
        elapsedAirlinesTestParse    = time.time() - airlinesTestParseStart
        row = {'testParseWallTime' : elapsedAirlinesTestParse}
        response = 'IsDepDelayed'
        ignored  = None
        doGBM(fs, fp,
                ignored_cols    = ignored, 
                classification  = 1,
                testFilehex     = 'atest.hex',
                ntrees          = 100,
                depth           = 5,
                minrows         = 10,
                nbins           = 100,
                learnRate       = 0.01,
                response        = response,
                row             = row
             )
    
    if fp == "AllBedrooms":    
        #ALLBEDROOMS
        allBedroomsTestParseStart   = time.time()
        hK                          =  "AllBedroomsHeader.csv"
        headerPathname              = bench+"/AllBedrooms" + "/" + hK
        h2i.import_only(bucket='home-0xdiag-datasets', path=headerPathname)
        headerKey                   = h2i.find_key(hK)
        testFile                    = h2i.import_parse(bucket='home-0xdiag-datasets', path=bench+'/AllBedrooms/AllBedroomsTest.csv', schema='local', hex_key="allBTest.hex", header=1, header_from_file=headerKey, separator=44,noPoll=True,doSummary=False)
        h2o_jobs.pollWaitJobs(timeoutSecs=16000, pollTimeoutSecs=16000, retryDelaySecs=5)
        elapsedAllBedroomsTestParse = time.time() - allBedroomsTestParseStart
        row = {'testParseWallTime' : elapsedAllBedroomsTestParse}
        response = 'medrent'
        ignored  = None
        doGBM(fs, fp,
                ignored_cols    = ignored,
                classification  = 0,
                testFilehex     = "allBTest.hex",
                ntrees          = 100,
                depth           = 5,
                minrows         = 10,
                nbins           = 100,
                learnRate       = 0.01,
                response        = response,
                row             = row 
             )
            
    #COVTYPE
    #covTypeTestParseStart   = time.time()
    #hK                          = "CovTypeHeader.csv"
    #headerPathname              = bench+"/CovType" + "/" + hK
    #h2i.import_only(bucket='home-0xdiag-datasets', path=headerPathname)
    #headerKey                   = h2i.find_key(hK)
    #testFile                    = h2i.import_parse(bucket='home-0xdiag-datasets', path=bench+'/CovType/CovTypeTest.csv', schema='local', hex_key="covTtest.hex", header=1, header_from_file=headerKey, separator=44, noPoll=True,doSummary=False)
    #h2o_jobs.pollWaitJobs(timeoutSecs=16000, pollTimeoutSecs=16000, retryDelaySecs=5)
    #elapsedCovTypeTestParse = time.time() - covTypeTestParseStart
    #row = {'testParseWallTime' : elapsedCovTypeTestParse}
    #response = 'C55'
    #ignored  = None
    #doGBM(files['Covtype'], folderPath='CovType', 
    #        ignored_cols    = ignored, 
    #        classification  = 1,
    #        testFilehex     = testFile['destination_key'], 
    #        ntrees          = 100,
    #        depth           = 5,
    #        minrows         = 10, 
    #        nbins           = 100,
    #        learnRate       = 0.01,
    #        response        = response,
    #        row             = row
    #     ) 


    h2o.tear_down_cloud()
