#KMeans bench
import os, sys, time, csv
sys.path.append('../py/')
sys.path.extend(['.','..'])
import h2o_cmd, h2o, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_rf, h2o_jobs

csv_header = ('h2o_build','nMachines','nJVMs','Xmx/JVM','dataset','nRows','nCols','parseWallTime','kmeansBuildTime')

files      = {'Airlines'    : {'train': ('AirlinesTrain1x', 'AirlinesTrain10x', 'AirlinesTrain100x'),          'test' : 'AirlinesTest'},
              'AllBedrooms' : {'train': ('AllBedroomsTrain1x', 'AllBedroomsTrain10x', 'AllBedroomsTrain100x'), 'test' : 'AllBedroomsTest'},
             }   
build = ""

def doKMeans(fs, folderPath): 
    benchmarkLogging = ['cpu','disk', 'network', 'iostats']
    date = '-'.join([str(x) for x in list(time.localtime())][0:3])
    for f in fs['train']:
        #h2o.cloudPerfH2O.switch_logfile(location='./BMLogs/'+build+ '/' + date, log='KMeans'+f+'.csv')
        overallWallStart = time.time()
        kmeansbenchcsv = 'benchmarks/'+build+'/'+date+'/kmeansbench.csv'
        if not os.path.exists(kmeansbenchcsv):
            output = open(kmeansbenchcsv,'w')
            output.write(','.join(csv_header)+'\n')
        else:
            output = open(kmeansbenchcsv,'a')
        csvWrt = csv.DictWriter(output, fieldnames=csv_header, restval=None, 
                        dialect='excel', extrasaction='ignore',delimiter=',')
        try:
            java_heap_GB = h2o.nodes[0].java_heap_GB
            #Train File Parsing#
            importFolderPath = "bench/" + folderPath
            if (f in ['AirlinesTrain1x','AllBedroomsTrain1x', 'AllBedroomsTrain10x', 'AllBedroomsTrain100x']): csvPathname = importFolderPath + "/" + f + '.csv'
            else: csvPathname = importFolderPath + "/" + f + "/*linked*"
            hex_key = f + '.hex'
            hK = folderPath + "Header.csv"
            headerPathname = importFolderPath + "/" + hK
            h2i.import_only(bucket='home-0xdiag-datasets', path=headerPathname)
            headerKey = h2i.find_key(hK)
            h#2o.cloudPerfH2O.message("=========PARSE TRAIN========")
            trainParseWallStart = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', hex_key=hex_key, header=1, header_from_file=headerKey, separator=44,
                timeoutSecs=7200,retryDelaySecs=5,pollTimeoutSecs=7200, benchmarkLogging=benchmarkLogging)
            parseWallTime = time.time() - trainParseWallStart
            #h2o.cloudPerfH2O.message("=========END PARSE TRAIN========") 
            #End Train File Parse#
            print "Parsing training file took ", parseWallTime ," seconds." 
            
            inspect  = h2o.nodes[0].inspect(parseResult['destination_key'])
            
            nMachines = 1 if len(h2o_hosts.hosts) is 0 else len(h2o_hosts.hosts) 
            row      =  {'h2o_build'          : build,
                         'nMachines'          : nMachines,
                         'nJVMs'              : len(h2o.nodes),
                         'Xmx/JVM'            : java_heap_GB,
                         'dataset'            : f,
                         'nRows'              : inspect['num_rows'],
                         'nCols'              : inspect['num_cols'],
                         'parseWallTime'      : parseWallTime,
                        }
        
            params   =  {'source_key'         : hex_key,
                         'k'                  : 6,
                         'initialization'     : 'Furthest',
                         'max_iter'           : 100,
                         'seed'               : 1234567,
                         'normalize'          : 0,
                         #'cols'               : ,
                         'destination_key'    : "KMeans("+f+")",
                        }
            kwargs       = params.copy()
            #h2o.cloudPerfH2O.message("=========KMEANS========")
            kmeansStart  = time.time()
            kmeans       = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=7200, benchmarkLogging=benchmarkLogging,**kwargs)
            kmeansTime   = time.time() - kmeansStart
            #h2o.cloudPerfH2O.message("=========END KMEANS========")
            row.update({'kmeansBuildTime' : kmeansTime})
            csvWrt.writerow(row)
        finally:
            output.close()

if __name__ == '__main__':
    build = sys.argv.pop(-1)
    h2o.parse_our_args()
    h2o_hosts.build_cloud_with_hosts(enable_benchmark_log=False)
    doKMeans(files['Airlines'], 'Airlines')
    doKMeans(files['AllBedrooms'], 'AllBedrooms')
    h2o.tear_down_cloud()
