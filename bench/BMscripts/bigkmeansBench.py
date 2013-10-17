#KMeans bench
import os, sys, time, csv
sys.path.append('../py/')
sys.path.extend(['.','..'])
import h2o_cmd, h2o, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_rf, h2o_jobs

csv_header = ('h2o_build','nMachines','nJVMs','Xmx/JVM','dataset','nRows','nCols','parseWallTime','kmeansBuildTime')

build = ""

def doKMeans(): 
    f = ""
    benchmarkLogging = ['cpu','disk', 'network', 'iostats']
    date = '-'.join([str(x) for x in list(time.localtime())][0:3])
    h2o.cloudPerfH2O.switch_logfile(location='./BMLogs/'+build+ '/' + date, log='BIGKMeans'+f+'.csv')
    overallWallStart = time.time()
    kmeansbenchcsv = 'benchmarks/'+build+'/'+date+'/bigkmeansbench.csv'
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
        importFolderPath = "kmeans_big"
        csvPathname = 'syn_sphere15_2711545732row_6col_180GB_from_7x.csv'
        hex_key = csvPathname + '.hex'
        h2o.cloudPerfH2O.message("=========PARSE TRAIN========")
        trainParseWallStart = time.time()
        parseResult = h2i.import_parse(bucket='home3-0xdiag-datasets', path=csvPathname, schema='local', hex_key=hex_key, separator=44,
            timeoutSecs=14400,retryDelaySecs=15,pollTimeoutSecs=14400, benchmarkLogging=benchmarkLogging)
        parseWallTime = time.time() - trainParseWallStart
        h2o.cloudPerfH2O.message("=========END PARSE TRAIN========") 
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
                     'k'                  : 15,
                     'initialization'     : 'Furthest',
                     'max_iter'           : 100,
                     'seed'               : 265211114317615310,
                     'normalize'          : 0,
                     #'cols'               : ,
                     'destination_key'    : "KMeans("+f+")",
                    }
        kwargs       = params.copy()
        h2o.cloudPerfH2O.message("=========KMEANS========")
        kmeansStart  = time.time()
        kmeans       = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=14400, benchmarkLogging=benchmarkLogging,**kwargs)
        kmeansTime   = time.time() - kmeansStart
        h2o.cloudPerfH2O.message("=========END KMEANS========")
        row.update({'kmeansBuildTime' : kmeansTime})
        csvWrt.writerow(row)
    finally:
        output.close()

if __name__ == '__main__':
    build = sys.argv.pop(-1)
    h2o.parse_our_args()
    h2o.build_cloud(1, java_heap_GB=240, enable_benchmark_log=True)
    h2o.tear_down_cloud()
