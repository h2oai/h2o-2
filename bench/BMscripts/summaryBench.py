#summary bench
import os, sys, time, csv, string
sys.path.append('../py/')
sys.path.extend(['.','..'])
import h2o_cmd, h2o, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_rf, h2o_util
import h2o_glm, h2o_exec as h2e, h2o_jobs

csv_header = ('h2o_build','nMachines','nJVMs','Xmx/JVM','dataset','nRows','nCols','parseWallTime','summaryBuildTime')

files      = {'Airlines'   : {'train': ('AirlinesTrain1x', 'AirlinesTrain10x', 'AirlinesTrain100x'),          'test' : 'AirlinesTest'},
              'AllBedrooms': {'train': ('AllBedroomsTrain1x', 'AllBedroomsTrain10x', 'AllBedroomsTrain100x'), 'test' : 'AllBedroomsTest'},
             }
build = ""
debug = False
json  = ""
def doSUM(f, folderPath):
    debug = False
    bench = "bench"
    if debug:
        print "Doing SUM DEBUG"
        bench = "bench/debug"
    #date = '-'.join([str(x) for x in list(time.localtime())][0:3])
    retryDelaySecs = 5 #if f == 'AirlinesTrain1x' else 30
    overallWallStart = time.time()
    pre = ""
    if debug: pre    = 'DEBUG'
    sumbenchcsv      = 'benchmarks/'+build+'/'+pre+'summarybench.csv'
    if not os.path.exists(sumbenchcsv):
        output = open(sumbenchcsv,'w')
        output.write(','.join(csv_header)+'\n')
    else:
        output = open(sumbenchcsv,'a')
    csvWrt     = csv.DictWriter(output, fieldnames=csv_header, restval=None, 
                    dialect='excel', extrasaction='ignore',delimiter=',')
    try:
        java_heap_GB     = h2o.nodes[0].java_heap_GB
        importFolderPath = bench + "/" + folderPath
        if (f in ['AirlinesTrain1x','AllBedroomsTrain1x', 'AllBedroomsTrain10x', 'AllBedroomsTrain100x']): 
            csvPathname = importFolderPath + "/" + f + '.csv'
        else: 
            csvPathname = importFolderPath + "/" + f + "/*linked*"
        
        hex_key             = f + '.hex'
        trainParseWallStart = time.time()
        hK                  = folderPath + "Header.csv"
        headerPathname      = importFolderPath + "/" + hK
        
        h2i.import_only(bucket='home-0xdiag-datasets', path=headerPathname)
        headerKey           = h2i.find_key(hK)
        parseResult = h2i.import_parse(bucket           = 'home-0xdiag-datasets', 
                                       path             = csvPathname, 
                                       schema           = 'local', 
                                       hex_key          = hex_key,
                                       header           = 1, 
                                       header_from_file = headerKey, 
                                       separator        = 44,
                                       timeoutSecs      = 7200, 
                                       retryDelaySecs   = retryDelaySecs,
                                       pollTimeoutSecs  = 7200,
                                       doSummary        = False
                                      )
        parseWallTime       = time.time() - trainParseWallStart
        print "Parsing training file took ", parseWallTime ," seconds." 
        inspect             = h2o.nodes[0].inspect(parseResult['destination_key'], timeoutSecs=7200)
        
        nMachines           = 1 if len(h2o_hosts.hosts) is 0 else len(h2o_hosts.hosts)
        row                 =  {'h2o_build'          : build, 
                                'nMachines'          : nMachines,
                                'nJVMs'              : len(h2o.nodes),
                                'Xmx/JVM'            : java_heap_GB,
                                'dataset'            : f,
                                'nRows'              : inspect['num_rows'],
                                'nCols'              : inspect['num_cols'],
                                'parseWallTime'      : parseWallTime,
                               }
    
        sumStart            = time.time()
        sumResult           = h2o_cmd.runSummary(key=hex_key,
                                       timeoutSecs = 7200)
                                     
        sumTime             = time.time() - sumStart
        cmd = 'bash startloggers.sh ' + json + ' stop_'
        os.system(cmd)
        row.update({'summaryBuildTime' : sumTime})
        csvWrt.writerow(row)
    finally:
        output.close()

if __name__ == '__main__':
    dat   = sys.argv.pop(-1)
    debug = sys.argv.pop(-1)
    build = sys.argv.pop(-1)
    json  = sys.argv[-1].split('/')[-1]
    fp    = 'Airlines' if 'Air' in dat else 'AllBedrooms'
    h2o.parse_our_args()
    h2o_hosts.build_cloud_with_hosts()
    if dat == 'Air1x'    : fs = files['Airlines']['train'][0]
    if dat == 'Air10x'   : fs = files['Airlines']['train'][1]
    if dat == 'Air100x'  : fs = files['Airlines']['train'][2]
    if dat == 'AllB1x'   : fs = files['AllBedrooms']['train'][0]
    if dat == 'AllB10x'  : fs = files['AllBedrooms']['train'][1]
    if dat == 'AllB100x' : fs = files['AllBedrooms']['train'][2]
    
    doSUM(fs, fp)
    h2o.tear_down_cloud()
