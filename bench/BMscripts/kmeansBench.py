#KMeans bench
import os, sys, time, csv
sys.path.append('../py/')
sys.path.extend(['.','..'])
import h2o_cmd, h2o, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_rf, h2o_jobs

csv_header = ('h2o_build','java_heap_GB','dataset','nRows','nCols','parseWallTime','kmeansBuildTime')

files      = {'Airlines'    : {'train': ('AirlinesTrain1x', 'AirlinesTrain10x', 'AirlinesTrain100x'),          'test' : 'AirlinesTest'},
              'AllBedrooms' : {'train': ('AllBedroomsTrain1x', 'AllBedroomsTrain10x', 'AllBedroomsTrain100x'), 'test' : 'AllBedroomsTest'},
             }   
build = ""

def doKMeans(fs, folderPath): 
    for f in fs['train']:
        #if f in ['AirlinesTrain10x', 'AirlinesTrain100x']: continue
        overallWallStart = time.time()
        date = '-'.join([str(x) for x in list(time.localtime())][0:3])
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
            trainParseWallStart = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', hex_key=hex_key, header=1, header_from_file=headerKey, separator=44,
                timeoutSecs=7200,retryDelaySecs=5,pollTimeoutSecs=7200)
            parseWallTime = time.time() - trainParseWallStart
            #End Train File Parse#
            print "Parsing training file took ", parseWallTime ," seconds." 
            
            inspect  = h2o.nodes[0].inspect(parseResult['destination_key'])
            
            row      =  {'h2o_build'          : build,  
                         'java_heap_GB'       : java_heap_GB,
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
            kmeansStart  = time.time()
            kmeans       = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=7200, **kwargs)
            kmeansTime   = time.time() - kmeansStart
            row.update({'kmeansBuildTime' : kmeansTime})
            csvWrt.writerow(row)
        finally:
            output.close()

if __name__ == '__main__':
    build = sys.argv.pop(-1)
    h2o.parse_our_args()
    h2o_hosts.build_cloud_with_hosts()
    doKMeans(files['Airlines'], 'Airlines')
    doKMeans(files['AllBedrooms'], 'AllBedrooms')
    h2o.tear_down_cloud()
