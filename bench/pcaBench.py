#PCA bench
import os, sys, time, csv
sys.path.append('../py/')
sys.path.extend(['.','..'])
import h2o_cmd, h2o, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_rf, h2o_util
import h2o_glm, h2o_exec as h2e, h2o_jobs
csv_header = ('java_heap_GB','dataset','nRows','nCols','parseWallTime','pcaBuildTime')

files      = {'Airlines'    : {'train': ('AirlinesTrain1x', 'AirlinesTrain10x', 'AirlinesTrain100x'),          'test' : 'AirlinesTest'},
              'AllBedrooms' : {'train': ('AllBedroomsTrain1x', 'AllBedroomsTrain10x', 'AllBedroomsTrain100x'), 'test' : 'AllBedroomsTest'},
             }

def doPCA(fs, folderPath): 
    for f in fs['train']:
        print "Doing PCA on ", f
        overallWallStart = time.time()
        if not os.path.exists('pcabench.csv'):
            output = open('pcabench.csv','w')
            output.write(','.join(csv_header)+'\n')
        else:
            output = open('pcabench.csv','a')
        csvWrt = csv.DictWriter(output, fieldnames=csv_header, restval=None, 
                        dialect='excel', extrasaction='ignore',delimiter=',')
        try:
            java_heap_GB = h2o.nodes[0].java_heap_GB
            importFolderPath = "bench-test/" + folderPath
            if (f in ['AirlinesTrain1x','AllBedroomsTrain1x', 'AllBedroomsTrain10x', 'AllBedroomsTrain100x']): csvPathname = importFolderPath + "/" + f + '.csv'
            else: csvPathname = importFolderPath + "/" + f + "/*linked*"
            hex_key = f + '.hex'
            trainParseWallStart = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', hex_key=hex_key,
                timeoutSecs=3600,retryDelaySecs=5,pollTimeoutSecs=3600)
            parseWallTime = time.time() - trainParseWallStart
            print "Parsing training file took ", parseWallTime ," seconds." 
            
            inspect  = h2o.nodes[0].inspect(parseResult['destination_key'])
            
            row      =  {'java_heap_GB'       : java_heap_GB,
                         'dataset'            : f,
                         'nRows'              : inspect['num_rows'],
                         'nCols'              : inspect['num_cols'],
                         'parseWallTime'      : parseWallTime,
                        }
        
            params   =  {'destination_key'    : "python_PCA_key",
                         'ignore'             : 0,
                         'tolerance'          : 0.0,
                         'standardize'        : 1,
                        }

            kwargs    = params.copy()
            pcaStart  = time.time()
            pcaResult = h2o_cmd.runPCA(parseResult=parseResult, timeoutSecs=3600, **kwargs)
            pcaTime   = time.time() - pcaStart
            row.update({'pcaBuildTime' : pcaTime})
            csvWrt.writerow(row)
        finally:
            output.close()

if __name__ == '__main__':
    h2o_hosts.build_cloud_with_hosts()
    doPCA(files['Airlines'], 'Airlines')
    doPCA(files['AllBedrooms'], 'AllBedrooms')
    h2o.tear_down_cloud()
