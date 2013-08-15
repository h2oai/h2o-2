import unittest,time,sys,os,csv,socket
sys.path.extend(['.','..','py'])
import h2o_cmd, h2o, h2o_hosts
import h2o_browse as h2b

csv_header = ('java_heap_GB','dataset','nRows','nCols',
                #'nIgnoredCols','ignoredCols',
              'trainParseWallTime','trainViewTime','testParseWallTime','testViewTime','overallWallTime','errRate')

s3n_files = {'train':'s3n://h2o-datasets/mnist8m/mnist8m-train-1.csv','test':'s3n://h2o-datasets/mnist8m/mnist8m-test-1.csv'}

def run_rf(files,configs):
    overallWallStart = time.time()
    uri = "s3n://h2o-datasets/mnist8m/"
    importHDFSRes = h2o.nodes[0]/import_hdfs(uri)
    s3nList = importHDFSRes['succeeded']
    self.assertGreater(len(s3nFullList),1,"<= 1 file found!")
    output = None
    if not os.path.exists('rfbench.csv'):
        output = open('rfbench.csv','w')
        output.write(','.join(csv_header)+'\n')
    else:
        output = open('rfbench.csv','a')
    csvWrt = csv.DictWriter(output, fieldnames=csv_header, restval=None, dialect='excel', extrasaction='ignore',delimiter=',')
    csvWrt.writeheader()
    try:
        java_heap_GB = h2o.nodes[0].java_heap_GB
        destKey = files['train'] + '.hex'
        #Train File Parsing#
        trainParseWallStart = time.time()
        parseKey = h2o.nodes[0].parse(files['train'], destKey,timeoutSecs=300,retryDelaySecs=5,pollTimeoutSecs=120)
        trainParseWallTime = time.time() - trainParseWallStart
        #End Train File Parse#

        #ignoredCols = h2o_glm.goodXFromColumnInfo(y=configs[0]['y'],key=parseKey['destination_key'],timeoutSecs=300,forRF=True).split(',')
        #nIgnoredCols = len(ignoredCols)
        #ignoredCols = ':'.join(ignoredCols)

        inspect = h2o.nodes[0].inspect(parseKey['destination_key'])
        row = {'java_heap_GB':java_heap_GB,'dataset':'mnist8m',
                'nRows': inspect['num_rows'],'nCols':inspect['num_cols'],
                #'nIgnoredCols':nIgnoredCols,'ignoredCols':ignoredCols,
                'trainParseWallTime':trainParseWallTime}
        #RF+RFView (train)#
        kwargs = configs.copy()
        trainRFStart = time.time()
        rfView = h2o_cmd.runRFOnly(parseKey=parseKey,rfView=True,timeoutSecs= 3600,pollTimeoutSecs= 60,retryDelaySecs = 2, **kwargs)
        trainViewTime = time.time() - trainRFStart
        #End RF+RFView (train)#
        row.update({'trainViewTime':trainViewTime})
        
        h2o_rf.simpleCheckRFView(None, rfView, **kwargs)
        modelKey = rfView['model_key']
        
        #Test File Parsing#
        testParseWallStart = time.time()
        destKey = files['test'] + '.hex'
        parseKey = h2o.nodes[0].parse(files['test'], destKey,timeoutSecs=300,retryDelaySecs=5,pollTimeoutSecs=120)
        testParseWallTime = time.time() - testParseWallStart
        #End Test File Parse#
        row.update({'testParseWallTime':testParseWallTime})
        modelKey = rfView['model_key']
        
        #RFView (score on test)#
        kwargs = configs.copy()
        testRFStart = time.time()
        rfView = h2o_cmd.runRFView(data_key=destKey,model_key=modelKey,ntree=10,timeoutSecs=180,doSimpleCheck=False,**kwargs)
        testViewTime = time.time() - testRFStart
        #End RFView (score on test)#
        errRate = rfView['classification_error']
        row.update({'testViewTime':testViewTime})
        overallWallTime = time.time() - overallWallStart 
        row.update({'overallWallTime':overallWallTime})
        row.uodate({'errRate':errRate})
        #h2o.nodes[0].remove_key(k)
    finally:
        output.close()

if __name__ == '__main__':
    #h2o.parse_our_args()
    h2o_hosts.build_cloud_with_hosts()
    #mnist8m parameters
    params = { 
                'response_variable': 0,
                'ntree': 10,
                'iterative_cm': 0,
                'out_of_bag_error_estimate': 0,
                'features': 37,
                'exclusive_split_limit': None,
                'depth': 2147483647,
                'stat_type': 'ENTROPY',
                'sampling_strategy': 'RANDOM',
                'sample': 100, 
                'model_key': 'RF_model',
                'bin_limit': 1024,
                'seed': 784834182943470027,
                'parallel': 1,
                'use_non_local_data': 0,
                'class_weights': '0=1.0,1=1.0,2=1.0,3=1.0,4=1.0,5=1.0,6=1.0,7=1.0,8=1.0,9=1.0',
                }
    run_rf(s3n_files,params)
    h2o.tear_down_cloud()
