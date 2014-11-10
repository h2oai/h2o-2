# this lets me be lazy..starts the cloud up like I want from my json, and gives me a browser
# copies the jars for me, etc. Just hangs at the end for 10 minutes while I play with the browser
import unittest
import time,sys
sys.path.extend(['.','..','py'])

import h2o_cmd, h2o, h2o_glm, h2o_args
import h2o_browse as h2b
import os, csv, time, socket

csv_header = ('time','nodes#','java_heap_GB','dataset','y','x','family','alpha','lambda','n_folds','nLines','nCols','dof','nullDev','resDev','aic','auc','iterations','model_time','model_iterations','val_time','val_iterations','lsm_time', 'wall_clock_secs')

ec2_files = {'allstate':'s3n://h2o-datasets/allstate/train_set.zip','airlines':'s3n://h2o-airlines-unpacked/allyears.csv'}
local_files = {'allstate':'hdfs://172.16.2.176/datasets/allstate.csv','airlines':'hdfs://172.16.2.176/datasets/airlines_all.csv'}

def is_ec2():
    # return False
    return  'AWS_ACCESS_KEY_ID' in os.environ

def run_glms(file,configs):
    output = None
    if not os.path.exists('glmbench.csv'):
        output = open('glmbench.csv','w')
        output.write(','.join(csv_header)+'\n')
    else:
        output = open('glmbench.csv','a')
    csvWrt = csv.DictWriter(output, fieldnames=csv_header, restval=None, dialect='excel', extrasaction='ignore',delimiter=',')
    # header!
    # csvWrt.writerow(dict((fn,fn) for fn in csv_header))
    csvWrt.writeheader()
    try:
        java_heap_GB = h2o.nodes[0].java_heap_GB
        k = parse_file(file)
        # gives us some reporting on missing values, constant values, to see if we have x specified well
        # figures out everything from parseResult['destination_key']
        # needs y to avoid output column (which can be index or name)
        # assume all the configs have the same y..just check with the firs tone
        goodX = h2o_glm.goodXFromColumnInfo(y=configs[0]['y'], key=k, timeoutSecs=300)

        for kwargs in configs:
            start = time.time()
            res = h2o.nodes[0].GLM(k, timeoutSecs=6000000, pollTimeoutSecs=180, **kwargs)
            wall_clock_secs = time.time() - start
            glm = res['GLMModel']

            print "glm model time (milliseconds):", glm['model_time']
            print "glm validations[0] time (milliseconds):", glm['validations'][0]['val_time']
            print "glm lsm time (milliseconds):", glm['lsm_time']
            print 'glm computation time',res['computation_time']

            coefs = glm['coefficients']
            print 'wall clock in', wall_clock_secs, 'secs'
            max_len = 0
            val = glm['validations'][0]
            row = {'time':time.asctime(),'nodes#':len(h2o.nodes)}

            row.update(kwargs)
            row.update(glm)
            row.update(val)
            row.update({'wall_clock_secs': wall_clock_secs})
            row.update({'java_heap_GB': java_heap_GB})
            csvWrt.writerow(row)
        h2o.nodes[0].remove_key(k)
    finally:
        output.close()
    
def parse_file(f):
    v = h2o.nodes[0].import_files()['succeeded'][0]
    return h2o.nodes[0].parse(v['key'],timeoutSecs=3600)['destination_key']

if __name__ == '__main__':
    h2o_args.parse_our_args()
    files = None
    if is_ec2():
        files = ec2_files
        h2o.init()
    else:
        files = local_files
        h2o.init(use_hdfs=True)

    # want to ignore columns with missing values, since GLM throws away those rows, (won't analyze as many rows)
    # Distance, CRSEElapsedTime has some...I guess ignore
    # column Year 0 type: int
    # column Month 1 type: int
    # column DayofMonth 2 type: int
    # column DayOfWeek 3 type: int
    # column DepTime 4 type: int num_missing_values: 2302136
    # column CRSDepTime 5 type: int
    # column ArrTime 6 type: int num_missing_values: 2584478
    # column CRSArrTime 7 type: int
    # column UniqueCarrier 8 type: enum enum_domain_size: 29
    # column FlightNum 9 type: int
    # column TailNum 10 type: int num_missing_values: 123534969
    # column ActualElapsedTime 11 type: int num_missing_values: 2587529
    # column CRSElapsedTime 12 type: int num_missing_values: 26234
    # column AirTime 13 type: int num_missing_values: 123534969
    # column ArrDelay 14 type: int num_missing_values: 2587529
    # column DepDelay 15 type: int num_missing_values: 2302136
    # column Origin 16 type: enum enum_domain_size: 347
    # column Dest 17 type: enum enum_domain_size: 352
    # column Distance 18 type: int num_missing_values: 202000
    # column TaxiIn 19 type: int num_missing_values: 123534969
    # column TaxiOut 20 type: int num_missing_values: 123534969
    # column Cancelled 21 type: int
    # column CancellationCode 22 type: enum enum_domain_size: 5 num_missing_values: 38955823
    # column Diverted 23 type: int
    # column CarrierDelay 24 type: int num_missing_values: 123534969
    # column WeatherDelay 25 type: int num_missing_values: 123534969
    # column NASDelay 26 type: int num_missing_values: 123534969
    # column SecurityDelay 27 type: int num_missing_values: 123534969
    # column LateAircraftDelay 28 type: int num_missing_values: 123534969
    # column IsArrDelayed 29 type: enum enum_domain_size: 2
    # column IsDepDelayed 30 type: enum enum_domain_size: 2

    # was:
    # x = '0,1,2,3,4,5,6,7,8,9,12,16,17,18'
    x = '0,1,2,3,5,7,8,9,16,17'
    # run airlines
    run_glms(files['airlines'],[{'y':'IsArrDelayed','x':x,'lambda':l,'alpha':a,'family':'binomial','n_folds':10,'case':1}
                for l in (0.035,0.025,1e-2,5e-3,1e-3,5e-4,1e-4,5e-5,1e-5,1e-8)
                for a in (1.0,0.5,0.0)])
    # run allstate
    run_glms(files['allstate'],[{'y':'Claim_Amount','lambda':l,'alpha':a,'family':'poisson','n_folds':10} 
                for l in (1e-4,1e-5) 
                for a in (1.0,0.5,0.0)])

    h2o.tear_down_cloud()


