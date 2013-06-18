# this lets me be lazy..starts the cloud up like I want from my json, and gives me a browser
# copies the jars for me, etc. Just hangs at the end for 10 minutes while I play with the browser
import unittest
import time,sys
sys.path.extend(['.','..','py'])

import h2o_cmd, h2o, h2o_hosts
import h2o_browse as h2b
import os
import csv
import time
import socket



csv_header = ('time','nodes#','dataset','y','x','family','alpha','lambda','n_folds','nLines','nCols','dof','nullDev','resDev','aic','auc','iterations','model_time','model_iterations','val_time','val_iterations','lsm_time')


ec2_files = {'allstate':'s3n://h2o-datasets/allstate/train_set.zip','airlines':'s3n://h2o-airlines-unpacked/allyears.csv'}
local_files = {'allstate':'hdfs://192.168.1.176/datasets/allstate.csv','airlines':'hdfs://192.168.1.176/datasets/airlines_all.csv'}

def is_ec2():
    return  'AWS_ACCESS_KEY_ID' in os.environ

def run_glms(file,configs):
    output = None
    if not os.path.exists('glmbench.csv'):
        output = open('glmbench.csv','w')
        output.write(','.join(csv_header)+'\n')
    else:
        output = open('glmbench.csv','a')
    csvWrt = csv.DictWriter(output, fieldnames=csv_header, restval=None, dialect='excel', extrasaction='ignore',delimiter=',')
    try:
        k = parse_file(file)
        try:
            for kwargs in configs:
                res = h2o.nodes[0].GLM(k, timeoutSecs=6000000, **kwargs)
                glm = res['GLMModel']
                coefs = glm['coefficients']
                coefs = glm['coefficients']
                print 'model computed in',res['computation_time']
                max_len = 0
                val = glm['validations'][0]
                row = {'time':time.asctime(),'nodes#':len(h2o.nodes)}
                row.update(kwargs)
                row.update(glm)
                row.update(val)
                csvWrt.writerow(row)
            h2o.nodes[0].remove_key(k)
    finally:
        output.close()
    
def parse_file(f):
    v = h2o.nodes[0].import_hdfs(f)['succeeded'][0]
    return h2o.nodes[0].parse(v['key'],timeoutSecs=3600)['destination_key']

if __name__ == '__main__':
    h2o.parse_our_args()
    files = None
    if is_ec2():
        files = ec2_files
        h2o_hosts.build_cloud_with_hosts()
    else:
        files = local_files
        h2o_hosts.build_cloud_with_hosts(use_hdfs=True,base_port=54321)
    try:
        # run alstate
        run_glms(files['allstate'],[{'y':'Claim_Amount','lambda':l,'alpha':a,'family':'poisson','n_folds':10} for l in (1e-4,1e-5) for a in (1.0,0.5,0.0)])
        # run airlines
        run_glms(files['airlines'],[{'y':'IsArrDelayed','x':'0,1,2,3,4,5,6,7,8,9,12,16,17,18','lambda':l,'alpha':a,'family':'binomial','n_folds':10,'case':1}
                                          for l in (0.035,0.025,1e-2,5e-3,1e-3,5e-4,1e-4,5e-5,1e-5,1e-8)
                                          for a in (1.0,0.5,0.0)])
        h2o.tear_down_cloud()



