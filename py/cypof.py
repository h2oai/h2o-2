import h2o
import h2o_cmd
import h2o_hosts
import sys
import time
import webbrowser
#import os, json, unittest, time, shutil, sys
#sys.path.extend(['.','..','py'])

h2o.config_json = "testdir_hosts/pytest_config-cypof.json"
h2o_hosts.build_cloud_with_hosts()

#h2o.build_cloud(4, java_heap_GB=1, capture_output=False, classpath=True)

file = csvPathname = h2o.find_file('smalldata/covtype/covtype.20k.data')
h2o_cmd.runKMeans(csvPathname=file, key='covtype', k=7)
webbrowser.open("http://localhost:54323/Progress.html?destination_key=covtype.kmeans")

time.sleep(1)
h2o.tear_down_cloud()
