import h2o, h2o_cmd
import h2o_browse as h2b
import time
import psutil
import webbrowser
#import os, json, unittest, time, shutil, sys
#sys.path.extend(['.','..','py'])

try:
    for proc in psutil.process_iter():
      if proc.name == 'java.exe':
        proc.kill()
        
    print 'Building cloud'
    h2o.clean_sandbox()
    #h2o.parse_our_args()
    h2o.build_cloud(4, java_heap_GB=1, capture_output=False, classpath=True)
    # h2o.nodes = [h2o.ExternalH2O()]
    print 'KMeans'
    file = csvPathname = h2o.find_file('smalldata/covtype/covtype.20k.data')
    h2o_cmd.runKMeans(csvPathname=file, key='covtype', k=7)
    print 'Web'
    webbrowser.open("http://localhost:54323/KMeansProgress.html?destination_key=covtype.kmeans")
except KeyboardInterrupt:
    print 'Interrupted'
finally:
    print 'EAT THE BABIES'
    # h2o.tear_down_cloud()
