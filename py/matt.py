import h2o, h2o_cmd
import h2o_browse as h2b
import time, sys

h2o.clean_sandbox()
h2o.parse_our_args()

try:
    print 'Building'
    h2o.build_cloud(3, capture_output=False)
    n = h2o.nodes[0]
    print 'Import'
    n.import_s3('h2o_datasets')
    print 'Parsing'
    n.parse('s3:h2o_datasets/covtype.data')
    print 'Import'
    n.import_s3('h2o_datasets')
except KeyboardInterrupt:
    print 'Interrupted'
finally:
    print 'EAT THE BABIES'
    h2o.tear_down_cloud()
