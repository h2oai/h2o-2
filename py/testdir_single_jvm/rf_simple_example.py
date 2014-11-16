import sys
import json
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_args


#
# This is intended to be the simplest possible RF example.
# Look at sandbox/commands.log for REST API requests to H2O.
#


print "--------------------------------------------------------------------------------"
print "BUILDING CLOUD"
print "--------------------------------------------------------------------------------"

h2o_args.parse_our_args()
h2o.init(node_count=2, java_heap_GB=2)

print "--------------------------------------------------------------------------------"
print "PARSING DATASET"
print "--------------------------------------------------------------------------------"

#
# What this really ends up doing is a REST API PostFile.json request.
#
csvPathname = 'iris/iris2.csv'
parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put')

print "--------------------------------------------------------------------------------"
print "RUNNING RF"
print "--------------------------------------------------------------------------------"

#
# For valid kwargs, look at h2o.py random_forest() params_dict variable.
timeoutSecs = 20
if (h2o.beta_features):
    kwargs = {"ntrees": 6}
else:
    kwargs = {"ntree": 6}

rf_json_response = h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
print json.dumps(rf_json_response, indent=4)

print "--------------------------------------------------------------------------------"
print "SHUTTING DOWN"
print "--------------------------------------------------------------------------------"
h2o.check_sandbox_for_errors()
h2o.tear_down_cloud()
