import sys
import json

sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_import as h2i


#
# This is intended to be the simplest possible RF example.
# Look at sandbox/commands.log for REST API requests to H2O.
#


print "--------------------------------------------------------------------------------"
print "BUILDING CLOUD"
print "--------------------------------------------------------------------------------"

h2o.parse_our_args()
h2o.build_cloud(node_count=2, java_heap_GB=2)

# False == Use VA form of algorithms (when available) (e.g. RF1).
# True == Use FVec form of algorithm (e.g. DRF2).
h2o.beta_features = True


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
# beta_features==False means Value Array (e.g. RF1).
# beta_features==True means Fluid Vec (e.g. DRF2).
#
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
