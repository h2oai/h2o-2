import h2o, h2o_cmd
import h2o_browse as h2b
import time, sys, h2o_hosts

h2o.clean_sandbox()
h2o.parse_our_args()

try:
    while True:
        sys.stdout.write('.')
        sys.stdout.flush()
        #h2o.build_cloud(5)
        h2o_hosts.build_cloud_with_hosts()
        h2o.tear_down_cloud()
        h2o.check_sandbox_for_errors()
except KeyboardInterrupt:
    print 'Interrupted'
finally:
    print 'EAT THE BABIES'
    h2o.tear_down_cloud()
    h2o.check_sandbox_for_errors()
