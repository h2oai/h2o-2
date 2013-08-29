import h2o, h2o_hosts

def do_R(rScript, rLibrary):
	shCmdString = "R -f " + rScript + " --args " + rLibrary + " " + h2o.nodes[0].http_addr + ":" + str(h2o.nodes[0].port)
	(ps, outpath, errpath) =  h2o.spawn_cmd('rtest_with_h2o', shCmdString.split())
	rc = h2o.spawn_wait(ps, outpath, errpath, timeout=10)
	if(rc != 0): raise Exception("R exited with non-zero return code %s" % rc)