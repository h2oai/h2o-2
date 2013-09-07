import h2o, h2o_test

l = h2o_test.setup_test_config(test_config_json='test_config.json')
print "\nsetup_test_config returns list of test config objs:", l

# Here are some ways to reference the config state that the json created

print "\nHow to reference.."
for i, obj in enumerate(h2o_test.configs):
    print "keys in config", i, ":", obj.__dict__.keys()

print h2o_test.configs[0].trees

for t in h2o_test.configs:
    print "\nTest config_name:", t.config_name
    print "trees:", t.trees
    print "params:", t.params
    print "params['timeoutSecs']:", t.params['timeoutSecs']



