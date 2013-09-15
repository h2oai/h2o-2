import os, json
import h2o

global configs
configs = []
def setup_test_config(test_config_json='test_config.json'):
    '''
        test_config.json is expected to be a list. 
        We'll return a list of test config objects
        That list is created in h2o.testConfig[], and can be referenced like h2o.testConfig[0]['trees']
    '''
    print "\nCreating state for testConfig from", test_config_json
    if not os.path.exists(test_config_json):
        print test_config_json, "doesn't exist, no test config imported to h2o_test.*"
        return None

    with open(test_config_json, 'rb') as f:
        testConfigJson = json.load(f)

    testConfigList = []
    for testConfig in testConfigJson:
        testConfigObj = TestConfig(testConfig)
        testConfigList.append(testConfigObj)

    # it's already there as a global in h2o.py. assign it!
    print testConfigList
    configs[:] = testConfigList
    print len(testConfigList), "total testConfigs ingested from", test_config_json, "into h2o_test.configs"
    return testConfigList


class TestConfig(object):
    '''
        this will hold a test's configuration state
        assumption is you load a json file with any kind of configuration state 
        you want. 
        The exact state can vary from test to test.
    '''
    def __init__(self, testConfig):
        if testConfig is None:
            print "\nEmpty testConfig", testConfig
            return

        if not isinstance(testConfig, dict) and not isinstance(testConfig, list):
            print "\ntestConfig:", testConfig
            raise Exception("Individual testConfig entries should be a list or dict, even if one item. Maybe you need {}?")
            
        print testConfig
        for k,v in testConfig.iteritems():
            print "testConfig init:", k, v
            # hack because it looks like the json is currently created with "None" for values of None
            # rather than worrying about that, just translate "None" to None here. "None" shouldn't exist
            # for any other reason.
            # Also: humans may not know the right thing for json (is it null? same issue with true/false?)
            if v == "None":
                v = None
            setattr(self, k, v) # achieves self.k = v
        print "Created", len(testConfig), "things for a h2o testConfig"

