import h2o
import random

def pickRandRfParams(paramDict, params):
    colX = 0
    randomGroupSize = random.randint(1,len(paramDict))
    for i in range(randomGroupSize):
        randomKey = random.choice(paramDict.keys())
        randomV = paramDict[randomKey]
        randomValue = random.choice(randomV)
        params[randomKey] = randomValue
        if (randomKey=='x'):
            colX = randomValue
        # temp hack to avoid CM=0 results if 100% sample and using OOBEE
        if 'sample' in params and params['sample']==100:
            params['out_of_bag_error_estimate'] = 0
    return colX

def simpleCheckRFView(node, rfv,**kwargs):
    if not node:
        node = h2o.nodes[0]

    if 'warnings' in rfv:
        warnings = rfv['warnings']
        # catch the 'Failed to converge" for now
        for w in warnings:
            print "\nwarning:", w
            if ('Failed' in w) or ('failed' in w):
                raise Exception(w)

    oclass = rfv['response_variable']
    if (oclass<0 or oclass>20000):
        raise Exception("class in RFView seems wrong. class:", oclass)

    # the simple assigns will at least check the key exists
    cm = rfv['confusion_matrix']
    header = cm['header'] # list

    scoresList = cm['scores'] # list
    totalScores = 0
    # individual scores can be all 0 if nothing for that output class
    # due to sampling
    for s in scoresList:
        totalScores += sum(s)
    if (totalScores<=0 or totalScores>5e9):
        raise Exception("scores in RFView seems wrong. scores:", scoresList)

    type = cm['type']
    used_trees = cm['used_trees']
    if (used_trees<=0 or used_trees>20000):
        raise Exception("used_trees in RFView seems wrong. used_trees:", used_trees)

    data_key = rfv['data_key']
    model_key = rfv['model_key']
    ntree = rfv['ntree']
    if (ntree<=0 or ntree>20000):
        raise Exception("ntree in RFView seems wrong. ntree:", ntree)
    response = rfv['response'] # Dict
    rfv_h2o = response['h2o']
    rfv_node = response['node']
    status = response['status']
    time = response['time']

    trees = rfv['trees'] # Dict
    depth = trees['depth']
    # zero depth okay?
    ## if ' 0.0 ' in depth:
    ##     raise Exception("depth in RFView seems wrong. depth:", depth)
    leaves = trees['leaves']
    if ' 0.0 ' in leaves:
        raise Exception("leaves in RFView seems wrong. leaves:", leaves)
    number_built = trees['number_built']
    if (number_built<=0 or number_built>20000):
        raise Exception("number_built in RFView seems wrong. number_built:", number_built)


    h2o.verboseprint("RFView response: number_built:", number_built, "leaves:", leaves, "depth:", depth)

    # just touching these keys to make sure they're good?
    confusion_key = rfv['confusion_key']

    # 2/14/13 kbn. can we not model any more? causes assertion error
    ### confusionInspect = node.inspect(confusion_key)
    ### modelInspect = node.inspect(model_key)
    dataInspect = node.inspect(data_key)



