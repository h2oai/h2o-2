import h2o_cmd, h2o,time,math

def simpleCheckPCA(self, pca, **kwargs):
    #print h2o.dump_json(pca)
    warnings = None
    if 'warnings' in pca:
        warnings = pca['warnings']
        # catch the 'Failed to converge" for now
        x = re.compile("[Ff]ailed")
        for w in warnings:
            print "\nwarning:", w
            if re.search(x,w): raise Exception(w)

    # Check other things in the json response dictionary 'pca' here
    destination_key = pca['destination_key']
    pcaResult = h2o_cmd.runInspect(key=destination_key, view=100)
    h2o.verboseprint('pcaResult Inspect:', h2o.dump_json(pcaResult))
    
    #Check no NaN in sdevs, propVars, or in PCs 
    print "Checking sdevs..."
    sdevs = pcaResult["PCAModel"]["stdDev"]
    for PC,s in sdevs.iteritems():
        if math.isnan(s):
            raise Exception("sdev", PC, "is NaN:", s)

    print "Checking propVars..."
    propVars = pcaResult["PCAModel"]["propVar"]
    for PC,propvar in propVars.iteritems():
        if math.isnan(propvar):
            raise Exception("propVar", PC, "is NaN:", propvar)

    print "Checking eigenvectors..."
    pcs = pcaResult["PCAModel"]["eigenvectors"]
    for i,s in enumerate(pcs):
        for r,e in s.iteritems():
            if math.isnan(e):
                raise Exception("Component", i, "has NaN:", e, "eigenvector",s)

    # shouldn't have any errors
    h2o.check_sandbox_for_errors()

    return warnings

def resultsCheckPCA(self, pca, **kwargs):
    #print h2o.dump_json(pca)
    destination_key = pca['destination_key']
    pcaResult = h2o_cmd.runInspect(key=destination_key, **{'view':100})

    print "Checking that propVars sum to 1"
    propVars = pcaResult["PCAModel"]["propVar"]
    sum_ = 1.0
    for PC,propVar in propVars.iteritems(): sum_ -= propVar
    self.assertAlmostEqual(sum_,0,msg="PropVar does not sum to 1.")

    if pcaResult["PCAModel"]["PCAParams"]["tolerance"] != 0.0 or \
       pcaResult["PCAModel"]["PCAParams"]["standardized"] != "True": return
    print "Checking that sdevs^2 sums to number of variables"
    #if not standardized or tolerance != 0, don't do check
    sdevs = pcaResult["PCAModel"]["stdDev"]
    sum_ = len(sdevs)
    for PC,sdev in sdevs.iteritems(): sum_ -= sdev**2
    if not ((sum_ -.5) < 0 < (sum_ +.5)):
        print "sum(sdevs^2) are not within .5 of 0. sdevs incorrect?"
        h2o.dump_json(sdevs)
