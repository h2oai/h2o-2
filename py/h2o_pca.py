import h2o_cmd, h2o,time,math,string

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
    h2o.verboseprint("pca sdevs:", h2o.dump_json(sdevs))
    
    # sdevs is supposed to be a list sorted by s 
    # sFirst = sdevs[0].s
    for PC,s in sdevs.iteritems():
        if math.isnan(s):
            raise Exception("sdev %s is NaN: %s" % (PC,s))
        # anqi says the list should be sorted..i.e. first first
        ## if s < sFirst:
        ##     raise Exception("sdev %s %s is > sFirst %s. Supposed to be sorted?" % (PC, s, sFirst))
            

    print "Checking propVars...",
    propVars = pcaResult["PCAModel"]["propVar"]
    h2o.verboseprint("pca propVars:", h2o.dump_json(propVars))
    for PC,propvar in propVars.iteritems():
        if math.isnan(propvar):
            raise Exception("propVar %s is NaN: %s", (PC, propvar))
    print " Good!"
    print "Checking eigenvectors...",
    pcs = pcaResult["PCAModel"]["eigenvectors"]
    h2o.verboseprint("pca eigenvectors:", h2o.dump_json(pcs))
    for i,s in enumerate(pcs):
        for r,e in s.iteritems():
            if math.isnan(e):
                raise Exception("Component %s has NaN: %s eigenvector %s", (i, e, s))
    print " Good!"

    print "How many components did we get? (after enum col dropping): %s", len(pcs)
    
    # now print the top ten. Sorting by the value...getting key,value tuples (so we can see the column)
    # it should match the column numbering..even if it skips cols due to enums
    import operator
    print "Just look at the sort for the first row in pca eigenvectors"
    i = 0
    s = pcs[i]
    sorted_s = sorted(s.iteritems(), key=lambda t: abs(t[1]))
    num = min(10, len(s))
    print "\n%s First (smallest) %d. sorted_pcs[0:9]: %s\n" % (i, num, sorted_s[0:num-1])
    print "The first entry from the eigenvector, should have the largest std dev, because it's sorted"
    print "Rule of thumb is we can then look at the sorted values, and guess it's related to column importance"
    print "The sort should be on the abs(), since the signs can be + or -"
    print "\n%s Last %d (largest) sorted_s[-10:]: %s\n" % (i, num, sorted_s[-num:])

    # shouldn't have any errors
    h2o.check_sandbox_for_errors()

    return warnings

def resultsCheckPCA(self, pca, **kwargs):
    #print h2o.dump_json(pca)
    destination_key = pca['destination_key']
    pcaResult = h2o_cmd.runInspect(key=destination_key, **{'view':100})

    print "Checking that propVars sum to 1",
    propVars = pcaResult["PCAModel"]["propVar"]
    sum_ = 1.0
    for PC,propVar in propVars.iteritems(): sum_ -= propVar
    self.assertAlmostEqual(sum_,0,msg="PropVar does not sum to 1.")
    print " Good!"
    if pcaResult["PCAModel"]["PCAParams"]["tolerance"] != 0.0 or pcaResult["PCAModel"]["PCAParams"]["standardized"] != True: 
        return
    print "Checking that sdevs^2 sums to number of variables"
    #if not standardized or tolerance != 0, don't do check
    sdevs = pcaResult["PCAModel"]["stdDev"]
    sum_ = len(sdevs)
    for PC,sdev in sdevs.iteritems(): sum_ -= sdev**2
    if not ((sum_ -.5) < 0 < (sum_ +.5)):
        print "sum(sdevs^2) are not within .5 of 0. sdevs incorrect?"
        h2o.dump_json(sdevs)
        raise Exception("Standard Deviations are possibly incorrect!")
    print " Good!"
    
