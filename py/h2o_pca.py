import time, math, string
import h2o_cmd
from pprint import pprint
from h2o_test import verboseprint, dump_json, check_sandbox_for_errors

def simpleCheckPCA(self, pca, **kwargs):
    #print dump_json(pca)
    warnings = None
    if 'warnings' in pca:
        warnings = pca['warnings']
        # catch the 'Failed to converge" for now
        x = re.compile("[Ff]ailed")
        for w in warnings:
            print "\nwarning:", w
            if re.search(x,w): raise Exception(w)

    # Check other things in the json response dictionary 'pca' here
    pcaResult = pca
    verboseprint('pcaResult Inspect:', dump_json(pcaResult))
    
    #Check no NaN in sdevs, propVars, or in PCs 
    print "Checking sdevs..."
    sdevs = pcaResult["pca_model"]["sdev"]
    verboseprint("pca sdevs:", dump_json(sdevs))
    
    # sdevs is supposed to be a list sorted by s 
    # sFirst = sdevs[0].s
    for PC,s in enumerate(sdevs):
        if math.isnan(s):
            raise Exception("sdev %s is NaN: %s" % (PC,s))
        # anqi says the list should be sorted..i.e. first first
        ## if s < sFirst:
        ##     raise Exception("sdev %s %s is > sFirst %s. Supposed to be sorted?" % (PC, s, sFirst))
            

    print "Checking propVars...",
    propVars = pcaResult["pca_model"]["propVar"]
    verboseprint("pca propVars:", dump_json(propVars))
    for PC,propvar in enumerate(propVars):
        if math.isnan(propvar):
            raise Exception("propVar %s is NaN: %s", (PC, propvar))
    print " Good!"
    print "Checking eigVec...",
    pcs = pcaResult["pca_model"]["eigVec"]
    verboseprint("pca eigVec:", dump_json(pcs))
    for i,s in enumerate(pcs):
        for r,e in enumerate(s):
            if math.isnan(e):
                raise Exception("Component %s has NaN: %s eigenvector %s", (i, e, s))
    print " Good!"

    print "How many components did we get? (after enum col dropping): %s" % len(pcs)
    
    # now print the top ten. Sorting by the value...getting key,value tuples (so we can see the column)
    # it should match the column numbering..even if it skips cols due to enums
    import operator
    print "Just look at the sort for the first row in pca eigVec"
    i = 0
    s = pcs[i]
    # print "s:", s
    unsorted_s = [(i,j) for i,j in enumerate(s)]
    sorted_s = sorted(unsorted_s, key=lambda t: abs(t[1]), reverse=True)
    print "\n%s First (larger). sorted_s: %s\n" % (i, sorted_s)
    print "The last entry from the eigenvector, should have the largest std dev, because it's sorted"
    print "Rule of thumb is we can then look at the sorted values, and guess it's related to column importance"
    print "The sort should be on the abs(), since the signs can be + or -"

    # shouldn't have any errors
    check_sandbox_for_errors()
    return warnings

def resultsCheckPCA(self, pca, **kwargs):
    pcaResult = pca

    print "Checking that propVars sum to 1",
    propVars = pcaResult["pca_model"]["propVar"]
    sum_ = 1.0
    for PC,propVar in enumerate(propVars): sum_ -= propVar
    self.assertAlmostEqual(sum_,0,msg="PropVar does not sum to 1.")
    print " Good!"
    if pcaResult["pca_model"]["parameters"]["tolerance"] != 0.0 or pcaResult["pca_model"]["parameters"]["standardize"] != True: 
        return
    print "Checking that sdevs^2 sums to number of variables"
    #if not standardize or tolerance != 0, don't do check
    sdevs = pcaResult["pca_model"]["sdev"]
    sumsdevs2 = sum([s**2 for s in sdevs])
    sum_ = len(sdevs)
    for PC,sdev in enumerate(sdevs): sum_ -= sdev**2
    if not ((sum_ -.5) < 0 < (sum_ +.5)):
        print "sum(sdevs^2) are not within .5 of 0. sdevs incorrect? The difference between the number of variables and sum(sdevs^2) is: ", sum_
        print "Perhaps the data was not standardized after all?"
        print "These were the parameters used for pca: ", pcaResult["pca_model"]["parameters"] 
        print "Dumping out the standard deviations: "
        print sdevs
        print "sum(sdevs^2) = ", sumsdevs2
        print "Expected     = ", len(sdevs)
        print "Difference   = ", sum_  
        raise Exception("Standard Deviations are possibly incorrect!")
    print " Good!"

    print "Checking that the sum of square component loadings is 1 for each component." 
    print "In symbols, we are checking: sum_j(a_ij)^2 == 1 for all i"
    pcs = pcaResult["pca_model"]["eigVec"]
    sums = [round(sum([a**2 for a in eigenvector]),5) for eigenvector in pcs]
    print "Sum of the square PC loadings are: ", sums
    if  sums != [1 for i in range(len(pcs))]:
        raise Exception("Sum of the square loadings do not add up to 1 for at least one eigenvector!")
    print "Good!"
