import h2o_cmd, h2o
import re, math

def simpleCheckKMeans(self, kmeans, **kwargs):
    ### print h2o.dump_json(kmeans)
    warnings = None
    if 'warnings' in kmeans:
        warnings = kmeans['warnings']
        # catch the 'Failed to converge" for now
        x = re.compile("[Ff]ailed")
        for w in warnings:
            print "\nwarning:", w
            if re.search(x,w): raise Exception(w)

    # Check other things in the json response dictionary 'kmeans' here
    destination_key = kmeans["destination_key"]
    kmeansResult = h2o_cmd.runInspect(key=destination_key)
    clusters = kmeansResult["KMeansModel"]["clusters"]
    for i,c in enumerate(clusters):
        for n in c:
            if math.isnan(n):
                raise Exception("center", i, "has NaN:", n, "center:", c)

    # shouldn't have any errors
    h2o.check_sandbox_for_errors()

    return warnings


def bigCheckResults(self, kmeans, csvPathname, parseKey, applyDestinationKey, **kwargs):
    simpleCheckKMeans(self, kmeans, **kwargs)
    model_key = kmeans['destination_key']
    kmeansResult = h2o_cmd.runInspect(key=model_key)
    centers = kmeansResult['KMeansModel']['clusters']

    kmeansApplyResult = h2o.nodes[0].kmeans_apply(
        data_key=parseKey['destination_key'], model_key=model_key,
        destination_key=applyDestinationKey)
    inspect = h2o_cmd.runInspect(None, applyDestinationKey)
    h2o_cmd.infoFromInspect(inspect, csvPathname)

    kmeansScoreResult = h2o.nodes[0].kmeans_score(
        key=parseKey['destination_key'], model_key=model_key)
    score = kmeansScoreResult['score']
    rows_per_cluster = score['rows_per_cluster']
    sqr_error_per_cluster = score['sqr_error_per_cluster']

    for i,c in enumerate(centers):
        print "\ncenters["+str(i)+"]: ", centers[i]
        print "rows_per_cluster["+str(i)+"]: ", rows_per_cluster[i]
        print "sqr_error_per_cluster["+str(i)+"]: ", sqr_error_per_cluster[i]

    return centers


# compare this clusters to last one. since the files are concatenations, 
# the results should be similar? 10% of first is allowed delta
def compareToFirstKMeans(self, clusters, firstclusters):
    # clusters could be a list or not. if a list, don't want to create list of that list
    # so use extend on an empty list. covers all cases?
    if type(clusters) is list:
        kList  = clusters
        firstkList = firstclusters
    elif type(clusters) is dict:
        raise Exception("compareToFirstGLm: Not expecting dict for " + key)
    else:
        kList  = [clusters]
        firstkList = [firstclusters]

    for k, firstk in zip(kList, firstkList):
        # delta must be a positive number?
        delta = .1 * abs(float(firstk))
        msg = "Too large a delta (>" + str(delta) + ") comparing current and first clusters: " + \
            str(float(k)) + ", " + str(float(firstk))
        self.assertAlmostEqual(float(k), float(firstk), delta=delta, msg=msg)
        self.assertGreaterEqual(abs(float(k)), 0.0, str(k) + " abs not >= 0.0 in current")

