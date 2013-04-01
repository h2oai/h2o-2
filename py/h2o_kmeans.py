import h2o_cmd, h2o
import re

def simpleCheckKMeans(self, kmeans, **kwargs):
    print h2o.dump_json(kmeans)
    warnings = None
    if 'warnings' in kmeans:
        warnings = kmeans['warnings']
        # catch the 'Failed to converge" for now
        x = re.compile("[Ff]ailed")
        for w in warnings:
            print "\nwarning:", w
            if re.search(x,w): raise Exception(w)

    print "KMeans time", kmeans['response']['time']

    # Check other things in the json response dictionary 'kmeans' here

    return warnings


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

