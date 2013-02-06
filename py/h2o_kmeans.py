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


