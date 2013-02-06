import h2o
import webbrowser, re, getpass, urllib
# just some things useful for debugging or testing. pops the brower and let's you look at things
# like the confusion matrix by matching the RFView json (h2o keeps the json history for us)


# always nice to have a browser up on the cloud while running at test. You can see the fork/join task state
# and browse to network stats, or look at the time line. Starting from the cloud page is sufficient.
def browseTheCloud():
    # disable browser stuff for jenkins
    if not h2o.browse_disable:
        # after cloud building, node[0] should have the right info for us
        port = h2o.nodes[0].port + 2
        cloud_url = "http://" + h2o.nodes[0].http_addr + ":" + str(port) + "/Cloud.html"

        # Open URL in new window, raising the window if possible.
        h2o.verboseprint("browseTheCloud:", cloud_url)
        webbrowser.open_new(cloud_url)

# match the first, swap the 2nd
def browseJsonHistoryAsUrlLastMatch(matchme,swapme=None):
    if not h2o.browse_disable:
        # get rid of the ".json" from the last url used by the test framework.
        # if we hit len(), we point to 0, so stop
        len_history= len(h2o.json_url_history)
        i = -1
        while (len_history+i!=0 and not re.search(matchme,h2o.json_url_history[i]) ):
            i = i - 1
        url = h2o.json_url_history[i]

        # chop out the .json to get a browser-able url (can look at json too)
        # Open URL in new window, raising the window if possible.
        # webbrowser.open_new_tab(json_url)
        # UPDATE: with the new API port, the browser stuff has .html
        # but we've not switched everything to new. So do it selectively

        if swapme is not None: url = re.sub(matchme, swapme, url)
        url = re.sub("GLMGridProgress","GLMGridProgress.html",url)
        url = re.sub("ParseProgress","ParseProgress.html",url)
        url = re.sub(".json",".html",url)

        h2o.verboseprint("browseJsonHistoryAsUrlLastMatch:", url)
        h2o.verboseprint("same, decoded:", urllib.unquote(url))
        webbrowser.open_new_tab(url)

# maybe not useful, but something to play with.
# go from end, backwards and see what breaks! (in json to html hack url transform)
# note that put/upload  and rf/rfview methods are different for html vs json
def browseJsonHistoryAsUrl():
    if not h2o.browse_disable:
        ignoring = "Cloud"
        i = -1
        # stop if you get to -50, don't want more than 50 tabs on browser
        tabCount = 0
        while (tabCount<50 and len_history+i!=0):
            i = i - 1
            # ignore the Cloud "alive" views
            # FIX! we probably want to expand ignoring to more than Cloud?
            if not re.search(ignoring,h2o.json_url_history[i]):
                url = h2o.json_url_history[i]
                url = re.sub("GLMGridProgress","GLMGridProgress.html",url)
                url = re.sub("ParseProgress","ParseProgress.html",url)
                url = re.sub(".json",".html",url)
                print "browseJsonHistoryAsUrl:", url
                print "same, decoded:", urllib.unquote(url)
                webbrowser.open(url)
                tabCount += 1
