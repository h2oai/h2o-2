print "Nothing to see here, move along."

# here's the perfect way to do tests for quantile algos
# 
# Pick a target data value T
# Pick a quantile threshod Q (0-1)
# Pick a dataset size M
# 
# then generate (Q*M) data points < your target value with some distribution (complicated is fine..i.e. multiple clusters)
# (these could start very close to T , or not)
# 
# generate M-Q-1 data points > your target value (complicated is fine i.e. multiple clusters)
# (these could end very close to T, or not)
# 
# and then generate your target value (just one)
# 
# assume data is randomly organized. (not sorted, although sorted would also be a good test)
# 
# Run your quantile algo
# 
# the answer should be the exact..for all distributions you use, and for all datasets (billions)
# 
# (this doesn't cover the threshold straddling cases where a mean is needed, but smaller datasets can be created to easily show that behavior)
# 
# correct for both int and real values.
# 
# throw some NAs in there too. shouldn't effect answer, even if 1B NAs.
# 
