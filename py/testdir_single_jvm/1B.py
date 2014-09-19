
import numpy as  np
finfo32 = np.finfo(np.float32)
finfo64 = np.finfo(np.float64)
print finfo32
print finfo64
# finfo32.resolution = 1e-6
# finfo64.resolution = 1e-15

# Now that you know how many decimals you want, 
# say, 15, just use a rstrip("0") to get rid of the unnecessary 0s:
import random
with open('reals_1B_15f.data', 'w') as f:
    count = 0
    size = 1000000000
    while count < size:
        value = random.random()
        r = random.randint(0,1)
        if r==0:
            value = -1 * value
        s = ("%.15f\n" % value).rstrip("0")
        f.write(s)
        count +=1
