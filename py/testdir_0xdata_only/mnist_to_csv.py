
# from  http://g.sweyla.com/blog/2012/mnist-numpy/
import os, struct
from array import array as pyarray
import numpy
from numpy import append, array, int8, uint8, zeros

DO_REALS=False
DO_IMAGES=False

# gzip infile to gzfile
def file_gzip(infile, gzfile):
    import gzip
    print "\nGzip-ing", infile, "to", gzfile
    in_file = open(infile, 'rb')
    zipped_file = gzip.open(gzfile, 'wb')
    zipped_file.writelines(in_file)
    in_file.close()
    zipped_file.close()
    print "\nGzip:", gzfile, "done"

def read(digits, dataset="training", path="."):
    """
    Loads MNIST files into 3D numpy arrays

    Adapted from: http://abel.ee.ucla.edu/cvxopt/_downloads/mnist.py
    """

    # assume these files exist and have been gunzipped.
    # download the 4 gz files from http://yann.lecun.com/exdb/mnist/
    if dataset is "training":
        fname_img = os.path.join(path, 'train-images-idx3-ubyte')
        fname_lbl = os.path.join(path, 'train-labels-idx1-ubyte')
    elif dataset is "testing":
        fname_img = os.path.join(path, 't10k-images-idx3-ubyte')
        fname_lbl = os.path.join(path, 't10k-labels-idx1-ubyte')
    else:
        raise ValueError, "dataset must be 'testing' or 'training"

    flbl = open(fname_lbl, 'rb')
    magic_nr, size = struct.unpack(">II", flbl.read(8))
    lbl = pyarray("b", flbl.read())
    flbl.close()

    fimg = open(fname_img, 'rb')
    magic_nr, size, rows, cols = struct.unpack(">IIII", fimg.read(16))
    img = pyarray("B", fimg.read())
    fimg.close()

    ind = [ k for k in xrange(size) if lbl[k] in digits ]
    N = len(ind)

    if DO_REALS:
        images = zeros((N, rows, cols), dtype=float)
        labels = zeros((N, 1), dtype=int8) # always need these to be int for H2O RF output
    else:
        images = zeros((N, rows, cols), dtype=int8)
        labels = zeros((N, 1), dtype=int8)

    for i in xrange(len(ind)):
        images[i] = array(img[ ind[i]*rows*cols : (ind[i]+1)*rows*cols ]).reshape((rows, cols))
        labels[i] = lbl[ind[i]]

    return images, labels


if __name__ == '__main__':
    from pylab import *
    # from numpy import *

    def doit(prefix, f):
        print "we want all the images"
        images, labels = read(range(10), f) 
        if DO_REALS:
            # If you want the values as floats between 0.0 and 1.0, just do
            images /= 255.0
        print images[0]

        print "labels.shape", labels.shape
        print "images.shape", images.shape
        print "images[0].shape", images[0].shape
        (a,b,c) = images.shape
        if DO_REALS:
            # If you want the values as floats between 0.0 and 1.0, just do
            images /= 255.0

        imagesF = images.reshape(a,b*c)
        labelsF = labels

        # stick label and pixels together
        bothF = numpy.concatenate((labelsF, imagesF), 1)
        print "labelsF.shape", labelsF.shape
        print "imagesF.shape", imagesF.shape
        print "bothF.shape", bothF.shape
        
        # the output label was first in the concatenate. do the same for header 
        headerList = ['label']
        headerList += ['p' + str(i) for i in range(784)]
        # comma separated!
        header = ','.join(map(str,headerList))
        print header # just so we can see it.
        if DO_REALS:
            # first has to be integer for stupid h2o rf output (doesn't take fp)
            # have to create a format string for each one as a result!
            fmt = ",".join(["%i"] + ["%f"] * imagesF.shape[1])
        else:
            fmt = '%d'
        numpy.savetxt(prefix + f + '.csv', bothF, header=header, delimiter=',', fmt=fmt)

    # create the two csv files
    if DO_REALS:
        prefix = "mnist_reals_"
    else:
        prefix = "mnist_"

    doit(prefix, 'training')
    doit(prefix, 'testing')

    # we can copy this multiple times to get bigger parsed gz
    file_gzip(prefix + 'training.csv', prefix + 'training.csv.gz')
    file_gzip(prefix + 'testing.csv',  prefix + 'testing.csv.gz')

    # show merged images
    if DO_IMAGES:
        images, labels = read(range(0,10), 'training')
        imshow(images.mean(axis=0), cmap=cm.gray)
        show()

    # If you want the values as floats between 0.0 and 1.0, just do
    # images, labels = read(range(10), "training")
    # images /= 255.0



