#!/usr/bin/env python

#
# The purpose of the script is to parse the DeepLearning.java file and emit
# code related to parameters.
#
# Currently pieces of R code get emitted and need to be pasted in manually to the R file.
#

import sys
import os
import shutil
import signal
import time
import random
import getpass
import re
import subprocess


class Blob:
    def __init__(self, n, help):
        self.n = n
        self.help = help

def read_deeplearning_file(deeplearning_file):
    """
    Read deep learning file and generate R parameter stub stuff.

    @param deeplearning_file: Java source code file
    @return: none
    """
    try:
        nlist = []
        in_api = False
        help = None

        f = open(deeplearning_file, "r")
        s = f.readline()
        lineno = 0
        while (len(s) != 0):
            lineno = lineno + 1
            stripped = s.strip()
            if (len(stripped) == 0):
                s = f.readline()
                continue
            if (stripped.startswith("@API")):
                # print("")
                if (in_api):
                    assert(False)
                in_api = True

                match_groups = re.search("help\s*=\s*\"([^\"]*)\"", stripped)
                if (match_groups == None):
                    print("Missing help")
                    sys.exit(1)
                help = match_groups.group(1)
                # print(help)
                s = f.readline()
                continue
            if (in_api):
                skip = False
                if "checkpoint" in stripped:
                    skip = True
                if "expert_mode" in stripped:
                    skip = True
                # if "activation" in stripped:
                #     skip = True
                # if "initial_weight_distribution" in stripped:
                #     skip = True
                # if "loss" in stripped:
                #     skip = True
                # if "score_validation_sampling" in stripped:
                #     skip = True

                if (skip):
                    in_api = False
                    s = f.readline()
                    continue

                match_groups = re.search("public boolean (\S+) = (\S+);", s)
                if (match_groups is not None):
                    t = "boolean"
                    n = match_groups.group(1)
                    v = match_groups.group(2)
                    print("  parms = .addBooleanParm(parms, k=\"{}\", v={})".format(n,n))
                    nlist.append(Blob(n, help))
                    # print(t, n, v)
                    in_api = False
                    s = f.readline()
                    continue

                match_groups = re.search("public Activation (\S+) = (\S+);", s)
                if (match_groups is not None):
                    t = "string"
                    n = match_groups.group(1)
                    v = match_groups.group(2)
                    print("  parms = .addStringParm(parms, k=\"{}\", v={})".format(n,n))
                    nlist.append(Blob(n, help))
                    # print(t, n, v)
                    in_api = False
                    s = f.readline()
                    continue

                match_groups = re.search("public int\[\] (\S+) = .*;", s)
                if (match_groups is not None):
                    t = "int array"
                    n = match_groups.group(1)
                    print("  parms = .addIntArrayParm(parms, k=\"{}\", v={})".format(n,n))
                    nlist.append(Blob(n, help))
                    # print(t, n)
                    in_api = False
                    s = f.readline()
                    continue

                match_groups = re.search("public int (\S+) = .*;", s)
                if (match_groups is not None):
                    t = "int"
                    n = match_groups.group(1)
                    print("  parms = .addIntParm(parms, k=\"{}\", v={})".format(n,n))
                    nlist.append(Blob(n, help))
                    # print(t, n)
                    in_api = False
                    s = f.readline()
                    continue

                match_groups = re.search("public double (\S+) = (\S+);", s)
                if (match_groups is not None):
                    t = "double"
                    n = match_groups.group(1)
                    v = match_groups.group(2)
                    print("  parms = .addDoubleParm(parms, k=\"{}\", v={})".format(n,n))
                    nlist.append(Blob(n, help))
                    # print(t, n, v)
                    in_api = False
                    s = f.readline()
                    continue

                match_groups = re.search("public float (\S+) = (\S+);", s)
                if (match_groups is not None):
                    t = "float"
                    n = match_groups.group(1)
                    v = match_groups.group(2)
                    print("  parms = .addFloatParm(parms, k=\"{}\", v={})".format(n,n))
                    nlist.append(Blob(n, help))
                    # print(t, n, v)
                    in_api = False
                    s = f.readline()
                    continue

                match_groups = re.search("public double\[\] (\S+);", s)
                if (match_groups is not None):
                    t = "double array"
                    n = match_groups.group(1)
                    print("  parms = .addDoubleArrayParm(parms, k=\"{}\", v={})".format(n,n))
                    nlist.append(Blob(n, help))
                    # print(t, n)
                    in_api = False
                    s = f.readline()
                    continue

                match_groups = re.search("public long (\S+) = new Random.*;", s)
                if (match_groups is not None):
                    t = "long"
                    n = match_groups.group(1)
                    v = -1
                    print("  parms = .addLongParm(parms, k=\"{}\", v={})".format(n,n))
                    nlist.append(Blob(n, help))
                    # print(t, n, v)
                    in_api = False
                    s = f.readline()
                    continue

                match_groups = re.search("public long (\S+) = (\S+);", s)
                if (match_groups is not None):
                    t = "long"
                    n = match_groups.group(1)
                    v = match_groups.group(2)
                    print("  parms = .addLongParm(parms, k=\"{}\", v={})".format(n,n))
                    nlist.append(Blob(n, help))
                    # print(t, n, v)
                    in_api = False
                    s = f.readline()
                    continue

                if (stripped == "public InitialWeightDistribution initial_weight_distribution = InitialWeightDistribution.UniformAdaptive;"):
                    t = "string"
                    n = "initial_weight_distribution"
                    print("  parms = .addStringParm(parms, k=\"{}\", v={})".format(n,n))
                    nlist.append(Blob(n, help))
                    # print(t, "initial_weight_distribution", "UniformAdaptive")
                    in_api = False
                    s = f.readline()
                    continue

                if (stripped == "public Loss loss = Loss.CrossEntropy;"):
                    t = "string"
                    n = "loss"
                    print("  parms = .addStringParm(parms, k=\"{}\", v={})".format(n,n))
                    nlist.append(Blob(n, help))
                    # print(t, "loss", "CrossEntropy")
                    in_api = False
                    s = f.readline()
                    continue

                if (stripped == "public ClassSamplingMethod score_validation_sampling = ClassSamplingMethod.Uniform;"):
                    t = "string"
                    n = "score_validation_sampling"
                    print("  parms = .addStringParm(parms, k=\"{}\", v={})".format(n,n))
                    nlist.append(Blob(n, help))
                    # print(t, "score_validation_sampling", "Uniform")
                    in_api = False
                    s = f.readline()
                    continue

                print("ERROR: No match group found on line ", lineno)
                sys.exit(1)

            s = f.readline()
        f.close()

        print("")
        print("")

        for blob in nlist:
            print("  {},".format(blob.n))

        print("")
        print("")

        for blob in nlist:
            print("        \item{\code{" + blob.n + "}: " + blob.help + "}")

    except IOError as e:
        print("")
        print("ERROR: Failure reading test list: " + deeplearning_file)
        print("       (errno {0}): {1}".format(e.errno, e.strerror))
        print("")
        sys.exit(1)

def main(argv):
    read_deeplearning_file("./src/main/java/hex/deeplearning/DeepLearning.java")

if __name__ == "__main__":
    main(sys.argv)
