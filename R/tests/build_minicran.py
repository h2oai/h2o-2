#!/usr/bin/python

import os
import sys


class MinicranBuilder:
    def __init__(self, print_only, output_dir, platform, rversion, branch, buildnum):
        self.print_only = print_only
        self.output_dir = output_dir
        self.platform = platform
        self.rversion = rversion
        self.branch = branch
        self.buildnum = buildnum

    def create_output_dir(self):
        if (os.path.exists(self.output_dir)):
            print("")
            print("ERROR: Output directory already exists: " + self.output_dir)
            print("")
            sys.exit(1)

        try:
            os.makedirs(self.output_dir)
        except OSError as e:
            print("")
            print("mkdir failed (errno {0}): {1}".format(e.errno, e.strerror))
            print("    " + self.output_dir)
            print("")
            sys.exit(1)

    def create_cran_layout(self):
        pass

    def download_h2o(self):
        pass

    def extract_h2o_dependencies(self):
        pass

    def download_h2o_dependencies(self):
        pass

    def build(self):
        self.create_output_dir()
        self.create_cran_layout()
        self.download_h2o()
        self.extract_h2o_dependencies()
        self.download_h2o_dependencies()


#--------------------------------------------------------------------
# Main program
#--------------------------------------------------------------------

g_default_rversion = "3.0"
g_default_branch = "master"
g_default_buildnum = "latest"

# Global variables that can be set by the user.
g_script_name = ""
g_platform = None
g_output_dir = None
g_rversion = g_default_rversion
g_branch = g_default_branch
g_buildnum = g_default_buildnum
g_print_only = False


def usage():
    print("")
    print("Build a minimal self-contained CRAN-line repo containing the H2O package")
    print("and its dependencies.  The intent is that the output of this tool can be")
    print("put on a USB stick and installed on a computer with no network connection.")
    print("")
    print("Usage:  " + g_script_name +
          " --platform <platform_name>"
          " --outputdir <dir>"
          " [--rversion <version_number>]"
          " [--branch <branch_name>]"
          " [--build <build_number>]"
          " [-n]")
    print("")
    print("    --platform    OS that R runs on.  (e.g. linux, windows, macosx)")
    print("")
    print("    --outputdir   Directory to be created.  It must not already exist.")
    print("")
    print("    --rversion    Version of R.  (e.g. 2.14, 2.15, 3.0)")
    print("                  (Default is: "+g_default_rversion+")")
    print("")
    print("    --branch      H2O git branch.  (e.g. master, rel-jacobi)")
    print("                  (Default is: "+g_default_branch+")")
    print("")
    print("    --buildnum    H2O build number.  (e.g. 1175, latest)")
    print("                  (Default is: "+g_default_buildnum+")")
    print("")
    print("    -n            Print what would be done, but don't actually do it.")
    print("")
    print("Examples:")
    print("")
    print("    Just accept the defaults and go:")
    print("        "+g_script_name)
    print("")
    print("    Build for R version 3.0.x on Windows; use the h2o jacobi release latest:")
    print("        "+g_script_name+" --platform windows --rversion 3.0 --branch rel-jacobi --build latest "
          "--outputdir h2o_minicran_windows_3.0_rel-jacobi")
    print("")
    sys.exit(1)


def unknown_arg(s):
    print("")
    print("ERROR: Unknown argument: " + s)
    print("")
    usage()


def bad_arg(s):
    print("")
    print("ERROR: Illegal use of (otherwise valid) argument: " + s)
    print("")
    usage()


def unspecified_arg(s):
    print("")
    print("ERROR: Argument must be specified: " + s)
    print("")
    usage()


def parse_args(argv):
    global g_platform
    global g_output_dir
    global g_rversion
    global g_branch
    global g_buildnum
    global g_print_only

    i = 1
    while (i < len(argv)):
        s = argv[i]

        if (s == "--platform"):
            i += 1
            if (i > len(argv)):
                usage()
            g_platform = argv[i]
            if (g_platform not in ["linux", "windows", "macosx"]):
                bad_arg(s)
        elif (s == "--rversion"):
            i += 1
            if (i > len(argv)):
                usage()
            g_rversion = argv[i]
            if (g_rversion not in ["2.14", "2.15", "3.0"]):
                bad_arg(s)
        elif (s == "--outputdir"):
            i += 1
            if (i > len(argv)):
                usage()
            g_output_dir = argv[i]
        elif (s == "--branch"):
            i += 1
            if (i > len(argv)):
                usage()
            g_branch = argv[i]
        elif (s == "--buildnum"):
            i += 1
            if (i > len(argv)):
                usage()
            g_buildnum = argv[i]
        elif (s == "-n"):
            g_print_only = True
        elif (s == "-h" or s == "--h" or s == "-help" or s == "--help"):
            usage()
        else:
            unknown_arg(s)

        i += 1

    if (g_platform is None):
        unspecified_arg("platform")

    if (g_output_dir is None):
        unspecified_arg("output directory")


def main(argv):
    """
    Main program.

    @return: none
    """
    global g_script_name

    g_script_name = os.path.basename(argv[0])

    # Override any defaults with the user's choices.
    parse_args(argv)

    b = MinicranBuilder(g_print_only, g_output_dir, g_platform, g_rversion, g_branch, g_buildnum)
    b.build()


if __name__ == "__main__":
    main(sys.argv)
