#!/usr/bin/python

#
# This tool goes through every file in the 'man' directory and automatically makes the example \dontrun.
#

import sys
import os
import re
import shutil


STATE_NONE = 1
STATE_IN_EXAMPLES = 2
STATE_IN_CRAN_EXAMPLES = 3
STATE_IN_DONTRUN = 4


class Example:
    def __init__(self, dir_name, file_name, new_dir_name):
        self.dir_name = dir_name
        self.file_name = file_name
        self.new_dir_name = new_dir_name
        self.lineno = 0
        self.state = STATE_NONE
        self.of = None

    def parse_error(self, message):
        print("ERROR " + message + " " + self.file_name + " line " + str(self.lineno))
        sys.exit(1)

    def set_state(self, new_state):
        self.state = new_state
        # print("state set to " + str(self.state))

    def emit_line(self, s):
        self.of.write(s)

    def inject_line(self, s):
        s2 = s
        # s2 = s2 + " # injected"
        s2 = s2 + "\n"
        self.emit_line(s2)

    def process(self):
        # print("Processing " + self.file_name + "...")

        self.set_state(STATE_NONE)

        found_examples = False
        injected_dontrun = False
        found_dontrun = False
        found_dontrun_closebrace = False

        f = open(os.path.join(self.dir_name, self.file_name), "r")
        self.of = open(os.path.join(self.new_dir_name, self.file_name), "w")

        s = f.readline()
        while (len(s) > 0):
            self.lineno = self.lineno + 1

            # print "s is:", s

            match_groups = re.search(r"^\\examples{", s)
            if (match_groups is not None):
                if (self.state == STATE_IN_EXAMPLES):
                    self.parse_error("examples may not be in examples")

                self.set_state(STATE_IN_EXAMPLES)
                found_examples = True

                self.emit_line(s)
                s = f.readline()
                continue

            match_groups = re.search(r"-- CRAN examples begin --", s)
            if (match_groups is not None):
                if (self.state != STATE_IN_EXAMPLES):
                    self.parse_error("CRAN examples must be in examples")
                self.state = STATE_IN_CRAN_EXAMPLES

                self.emit_line(s)
                s = f.readline()
                continue

            match_groups = re.search(r"-- CRAN examples end --", s)
            if (match_groups is not None):
                if (self.state != STATE_IN_CRAN_EXAMPLES):
                    self.parse_error("CRAN examples end must be in CRAN examples")
                self.set_state(STATE_IN_EXAMPLES)

                self.emit_line(s)
                s = f.readline()
                continue

            if (self.state == STATE_IN_CRAN_EXAMPLES):
                self.emit_line(s)
                s = f.readline()
                continue

            match_groups = re.search(r"^\\dontrun{", s)
            if (match_groups is not None):
                if (self.state != STATE_IN_EXAMPLES):
                    self.parse_error("dontrun must be in examples")

                if (found_dontrun):
                    self.parse_error("only one dontrun section is supported")

                if (injected_dontrun):
                    self.inject_line("}")
                    injected_dontrun = False

                self.set_state(STATE_IN_DONTRUN)
                found_dontrun = True

                self.emit_line(s)
                s = f.readline()
                continue

            match_groups = re.search(r"^}", s)
            if (found_examples and (match_groups is not None)):
                if (self.state == STATE_IN_EXAMPLES):
                    if (injected_dontrun):
                        self.inject_line("}")
                        injected_dontrun = False
                    self.set_state(STATE_NONE)
                elif (self.state == STATE_IN_DONTRUN):
                    self.set_state(STATE_IN_EXAMPLES)
                    found_dontrun_closebrace = True
                else:
                    self.parse_error("unaccounted for close brace")
                    sys.exit(1)

                self.emit_line(s)
                s = f.readline()
                continue

            if (found_dontrun_closebrace):
                self.parse_error("extra stuff after dontrun close brace")

            if ((self.state == STATE_IN_EXAMPLES) and not injected_dontrun and not found_dontrun):
                # Skip blank lines, but insert a dontrun block if there is content.
                match_groups = re.match(r"^\s*$", s)
                if (match_groups is None):
                    self.inject_line("\dontrun{")
                    injected_dontrun = True

            self.emit_line(s)
            s = f.readline()
            continue

        f.close()
        self.of.close()

        # if (not found_examples):
        #    self.parse_error("did not find examples")


def main(argv):
    if (not os.path.exists("DESCRIPTION")):
        print("ERROR:  You must run this script inside the generated R package source directory.")
        sys.exit(1)

    os.mkdir("newman")

    for root, dirs, files in os.walk("man"):
        for f in files:
            ex = Example("man", f, "newman")
            ex.process()

    # os.rename("man", "oldman")
    shutil.rmtree("man")
    os.rename("newman", "man")


if __name__ == "__main__":
    main(sys.argv)
