import subprocess
import gzip, shutil, random, time, re
import random, re

# x = choice_with_probability( [('one',0.25), ('two',0.25), ('three',0.5)] )
# need to sum to 1 or less. check error case if you go negative
def choice_with_probability(lst):
    n = random.uniform(0, 1)
    for item, prob in lst:
        if n < prob: break
        n = n - prob
        if n < 0: 
            raise Exception("h2o_util.choice_with_probability() error, prob's sum > 1")
    return item

# since we hang if hosts has bad IP addresses, thought it'd be nice
# to have simple obvious feedback to user if he's running with -v 
# and machines are down or his hosts definition has bad IPs.
# FIX! currently not used
def ping_host_if_verbose(host):
    # if (h2o.verbose) 
    ping = subprocess.Popen( ["ping", "-c", "4", host]) 
    ping.communicate()

# gunzip gzfile to outfile
def file_gunzip(gzfile, outfile):
    print "\nGunzip-ing", gzfile, "to", outfile
    start = time.time()
    zipped_file = gzip.open(gzfile, 'rb')
    out_file = open(outfile, 'wb')
    out_file.writelines(zipped_file)
    out_file.close()
    zipped_file.close()
    print "\nGunzip took",  (time.time() - start), "secs"

# cat file1 and file2 to outfile
def file_cat(file1, file2, outfile):
    print "\nCat'ing", file1, file2, "to", outfile
    start = time.time()
    destination = open(outfile,'wb')
    shutil.copyfileobj(open(file1,'rb'), destination)
    shutil.copyfileobj(open(file2,'rb'), destination)
    destination.close()
    print "\nCat took",  (time.time() - start), "secs"


def file_shuffle(infile, outfile):
    print "\nShuffle'ing", infile, "to", outfile
    start = time.time()
#    lines = open(infile).readlines()
#    random.shuffle(lines)
#    open(outfile, 'w').writelines(lines)
    fi = open(infile, 'r')
    fo = open(outfile, 'w')
    subprocess.call(["sort", "-R"],stdin=fi, stdout=fo)
    print "\nShuffle took",  (time.time() - start), "secs"


# FIX! This is a hack to deal with parser bug
def file_strip_trailing_spaces(csvPathname1, csvPathname2):
        infile = open(csvPathname1, 'r')
        outfile = open(csvPathname2,'w') # existing file gets erased
        for line in infile.readlines():
            # remove various lineends and whitespace (leading and trailing)
            # make it unix linend
            outfile.write(line.strip(" \n\r") + "\n")
        infile.close()
        outfile.close()
        print "\n" + csvPathname1 + " stripped to " + csvPathname2

# can R deal with comments in a csv?
def file_strip_comments(csvPathname1, csvPathname2):
        infile = open(csvPathname1, 'r')
        outfile = open(csvPathname2,'w') # existing file gets erased
        for line in infile.readlines():
            if not line.startswith('#'): outfile.write(line)
        infile.close()
        outfile.close()
        print "\n" + csvPathname1 + " w/o comments to " + csvPathname2

def file_spaces_to_comma(csvPathname1, csvPathname2):
        infile = open(csvPathname1, 'r')
        outfile = open(csvPathname2,'w') # existing file gets erased
        for line in infile.readlines():
            outfile.write(re.sub(r' +',r',',line))
        infile.close()
        outfile.close()
        print "\n" + csvPathname1 + " with space(s)->comma to " + csvPathname2

# UPDATE: R seems to be doing some kind of expand_cat on cols with '.' in them for NA
# (the umass/princeton) data sets. Change to 0 for now so both H2O and R use them the 
# same way
def file_clean_for_R(csvPathname1, csvPathname2):
        infile = open(csvPathname1, 'r')
        outfile = open(csvPathname2,'w') # existing file gets erased
        for line in infile.readlines():
            # 1) remove comments and header???
            if not line.startswith('#') and not re.match('[A-Za-z]+',line):
                # 2) remove various lineends and whitespace (leading and trailing)..make it unix linend
                line = line.strip(" \n\r") + "\n"
                # 3) change spaces to comma (don't worry about spaces in enums..don't have them for now)
                line = re.sub(r' +',r',',line)
                # 4) change "." fields to 0
                line = re.sub(',\.,',',0,',line) # middle of line
                line = re.sub('^\.,','0,',line)  # beginning of line
                line = re.sub(',\.$',',0',line)  # end of line
                outfile.write(line)
        infile.close()
        outfile.close()
        print "\n" + csvPathname1 + " cleaned for R to " + csvPathname2


# this might be slightly pessimistic, but should be superset
def might_h2o_think_whitespace(token):
    # we allow $ prefix and % suffix as decorators to numbers?
    whitespaceRegex = re.compile(r"""
        \s*$     # begin, white space or empty space, end
        """, re.VERBOSE)
    if whitespaceRegex.match(token):
        return True
    else:
        return False


# this might be slightly pessimistic, but should be superset
def might_h2o_think_number_or_whitespace(token):
    # this matches white space? makes all white space count as number?
    specialRegex = re.compile(r"""
        \s*
        [\$+-]? # single chars that might be considered numbers. alow spaces in between
        \s*$ 
        """, re.VERBOSE)

    # this matches white space? makes all white space count as number?
    number1Regex = re.compile(r"""
        [\s\$\%]*     # begin, white space or empty space. any number of leading % or $ too
        [+-]?    # plus or minus. maybe h2o matches multiple?
        ([0-9]*\.[0-9]*)?  # decimal point focused. optional whole and fractional digits. h2o thinks whole thing optional?
        ([eE][-+]?[0-9]*)? # optional exponent. A single e matches (incorrectly)
        (\s*\[\% ]*)? # can have zero or more percent. Percent can have a space?
        [\s\$\%]*$     # white space or empty space, any number of trailing % or $ too. end
        """, re.VERBOSE)

    # this matches white space? makes all white space count as number?
    number2Regex = re.compile(r"""
        [\s\$\%]*     # begin, white space or empty space. any number of leading % or $ too
        [+-]?    # plus or minus. maybe h2o matches multiple?
        ([0-9]+)? # one or more digits. h2o thinks whole thing optional
        (\.[0-9]*)? # optional decimal point and fractional digits
        ([eE][-+]?[0-9]*)? # optional exponent. a single e matches (incorrectly)
        (\s*\[\% ]*)? # can have zero or more percent. Percent can have a space?
        [\s\$\%]*$     # white space or empty space, any number of trailing % or $ too. end
        """, re.VERBOSE)

    # can nans have the +-%$ decorators?. allow any case?
    nanRegex = re.compile(r"""
        [\s\$\%]*     # begin, white space or empty space. any number of leading % or $ too
        [+-]?    # plus or minus
        [Nn][Aa][Nn]? # nan or na
        (\s*\[\% ]*)? # can have zero or more percent. Percent can have a space?
        [\s\$\%]*$     # white space or empty space, any number of trailing % or $ too. end
        """, re.VERBOSE)

    if specialRegex.match(token) or number1Regex.match(token) or number2Regex.match(token) or nanRegex.match(token):
        return True
    else:
        return False

