import subprocess
import gzip, shutil, random, time, re
import os, zipfile
import h2o

# x = choice_with_probability( [('one',0.25), ('two',0.25), ('three',0.5)] )
# need to sum to 1 or less. check error case if you go negative
def choice_with_probability(tupleList):
    n = random.uniform(0, 1)
    for item, prob in tupleList:
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

def file_line_count(fname):
    return sum(1 for line in open(fname))

# the logfiles are zipped with directory structure
# unzip it to the zipdir, throwing away the directory structure.
# (so we don't have to know the names of the intermediate directories)
def flat_unzip(my_zip, my_dir):
    resultList = []
    with zipfile.ZipFile(my_zip) as zip_file:
        for member in zip_file.namelist():
            filename = os.path.basename(member)
            # skip directories
            if not filename:
                continue
            # copy file (taken from zipfile's extract)
            source = zip_file.open(member)
            target = file(os.path.join(my_dir, filename), "wb")
            with source, target:
                shutil.copyfileobj(source, target)
                resultList.append(target)
    return resultList

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

# gzip infile to gzfile
def file_gzip(infile, gzfile):
    print "\nGzip-ing", infile, "to", gzfile
    start = time.time()
    in_file = open(infile, 'rb')
    zipped_file = gzip.open(gzfile, 'wb')
    zipped_file.writelines(in_file)
    in_file.close()
    zipped_file.close()
    print "\nGzip took",  (time.time() - start), "secs"


# cat file1 and file2 to outfile
def file_cat(file1, file2, outfile):
    print "\nCat'ing", file1, file2, "to", outfile
    start = time.time()
    destination = open(outfile,'wb')
    shutil.copyfileobj(open(file1,'rb'), destination)
    shutil.copyfileobj(open(file2,'rb'), destination)
    destination.close()
    print "\nCat took",  (time.time() - start), "secs"

# used in loop, so doing always print
def file_append(infile, outfile):
    h2o.verboseprint("\nAppend'ing", infile, "to", outfile)
    start = time.time()
    in_file = open(infile,'rb')
    out_file = open(outfile,'a')
    out_file.write(in_file.read())
    out_file.close()
    h2o.verboseprint("\nAppend took",  (time.time() - start), "secs")


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
        ([eE][-+]*[0-9]+)? # optional exponent. A single e matches (incorrectly) apparently repeated +- after the e doesn't matter
        (\s*\[\% ]*)? # can have zero or more percent. Percent can have a space?
        [\s\$\%]*$     # white space or empty space, any number of trailing % or $ too. end
        """, re.VERBOSE)


    # apparently these get detected as number
    # +e+++10
    # +e---10

    # this matches white space? makes all white space count as number?
    number2Regex = re.compile(r"""
        [\s\$\%]*     # begin, white space or empty space. any number of leading % or $ too
        [+-]?    # plus or minus. maybe h2o matches multiple?
        ([0-9]+)? # one or more digits. h2o thinks whole thing optional
        (\.[0-9]*)? # optional decimal point and fractional digits
        ([eE][-+]*[0-9]+)? # optional exponent. a single e matches (incorrectly) apparently repeated +- after the e doesn't matter
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

# from nmb10 at http://djangosnippets.org/snippets/2247/
# Shows difference between two json like python objects. 
# Shows properties, values from first object that are not in the second.
# Examples:
# import simplejson # or other json serializer
# first = simplejson.loads('{"first_name": "Poligraph", "last_name": "Sharikov",}')
# second = simplejson.loads('{"first_name": "Poligraphovich", "pet_name": "Sharik"}')
# df = JsonDiff(first, second)
# df.difference is ["path: last_name"]
# JsonDiff(first, second, vice_versa=True) gives you difference from both objects in the one result.
# df.difference is ["path: last_name", "path: pet_name"]
# JsonDiff(first, second, with_values=True) gives you difference of the values strings. 
class JsonDiff(object):
    def __init__(self, first, second, with_values=False, vice_versa=False):
        self.difference = []
        self.check(first, second, with_values=with_values)
        if vice_versa:
            self.check(second, first, with_values=with_values)
        
    def check(self, first, second, path='', with_values=False):
        if second != None:
            if not isinstance(first, type(second)):
                message = '%s- %s, %s' % (path, type(first), type(second))
                self.save_diff(message, TYPE)

        if isinstance(first, dict):
            for key in first:
                # the first part of path must not have trailing dot.
                if len(path) == 0:
                    new_path = key
                else:
                    new_path = "%s.%s" % (path, key)

                if isinstance(second, dict):
                    if second.has_key(key):
                        sec = second[key]
                    else:
                        #  there are key in the first, that is not presented in the second
                        self.save_diff(new_path, PATH)
                        # prevent further values checking.
                        sec = None

                    # recursive call
                    self.check(first[key], sec, path=new_path, with_values=with_values)
                else:
                    # second is not dict. every key from first goes to the difference
                    self.save_diff(new_path, PATH)                
                    self.check(first[key], second, path=new_path, with_values=with_values)
                
        # if object is list, loop over it and check.
        elif isinstance(first, list):
            for (index, item) in enumerate(first):
                new_path = "%s[%s]" % (path, index)
                # try to get the same index from second
                sec = None
                if second != None:
                    try:
                        sec = second[index]
                    except (IndexError, KeyError):
                        # goes to difference
                        self.save_diff('%s - %s, %s' % (new_path, type(first), type(second)), TYPE)
                # recursive call
                self.check(first[index], sec, path=new_path, with_values=with_values)

        # not list, not dict. check for equality (only if with_values is True) and return.
        else:
            if with_values and second != None:
                if first != second:
                    self.save_diff('%s - %s | %s' % (path, first, second), 'diff')
            return 
            
    def save_diff(self, diff_message, type_):
        message = '%s: %s' % (type_, diff_message)
        if diff_message not in self.difference:
            self.difference.append(message)

