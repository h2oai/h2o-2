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
