#!/usr/bin/perl

use strict 'vars';
use List::Util 'shuffle';
use feature "switch";

# Examples: 2**10 inputs
# binary in, binary out (parity)
# octal in, octal out (categorical 8 input?, categorical 8 input?) what about just doing by 4?
# hex in, hex out
# do 1 million and shuffle

use constant IBIAS => 0;
use constant OBIAS => 0;
use constant EXTRAFEATURES => 0;

# these should always be power of two multiples of each other?
# if we want balanced trees?
# HMM..what happens if we start with different seeds...should still work (think of the boolean
# logic change)

if (scalar(@ARGV) != 4) {
    die( "Four arguments are required: inumLimit onumLimit maxrows inputBase\n" );
}

# globals
my $inumLimit    = int($ARGV[0]);
my $onumLimit    = int($ARGV[1]);
my $maxrows      = int($ARGV[2]);
# "quad" or "bin"
my $inputBase    = $ARGV[3];

# print info at the top. no fields!
my $features;
checkIO();

# header to created output file name
my $file = "./syn_datasets/parity_" . $inumLimit . "_" . $onumLimit . "_" . $maxrows . "_" . $inputBase . ".data";

print "output file is: $file\n";
open my $fh,'>', $file or die "Can't open the output file: $!";
# 11/15/12 H2O doesn't support #. will mess things up, especially if first line
# we should handle blank though. Just print a blank
# print $fh "# ".localtime()." parity.pl ".$inumLimit." ".$onumLimit." ".$maxrows."\n";
print $fh "";

# go do it!
makeIO($maxrows,$inputBase);

#*************************
sub roundup { 
    my $n = shift; 
    return(($n == int($n)) ? $n : int($n + 1)) 
}

#*************************
# need another one for hstring?
sub stringToCSV {
    my $inumber = $_[0];
    my $bias = $_[1];
    my $inputBase = $_[2];

    if ($features<1 || $features>12) {
        die "ERROR: bad num of features: $features\n";
    }

    my $ibits;
    # this should always be big enough to cover the number space?
    # HMM does this mean the input classes * columns is a constant, compared to the binary
    # that's interesting for comparison, I guess.
    ### print "DEBUG: inputBase: $inputBase\n";

    if ($inputBase eq "quad") { # divide the feature count by 4 to get the width
        $ibits = sprintf("%0".roundup($features/4)."x", $inumber);
    }
    elsif ($inputBase eq "bin") { # binary
        $ibits = sprintf("%0".$features."b", $inumber);
    }
    else {
        die "ERROR: bad inputBase: $inputBase Should be bin or quad\n";
    }

    ### print "DEBUG: ibits: ", $ibits, " len: ", length($ibits), "\n";
    my $istring;
    for (my $i=0;$i<length($ibits);$i++) {
        # bias can be any positive or negative number. integer?
        ### print "DEBUG:", (substr $ibits, $i, 1), "\n";
        my $ibitsPlusBias = $bias + int(substr $ibits, $i, 1);
        if ($i>0) {
            $istring .= ",";
        }
        $istring .= $ibitsPlusBias;
    }

    # UPDATE: add hex 5 and 10 for the constant columns if in hex
    # note "10" is two digits..that's okay!!
    if ($inputBase eq "quad") { 
        $istring = "10," . $istring . ",5";
    }
    else {
        # UPDATE: add leading "1," and trailing ",0" to istring ...i.e. non-changing features
        $istring = "1," . $istring . ",0";
    }
    ### print ("DEBUG: istring: ", $istring, " ibits: ", $ibits, " inumber: ", $inumber, "\n");

    return ($istring);
}

#*************************
sub checkIO {
    if ($inputBase ne "bin" && $inputBase ne "quad") {
        die "ERROR: bad inputBase: $inputBase Should be bin or quad\n";
    }

    # doesn't have to be a power of two
    # just protect against long shuffle
    if ($maxrows<0 || $maxrows>10000000) {
        die "ERROR: bad maxrows: $maxrows\n"
    }


    for ($inumLimit) {
        when (2) {$features = 1;}
        when (4) {$features = 2;}
        when (8) {$features = 3;}
        when (16) {$features = 4;}
        when (32) {$features = 5;}
        when (64) {$features = 6;}
        when (128) {$features = 7;}
        when (256) {$features = 8}
        when (512) {$features = 9;}
        when (1024) {$features = 10;}
        default {
            die "ERROR: bad inumLimit: $inumLimit\n";
        }
        # UPDATE: should we add unnecessary features?
        # should add leading zeroes to input bstring
        $features += EXTRAFEATURES;
    }

    for ($onumLimit) {
        # don't want more than 16 output classes?
        # NOTE: we can shift the output class with a bias later on? (bias it to go negative?)
        when (2) {;}
        when (4) {;}
        when (8) {;}
        when (16) {;}
        default {
            die "ERROR: bad onumLimit: $onumLimit\n"
        }
    }
    # print "args good\n"
}

#*************************
sub makeIO {
    my $maxrows = $_[0];
    my $inputBase = $_[1];
    my @list = ();

    # NOTE: has to start with 0, so within-wrap stays as one char? add bias later?
    my $inum = 0;
    my $onum = 0;

    for (my $i=1;$i<=$maxrows;$i++) {
        # UPDATE: this can be oct or hex on the input side too??
        # get binary classifier working first
        ($inum,$onum) = makeRow($inum, $onum, \@list, $inputBase);
        ### print "DEBUG: row $i\n";
    }

    # now shuffle it up, and throw it out!
    ### print "DEBUG: made $maxrows rows in list\n";
    print $fh shuffle(@list);
}

#*************************
sub makeRow {
    my $inum = $_[0];
    my $onum = $_[1];
    my $listref = $_[2];
    # split the input number into hex or binary
    my $inputBase = $_[3];

    my $istring = stringToCSV($inum,IBIAS,$inputBase);

    # I guess we support an arbitrary # of output classes, so can be 1 or more digits
    my $ostring = sprintf("%d", ($onum + OBIAS));

    # UPDATE: double up the ostring, so we have two correlated features and twice the # of features
    # FIX: we should randomly permute the columns
    # UPDATE: we added one leading 0 and one trailing 1 to the istring in stringToCSV already
    push(@$listref, $istring . "," . $istring . "," . $ostring . "\n");

    $inum++;
    $onum++;

    # wrap at 2**10 unique. That should be a balanced 10 level tree
    if ($inum==$inumLimit) {
        $inum = 0;
    }

    # wrap the output at a different limit
    if ($onum==$onumLimit) {
        $onum = 0;
    }

    return ($inum, $onum);
}
