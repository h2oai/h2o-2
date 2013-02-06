#!/usr/bin/perl

# invoke with 0 for full
# invoke with 1-4 for split into first cards 4 suits (can parallelize)

use strict 'vars';

# 10 predictive attributes, 1 goal attribute
# Within a hand, no order of rank or suit
# no missing attributes

# 1) S1 Suit of card #1 Ordinal (1-4) representing {Hearts, Spades, Diamonds, Clubs}
# 2) C1 Rank of card #1 Numerical (1-13) representing (Ace, 2, 3, ... , Queen, King)
# 3) S2 Suit of card #2 Ordinal (1-4) representing {Hearts, Spades, Diamonds, Clubs}
# 4) C2 Rank of card #2 Numerical (1-13) representing (Ace, 2, 3, ... , Queen, King)
# 5) S3 Suit of card #3 Ordinal (1-4) representing {Hearts, Spades, Diamonds, Clubs}
# 6) C3 Rank of card #3 Numerical (1-13) representing (Ace, 2, 3, ... , Queen, King)
# 7) S4 Suit of card #4 Ordinal (1-4) representing {Hearts, Spades, Diamonds, Clubs}
# 8) C4 Rank of card #4 Numerical (1-13) representing (Ace, 2, 3, ... , Queen, King)
# 9) S5 Suit of card #5 Ordinal (1-4) representing {Hearts, Spades, Diamonds, Clubs}
# 10) C5 Rank of card #5 Numerical (1-13) representing (Ace, 2, 3, ... , Queen, King) 

# 11) CLASS Poker Hand Ordinal (0-9)
#
# 0: Nothing in hand; not a recognized poker hand 
# 1: One pair; one pair of equal ranks within five cards
# 2: Two pairs; two pairs of equal ranks within five cards
# 3: Three of a kind; three equal ranks within five cards
# 4: Straight; five cards, sequentially ranked with no gaps
# 5: Flush; five cards with the same suit
# 6: Full house; pair + different rank three of a kind
# 7: Four of a kind; four equal ranks within five cards
# 8: Straight flush; straight + flush
# 9: Royal flush; {Ace, King, Queen, Jack, Ten} + flush

# Statistics the full output should have
# Poker Hand       # of hands   Probability # of combinations
# Royal Flush      4        0.00000154  480
# Straight Flush   36       0.00001385  4320
# Four of a kind   624      0.0002401   74880
# Full house       3744     0.00144058  449280
# Flush            5108     0.0019654   612960
# Straight         10200    0.00392464  1224000
# Three of a kind  54912    0.02112845  6589440
# Two pairs        123552   0.04753902  14826240
# One pair         1098240  0.42256903  131788800
# Nothing          1302540  0.50117739  156304800

# Total            2598960  1.0         311875200
# The number of combinations represents the number of instances in the entire domain.

# Note all possibilities are in order. This might not be good for a random forest bagging
# Or it might be! can randomize afterwards?


#*******************
sub makeSuitRank {
    my ($c, $cs, $cr);
            
    $c = @_[0];
    if (($c<0) || ($c>51)) {
        print "# ERROR $c\n";
    }

    # this makes us stay on the same suits for a long time?
    $cs = (int ($c / 13)) + 1; # 1-4
    $cr = ($c % 13) + 1;      # 1-13

    return($cs, $cr);
}

#*******************
# for histogramming

# globals
my $totalHands;
my @scoreArr;

my $score;
sub scoreHandAndCount;


#*******************

# Go thru all 52 cards in each slot. skip if matches card already used. use modulo to break out rank/suit
# rank is 1 to 13, suit is 1 to 4

# globals
my ($c1, $c1s, $c1r);
my ($c2, $c2s, $c2r);
my ($c3, $c3s, $c3r);
my ($c4, $c4s, $c4r);
my ($c5, $c5s, $c5r);

my $DECKM1 = 51; # should be 51

my $startC1;
my $endC1;

# take an optional argument so we can break it into 4
# 1-4
if (($ARGV[0]>=1) && ($ARGV[0]<=4)) {
    $startC1 = (($ARGV[0] - 1) * 13) ;
    $endC1 = ($startC1 + 13) - 1;
}
# full on 0 or error
else {
    $startC1 = 0;
    $endC1 = $DECKM1;
}
print "# Doing $startC1 thru $endC1\n";


for ($c1=$startC1;$c1<=$endC1;$c1++) {
    for ($c2=0;$c2<=$DECKM1;$c2++) {
        next if ($c2==$c1); 
        for ($c3=0;$c3<=$DECKM1;$c3++) {
            next if ( ($c3==$c2) || ($c3==$c1) );
            for ($c4=0;$c4<=$DECKM1;$c4++) {
                next if ( ($c4==$c3) || ($c4==$c2) || ($c4==$c1) );
                for ($c5=0;$c5<=$DECKM1;$c5++) {
                    next if ( ($c5==$c4) || ($c5==$c4) || ($c5==$c3) || ($c5==$c2) || ($c5==$c1));

                    ($c1s, $c1r) = &makeSuitRank($c1);
                    ($c2s, $c2r) = &makeSuitRank($c2);
                    ($c3s, $c3r) = &makeSuitRank($c3);
                    ($c4s, $c4r) = &makeSuitRank($c4);
                    ($c5s, $c5r) = &makeSuitRank($c5);

                    $score = &scoreHandAndCount();
                    print "$c1s,$c1r,$c2s,$c2r,$c3s,$c3r,$c4s,$c4r,$c5s,$c5r,$score\n";
                }
            }
        }
    }
}

print "#\n";
print "# totalHands: $totalHands\n";
my $i;

my @desc = (
    "none", "one pair", "two pair", "three of a kind", 
    "straight", "flush", "full house", "four of a kind",
    "straight flush", "royal flush"
);

for ($i=0;$i<=9;$i++) {
    print "# $i: $scoreArr[$i] ";
    print $desc[$i];
    print "\n";
}

# we can't reorder the columns since we are generating all combinations.
# so we can only reorder the records.
# here's a one-liner "unsorter"..one pass plus a sort
#
# perl -wne 'printf "%016.0f%s", rand 2**53, $_'  | sort | cut -b17-
# awk 'BEGIN{srand()}{print rand(),$1}' /path/to/input | sort -n | awk '{print $2}' > output

#*************************************************************************
sub scoreHandAndCount {
    use sort "_quicksort";

    # check highest first, early out on any hit. 
    # sort the 5 cards by rank for easy compare?
    my @nsr = ($c1r, $c2r, $c3r, $c4r, $c5r);
    my @sr = sort {$a <=> $b} @nsr; # numerical sort of the ranks.

    # independently sorted..so rank and suit sorted can't be used together
    # sort the 5 cards by suit for easy compare?
    my @nss = ($c1s, $c2s, $c3s, $c4s, $c5s);
    my @ss = sort {$a <=> $b} @nss; # numerical sort of the ranks.
    # print "# non sorted suit: $nss[0],$nss[1],$nss[2],$nss[3],$nss[4]\n";
    # print "# sorted suit: $ss[0],$ss[1],$ss[2],$ss[3],$ss[4]\n";

    my $ACE = 1;
    my $TEN = 10;
    my $JACK = 11;
    my $QUEEN = 12;
    my $KING = 13;

    # should check the most likely first, for easy out (speed?)..too hard?
    # go in reverse order
    # 9: Royal flush; {Ace, King, Queen, Jack, Ten} + flush
    # all suits match

    # aces are normally high, but can be low for straight or straight flush
    # we only care about ace order for straights and straight flush
    # an ace height straight is the same as a royal flush, without the flush.
    # so check that special as "aceHiStraight"
    # normal straight ordering (low ace) won't catch that.

    # okay to precompute all this, because the early outs are least likely?!

    # all 5 suits the same
    my $flush = 
        ($ss[0]==$ss[1]) && 
        ($ss[0]==$ss[2]) && 
        ($ss[0]==$ss[3]) && 
        ($ss[0]==$ss[4]);

    my $aceHiStraight = 
        ($sr[0]==$ACE) && # remember that $ACE==1
        ($sr[1]==$TEN) && 
        ($sr[2]==$JACK) && 
        ($sr[3]==$QUEEN) && 
        ($sr[4]==$KING);

    # naturally also covers ace-low straight.
    # straight = in order ranking
    my $straight1 = 
        ($sr[1]==($sr[0]+1)) && 
        ($sr[2]==($sr[0]+2)) && 
        ($sr[3]==($sr[0]+3)) && 
        ($sr[4]==($sr[0]+4));

    my $straight = $aceHiStraight || $straight1;

    #*******************
    # overlaps 5 of kind, but that's ok. 
    # we continue with that okayness for all overlapping but lesser scores
    # Note that the sorted order limits the cases to check

    # four equal ranks within the five cards
    my $fourOfKind0 = 
        ($sr[0]==$sr[1]) && 
        ($sr[0]==$sr[2]) && 
        ($sr[0]==$sr[3]);

    my $fourOfKind1 = 
        ($sr[1]==$sr[2]) && 
        ($sr[1]==$sr[3]) && 
        ($sr[1]==$sr[4]);

    my $fourOfKind = $fourOfKind0 || $fourOfKind1;

    #**************************
    # make sure three of a kind don't overlap each other
    # three equal ranks within five cards
    my $triplet012 = ($sr[0]==$sr[1]) && ($sr[0]==$sr[2]);
    my $triplet123 = ($sr[1]==$sr[2]) && ($sr[1]==$sr[3]);
    my $triplet234 = ($sr[2]==$sr[3]) && ($sr[2]==$sr[4]);

    my $triplet = $triplet012 || $triplet123 || $triplet234;

    #**************************
    # two equal ranks within the five cards 
    my $pair01 = $sr[0]==$sr[1];
    my $pair02 = $sr[0]==$sr[2];
    my $pair03 = $sr[0]==$sr[3];
    my $pair04 = $sr[0]==$sr[4];

    my $pair12 = $sr[1]==$sr[2];
    my $pair13 = $sr[1]==$sr[3];
    my $pair14 = $sr[1]==$sr[4];

    my $pair23 = $sr[2]==$sr[3];
    my $pair24 = $sr[2]==$sr[4];

    my $pair34 = $sr[3]==$sr[4];

    my $pair = 
        $pair01 || $pair02 || $pair03 || $pair04 ||
        $pair12 || $pair13 || $pair14 || 
        $pair23 || $pair24 || 
        $pair34;

    #**************************
    # make sure two pair don't overlap!
    my $twoPair = 
        ($pair01 & $pair23) ||
        ($pair01 & $pair34) ||
        ($pair12 & $pair34);

    #**************************
    # 3 of kind plus 2 of kind
    my $fullHouse0 = $pair01 && $triplet234;
    my $fullHouse1 = $pair34 && $triplet012;
    my $fullHouse = $fullHouse0 || $fullHouse1;

    # royal flush
    $totalHands++;
    if ($flush && $aceHiStraight) {
        $score = 9;
        $scoreArr[9]++; # histogram
    }
    # straight flush
    elsif ($flush && $straight) {
        $score = 8;
        $scoreArr[8]++;
    }
    elsif ($fourOfKind) {
        $score = 7;
        $scoreArr[7]++;
    }
    elsif ($fullHouse) {
        $score = 6;
        $scoreArr[6]++;
    }
    elsif ($flush) {
        $score = 5;
        $scoreArr[5]++;
    }
    elsif ($straight) { # includes ace high straight
        $score = 4;
        $scoreArr[4]++;
    }
    elsif ($triplet) {
        $score = 3;
        $scoreArr[3]++;
    }
    elsif ($twoPair) {
        $score = 2;
        $scoreArr[2]++;
    }
    elsif ($pair) {
        $score = 1;
        $scoreArr[1]++;
    }
    else {
        $score = 0;
        $scoreArr[0]++;
    }

    return ($score);

}

