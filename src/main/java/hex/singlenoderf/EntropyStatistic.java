package hex.singlenoderf;
import hex.singlenoderf.*;
import hex.singlenoderf.Data;
import water.util.Utils;

import java.util.Random;


/**The entropy formula is the classic Shannon entropy gain, which is:
 *
 * - \sum(p_i * log2(_pi))
 *
 * where p_i is the probability of i-th class occurring. The entropy is
 * calculated for the left and right node after the given split and they are
 * combined together weighted on their probability.
 *
 * ent left * weight left + ent right * weight right
 * --------------------------------------------------
 *                  total weight
 *
 * And to get the gain, this is subtracted from potential maximum of 1
 * simulating the previous node. The biggest gain is selected as the tree split.
 *
 * The same is calculated also for exclusion, where left stands for the rows
 * where column equals to the split point and right stands for all others.
 */
class EntropyStatistic extends Statistic {

  public EntropyStatistic(Data data, int features, long seed, int exclusiveSplitLimit) { super(data, features, seed, exclusiveSplitLimit, false /*classification*/); }
  /** LessThenEqual splits s*/
  @Override protected Split ltSplit(int col, Data d, int[] dist, int distWeight, Random rand) {
    final int[] distL = new int[d.classes()], distR = dist.clone();
    final double upperBoundReduction = upperBoundReduction(d.classes());
    double maxReduction = -1;
    int bestSplit = -1;
    int totL = 0, totR = 0;     // Totals in the distribution
    int classL = 0, classR = 0; // Count of non-zero classes in the left/right distributions
    for (int e: distR) { // All zeros for the left, but need to compute for the right
      totR += e;
      if( e != 0 ) classR++;
    }
    // For this one column, look at all his split points and find the one with the best gain.
    for (int i = 0; i < _columnDists[col].length - 1; ++i) {
      int [] cdis = _columnDists[col][i];
      for (int j = 0; j < distL.length; ++j) {
        int v = cdis[j];
        if( v == 0 ) continue;              // No rows with this class
        totL     += v;  totR     -= v;
        if( distL[j]== 0 ) classL++; // One-time transit from zero to non-zero for class j
        distL[j] += v;  distR[j] -= v;
        if( distR[j]== 0 ) classR--; // One-time transit from non-zero to zero for class j
      }
      if (totL == 0) continue;  // Totals are zero ==> this will not actually split anything
      if (totR == 0) continue;  // Totals are zero ==> this will not actually split anything

      // Compute gain.
      // If the distribution has only 1 class, the gain will be zero.
      double eL = 0, eR = 0;
      if( classL > 1 ) for (int e: distL) eL += gain(e,totL);
      if( classR > 1 ) for (int e: distR) eR += gain(e,totR);
      double eReduction = upperBoundReduction - ( (eL * totL + eR * totR) / (totL + totR) );

      if (eReduction == maxReduction) {
        // For now, don't break ties.  Most ties are because we have several
        // splits with NO GAIN.  This happens *billions* of times in a standard
        // covtype RF, because we have >100K leaves per tree (and 50 trees and
        // 54 columns per leave and however many bins per column), and most
        // leaves have no gain at most split points.
        //if (rand.nextInt(10)<2) bestSplit=i;
      } else if (eReduction > maxReduction) {
        bestSplit = i;  maxReduction = eReduction;
      }
    }
    return bestSplit == -1
            ? Split.impossible(Utils.maxIndex(dist,_random))
            : Split.split(col,bestSplit,maxReduction);
  }
  /**Gain function*/
  private double gain(int e, int tot) {
    if (e == 0) return 0;
    double v = e/(double)tot;
    double r = v * Math.log(v) / log2;
    return -r;
  }
  /**Maximal entropy*/
  private double upperBoundReduction(double classes) {
    double p = 1/classes;
    double r = p * Math.log(p)/log2 * classes;
    return -r;
  }
  /**Compute an exclusive split (i.e. 'feature' '==' 'val') */
  @Override protected Split eqSplit(int col, Data d, int[] dist, int distWeight, Random rand) {
    final int[] distR = new int[d.classes()], distL = dist.clone();
    final double upperBoundReduction = upperBoundReduction(d.classes());
    double maxReduction = -1;
    int bestSplit = -1;
    int min = d.colMinIdx(col);
    int max = d.colMaxIdx(col);
    for (int i = min; i < max+1; ++i) {
      for (int j = 0; j < distR.length; ++j) {
        int v = _columnDists[col][i][j];
        distL[j] += distR[j];
        distR[j] = v;
        distL[j] -= v;
      }
      int totL = 0, totR = 0;
      for (int e: distL) totL += e;
      if (totL == 0)   continue;
      for (int e: distR) totR += e;
      if (totR == 0) continue;
      double eL = 0, eR = 0;
      for (int e: distL) eL += gain(e,totL);
      for (int e: distR) eR += gain(e,totR);
      double eReduction = upperBoundReduction - ( (eL * totL + eR * totR) / (totL + totR) );

      if (eReduction == maxReduction){
        if (rand.nextInt(10)<2) bestSplit=i; // randomly pick one out of several
      } else if (eReduction > maxReduction) {
        bestSplit = i;  maxReduction = eReduction;
      }
      if (i==0 && d.columnArity(col) == 1) break; // for boolean features, only one split needs to be evaluated
    }
    return bestSplit == -1
            ? Split.impossible(Utils.maxIndex(dist,_random))
            : Split.exclusion(col,bestSplit,maxReduction);
  }

  @Override
  protected Split ltSplit(int colIndex, Data d, float[] dist, float distWeight, Random rand) {
    return null;  //not called for classification
  }

  @Override
  protected Split eqSplit(int colIndex, Data d, float[] dist, float distWeight, Random rand) {
    return null;  //not called for classification
  }

  static final double log2 = Math.log(2);
}
