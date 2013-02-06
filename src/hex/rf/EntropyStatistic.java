 package hex.rf;

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

  public EntropyStatistic(Data data, int features, long seed, int exclusiveSplitLimit) { super(data, features, seed, exclusiveSplitLimit); }
  /** LessThenEqual splits s*/
  @Override protected Split ltSplit(int col, Data d, int[] dist, int distWeight, Random rand) {
    final int[] distL = new int[d.classes()], distR = dist.clone();
    final double upperBoundReduction = upperBoundReduction(d.classes());
    double maxReduction = -1;
    int bestSplit = -1;
    for (int i = 0; i < _columnDists[col].length - 1; ++i) {
      for (int j = 0; j < distL.length; ++j) {
        double v = _columnDists[col][i][j];
        distL[j] += v;  distR[j] -= v;
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

      if (eReduction == maxReduction) {
        if (rand.nextInt(10)<2) bestSplit=i;
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
  static final double log2 = Math.log(2);
}
