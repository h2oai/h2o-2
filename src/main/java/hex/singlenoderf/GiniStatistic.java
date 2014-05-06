package hex.singlenoderf;

import water.util.Utils;
import java.util.Random;

/** Computes the gini split statistics.
 *
 * The Gini fitness is calculated as a probability that the element will be
 * misclassified, which is:
 *
 * 1 - \sum(p_i^2)
 *
 * This is computed for the left and right subtrees and added together:
 *
 * gini left * weight left + gini right * weight left
 * --------------------------------------------------
 *                weight total
 *
 * And subtracted from an ideal worst 1 to simulate the gain from previous node.
 * The best gain is then selected. Same is done for exclusions, where again
 * left stands for the rows with column value equal to the split value and
 * right for all different ones.
 */
public class GiniStatistic extends Statistic {

  public GiniStatistic(Data data, int features, long seed, int exclusiveSplitLimit) { super(data, features, seed, exclusiveSplitLimit, false /*classification*/); }

  private double gini(int[] dd, int sum) {
    double result = 1.0;
    double sd = (double)sum;
    for (int d : dd) {
      double tmp = ((double)d)/sd;
      result -= tmp*tmp;
    }
    return result;
  }

  @Override protected Split ltSplit(int col, Data d, int[] dist, int distWeight, Random _) {
    int[] leftDist = new int[d.classes()];
    int[] riteDist = dist.clone();
    int lW = 0;
    int rW = distWeight;
    double totWeight = rW;
    // we are not a single class, calculate the best split for the column
    int bestSplit = -1;
    double bestFitness = 0.0;
    assert leftDist.length==_columnDists[col][0].length;

    for (int i = 0; i < _columnDists[col].length-1; ++i) {
      // first copy the i-th guys from rite to left
      for (int j = 0; j < leftDist.length; ++j) {
        int t = _columnDists[col][i][j];
        lW += t;
        rW -= t;
        leftDist[j] += t;
        riteDist[j] -= t;
      }
      // now make sure we have something to split
      if( lW == 0 || rW == 0 ) continue;
      double f = 1.0 -
              (gini(leftDist,lW) * ((double)lW / totWeight) +
                      gini(riteDist,rW) * ((double)rW / totWeight));
      if( f>bestFitness ) { // Take split with largest fitness
        bestSplit = i;
        bestFitness = f;
      }
    }
    return bestSplit == -1
            ? Split.impossible(Utils.maxIndex(dist, _random))
            : Split.split(col, bestSplit, bestFitness);
  }

  @Override protected Split eqSplit(int colIndex, Data d, int[] dist, int distWeight, Random _) {
    int[] inclDist = new int[d.classes()];
    int[] exclDist = dist.clone();
    // we are not a single class, calculate the best split for the column
    int bestSplit = -1;
    double bestFitness = 0.0;   // Fitness to maximize
    for( int i = 0; i < _columnDists[colIndex].length-1; ++i ) {
      // first copy the i-th guys from rite to left
      int sumt = 0;
      for( int j = 0; j < inclDist.length; ++j ) {
        int t = _columnDists[colIndex][i][j];
        sumt += t;
        inclDist[j] = t;
        exclDist[j] = dist[j] - t;
      }
      int inclW = sumt;
      int exclW = distWeight - inclW;
      // now make sure we have something to split
      if( inclW == 0 || exclW == 0 ) continue;
      double f = 1.0 -
              (gini(inclDist,inclW) * ((double)inclW / distWeight) +
                      gini(exclDist,exclW) * ((double)exclW / distWeight));
      if( f>bestFitness ) { // Take split with largest fitness
        bestSplit = i;
        bestFitness = f;
      }
    }
    return bestSplit == -1
            ? Split.impossible(Utils.maxIndex(dist, _random))
            : Split.exclusion(colIndex, bestSplit, bestFitness);
  }

  @Override
  protected Split ltSplit(int colIndex, Data d, float[] dist, float distWeight, Random rand) {
    return null;  //not called for classification
  }

  @Override
  protected Split eqSplit(int colIndex, Data d, float[] dist, float distWeight, Random rand) {
    return null;  //not called for classification
  }
}
