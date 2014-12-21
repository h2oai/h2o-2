package hex.singlenoderf;

import water.util.Utils;

import java.util.Random;

/** Computes the twoing split statistic.
 *
 * The decrease in (twoing) impurity as the result of a given split is
 * computed as follows:
 *
 * 1   weight left    weight right
 * - * ------------ * ------------- * twoing( left, right )
 * 4   weight total   weight total
 *
 * twoing( left, right ) = (\sum(|p_i(left) - p_i(right)|)^2, where
 * p_i( left ) is the fraction of observations in the left node of class i
 * p_i( right ) is the fraction of observations in the right node of class i
 *
 * The split that produces the largest decrease in impurity is selected.
 * Same is done for exclusions, where again left stands for the rows with column
 * value equal to the split value and right for all different ones.
 *
 * ece 11/14
 */
public class TwoingStatistic extends Statistic {

  public TwoingStatistic(Data data, int features, long seed, int exclusiveSplitLimit) { super(data, features, seed, exclusiveSplitLimit, false /*classification*/); }

  private double twoing(int[] dd_l, int sum_l, int[] dd_r, int sum_r ) {
    double result = 0.0;
    double sd_l = (double)sum_l;
    double sd_r = (double)sum_r;
    for (int i = 0; i < dd_l.length; i++) {
      double tmp = Math.abs(((double)dd_l[i])/sd_l - ((double)dd_r[i])/sd_r);
      result = result + tmp;
    }
    result = result * result;
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
      double f = 0.25 * ((double)lW / totWeight) * ((double)rW / totWeight) *
                 twoing(leftDist, lW, riteDist, rW);
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
      double f = ((double)inclW / distWeight) * ((double)exclW / distWeight) *
          twoing(inclDist, inclW, exclDist, exclW);
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
