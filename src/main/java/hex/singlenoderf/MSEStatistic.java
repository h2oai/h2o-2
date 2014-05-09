package hex.singlenoderf;

import water.util.Utils;
import java.util.Random;

/** Computes the mse split statistics.
 *
 * For regression: Try to minimize the squared error at each split.
 *
 * Begin with the sum of the current target responses in the node, Yt. Iterate over the rows
 * for the column and choose the split that minimizes the MSE. This will be the split s* that
 * _maximizes_ the following expression:
 *
 *
 *              (Y_L^2 * weights_left) + (Y_R^2 * weights_right)
 *
 *  Where Y_i = sum of the target responses going to node i (i is L or R),
 *  and weights_i = 1 / nobs_i (number of observations in the node).
 */
public class MSEStatistic extends Statistic {

  public MSEStatistic(Data data, int features, long seed, int exclusiveSplitLimit) {
    super(data, features, seed, exclusiveSplitLimit, true /*regression*/);
  }

  @Override
  protected Split ltSplit(int colIndex, Data d, float[] dist, float distWeight, Random rand) {
//    float Y_R = distWeight;
//    float Y_L = 0.f;
//    int nobs_R = d.rows();
//    int nobs_L = 0;
    float bestSoFar = 0.f;
    int bestSplit = -1;

    for (int j = 0; j < _columnDistsRegression[colIndex].length - 1; ++j) {
      float Y_R = distWeight;
      float Y_L = 0.f;
      int nobs_R = d.rows();
      int nobs_L = 0;
      for (float aDist : dist) {
        Y_L += aDist;
        Y_R -= aDist;
        nobs_L++;
        nobs_R--;
        float newSplitValue = (Y_L * Y_L / (float) nobs_L) + (Y_R * Y_R / (float) nobs_R);
        if (newSplitValue > bestSoFar) {
          bestSoFar = newSplitValue;
          bestSplit = j;
        }
      }
    }
    return bestSplit == -1
            ? Split.impossible(Utils.maxIndex(dist, _random))
            : Split.split(colIndex, bestSplit, bestSoFar);
  }

  @Override
  protected Split eqSplit(int colIndex, Data d, float[] dist, float distWeight, Random rand) {
    // we are not a single class, calculate the best split for the column
    int bestSplit = -1;
    float bestSoFar = 0.f;   // Fitness to maximize
    for( int i = 0; i < _columnDists[colIndex].length-1; ++i ) {
      float Y_incl = 0.f;
      float Y_excl = distWeight;
      int nobs_incl = 0;
      int nobs_excl = d.rows();
      for (float aDist : dist) {
        Y_incl += aDist;
        Y_excl -= aDist;
        nobs_incl++;
        nobs_excl--;
        float newSplitValue = (Y_incl * Y_incl / (float) nobs_incl) + (Y_excl * Y_excl / (float) nobs_excl);
        if (newSplitValue > bestSoFar) {
          bestSoFar = newSplitValue;
          bestSplit = i;
        }
      }
    }
    return bestSplit == -1
            ? Split.impossible(Utils.maxIndex(dist, _random))
            : Split.exclusion(colIndex, bestSplit, bestSoFar);
  }



  @Override protected Split ltSplit(int col, Data d, int[] dist, int distWeight, Random _) {
    return null; //not called for regression
  }

  @Override protected Split eqSplit(int colIndex, Data d, int[] dist, int distWeight, Random _) {
    return null; //not called for regression
  }
}