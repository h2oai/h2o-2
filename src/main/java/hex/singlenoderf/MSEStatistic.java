package hex.singlenoderf;

import water.util.Utils;
import java.util.Random;

/** Computes the mse split statistics.
 *
 * For regression: Try to minimize the squared error at each split.
 */
public class MSEStatistic extends Statistic {

  public MSEStatistic(Data data, int features, long seed, int exclusiveSplitLimit) {
    super(data, features, seed, exclusiveSplitLimit, true /*regression*/);
  }

  private float computeAv(float[] dist, Data d, int sum) {
    float res = 0f;
    for (int i = 0; i < dist.length; ++i) {
      int tmp = (int) dist[i];
      res += d._dapt._c[d._dapt._c.length - 1]._binned2raw[i] * tmp;
    }
    return sum == 0 ? Float.POSITIVE_INFINITY : res / (float) sum;
  }

  private float[] computeDist(Data d, int colIndex) {
    float[] res = new float[d.columnArityOfClassCol()];
    for (int i = 0; i < _columnDistsRegression[colIndex].length - 1; ++i) {
      for (int j = 0; j < _columnDistsRegression[colIndex][i].length - 1; ++j) {
        res[j] += _columnDistsRegression[colIndex][i][j];
      }
    }
    return res;
  }

  @Override
  protected Split ltSplit(int colIndex, Data d, float[] dist, float distWeight, Random rand) {
    float bestSoFar = Float.POSITIVE_INFINITY;
    int bestSplit = -1;

    int lW = 0;
    int rW = d.rows();
    float[] leftDist = new float[d.columnArityOfClassCol()];
    float[] riteDist = computeDist(d, colIndex); //dist.clone();

    for (int j = 0; j < _columnDistsRegression[colIndex].length - 1; ++j) {
      for (int i = 0; i < dist.length; ++i) {
        int t = _columnDistsRegression[colIndex][j][i];
        lW += t;
        rW -= t;
        leftDist[i] += t;
        riteDist[i] -= t;
      }

      float Y_R = computeAv(riteDist, d, rW);
      float Y_L = computeAv(leftDist, d, lW);

      float newSplitValue = Y_R + Y_L;
      if (newSplitValue < bestSoFar) {
        bestSoFar = newSplitValue;
        bestSplit = j;
      }
    }

    return (bestSplit == -1 || bestSoFar == Float.POSITIVE_INFINITY)
            ? Split.impossible(Utils.maxIndex(computeDist(d, colIndex), _random))
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



  @Override protected Split ltSplit(int col, Data d, int[] dist, int distWeight, Random r) {
    return null; //not called for regression
  }

  @Override protected Split eqSplit(int colIndex, Data d, int[] dist, int distWeight, Random r) {
    return null; //not called for regression
  }
}
