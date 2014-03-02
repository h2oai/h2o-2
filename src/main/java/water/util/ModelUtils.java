package water.util;

import java.util.Arrays;
import java.util.Random;

import water.H2O;

/**
 * Shared static code to support modeling, prediction, and scoring.
 *
 *  Used by interpreted models as well as by generated model code.
 *
 */
public class ModelUtils {

  /**
   *  Utility function to get a best prediction from an array of class
   *  prediction distribution.  It returns index of max value if predicted
   *  values are unique.  In the case of tie, the implementation solve it in
   *  psuedo-random way.
   *  @param preds an array of prediction distribution.  Length of arrays is equal to a number of classes+1.
   *  @return the best prediction (index of class, zero-based)
   */
  public static int getPrediction( float[] preds, double data[] ) {
    int best=1, tieCnt=0;   // Best class; count of ties
    for( int c=2; c<preds.length; c++) {
      if( preds[best] < preds[c] ) {
        best = c;               // take the max index
        tieCnt=0;               // No ties
      } else if (preds[best] == preds[c]) {
        tieCnt++;               // Ties
      }
    }
    if( tieCnt==0 ) return best-1; // Return zero-based best class
    // Tie-breaking logic
    float res = preds[best];    // One of the tied best results
    long hash = 0;              // hash for tie-breaking
    if( data != null )
      for( double d : data ) hash ^= Double.doubleToRawLongBits(d);
    int idx = (int)hash%(tieCnt+1);  // Which of the ties we'd like to keep
    for( best=1; best<preds.length; best++)
      if( res == preds[best] && --idx < 0 )
        return best-1;          // Return best
    throw H2O.fail();           // Should Not Reach Here
  }

  public static int getPrediction(float[] preds, int row) {
    int best=1, tieCnt=0;   // Best class; count of ties
    for( int c=2; c<preds.length; c++) {
      if( preds[best] < preds[c] ) {
        best = c;               // take the max index
        tieCnt=0;               // No ties
      } else if (preds[best] == preds[c]) {
        tieCnt++;               // Ties
      }
    }
    if( tieCnt==0 ) return best-1; // Return zero-based best class
    // Tie-breaking logic
    float res = preds[best];    // One of the tied best results
    int idx = row%(tieCnt+1);   // Which of the ties we'd like to keep
    for( best=1; best<preds.length; best++)
      if( res == preds[best] && --idx < 0 )
        return best-1;          // Return best
    throw H2O.fail();           // Should Not Reach Here
  }


  /**
   * Sample out-of-bag rows with given rate with help of given sampler.
   * It returns array of sampled rows. The first element of array contains a number
   * of sampled rows. The returned array can be larger than number of returned sampled
   * elements.
   *
   * @param nrows number of rows to sample from.
   * @param rate sampling rate
   * @param sampler random "dice"
   * @return an array contains numbers of sampled rows. The first element holds a number of sampled rows. The array length
   * can be greater than number of sampled rows.
   */
  public static int[] sampleOOBRows(int nrows, float rate, Random sampler) {
    return sampleOOBRows(nrows, rate, sampler, new int[1+(int)((1f-rate)*nrows*1.2f)]);
  }
  /**
   * In-situ version of {@link #sampleOOBRows(int, float, Random)}.
   *
   * @param oob an initial array to hold sampled rows. Can be internally realocted.
   * @return an array containing sampled rows.
   *
   * @see #sampleOOBRows(int, float, Random)
   */
  public static int[] sampleOOBRows(int nrows, float rate, Random sampler, int[] oob) {
    int oobcnt = 0; // Number of oob rows
    Arrays.fill(oob, 0);
    for(int row = 0; row < nrows; row++) {
      if (sampler.nextFloat() >= rate) { // it is out-of-bag row
        oob[1+oobcnt++] = row;
        if (1+oobcnt>=oob.length) oob = Arrays.copyOf(oob, (int)(1.2f*oob.length)+1);
      }
    }
    oob[0] = oobcnt;
    return oob;
  }
}
