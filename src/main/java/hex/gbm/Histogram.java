package hex.gbm;

import water.*;
import water.util.Log;

/**
   A Histogram, computed in parallel over a Vec.

   A {@code Histogram} bins (by default into {@value BINS} bins) every value
   added to it, and computes a the min, max, mean & variance for each bin.
   {@code Histogram}s are initialized with a min, max and number-of-elements to
   be added (all of which are generally available from a Vec).  Bins normally
   run from min to max in uniform sizes, but if the {@code Histogram} can
   determine that fewer bins are needed (e.g. boolean columns run from 0 to 1,
   but only ever take on 2 values, so only 2 bins are needed), then fewer bins
   are used.

   @author Cliff Click
*/

// A Histogram over a particular Split.  The histogram runs from min to max
// per each column (i.e., we actually make #cols histograms in parallel), and
// is given the number of elements that will land in some bin (for small
// enough elements, we make fewer bins).  Each column's range is independent
// and recomputed at each split/histogram
class Histogram extends Iced {
  public static final int BINS=4;
  transient final String   _name;        // Column name, for pretty-printing
  public    final double   _step;        // Linear interpolation step per bin
  public    final long  [] _bins;        // Bin counts
  public    final double[] _Ms;          // Rolling mean, per-bin
  public    final double[] _Ss;          // Rolling var , per-bin
  public    final double[] _mins, _maxs; // Min, Max, per-bin
  public    final double[] _MSEs;        // Rolling mean-square-error, per-bin

  public Histogram( String name, long nelems, double min, double max, boolean isInt ) {
    assert nelems > 0;
    assert max > min : "Caller ensures max>min, since if max==min the column is all constants";
    _name = name;
    int xbins = Math.max((int)Math.min(BINS,nelems),1); // Default bin count
    // See if we can show there are fewer unique elements than nbins.
    // Common for e.g. boolean columns, or near leaves.
    int nbins = xbins;      // Default size for most columns        
    if( isInt && max-min < xbins )
      nbins = (int)((long)max-(long)min+1L); // Shrink bins
    // Build bin stats
    _bins = new long  [nbins];
    _Ms   = new double[nbins];
    _Ss   = new double[nbins];
    _mins = new double[nbins];
    _maxs = new double[nbins];
    _MSEs = new double[nbins];
    // Set step & min/max for each bin
    _step = (max-min)/nbins;       // Step size for linear interpolation
    for( int j=0; j<nbins; j++ ) { // Set bad bounds for min/max
      _mins[j] =  Double.MAX_VALUE;
      _maxs[j] = -Double.MAX_VALUE;
    }
    _mins[      0] = min; // Know better bounds for whole column min/max
    _maxs[nbins-1] = max;
  }

  // Add 1 count to bin specified by double.  Simple linear interpolation to
  // specify bin.  Also passed in the response variable; add to the variance
  // per-bin using the recursive strategy.
  //   http://www.johndcook.com/standard_deviation.html
  void incr( double d, double y ) {
    int idx = bin(d);           // Compute bin# via linear interpolation
    _bins[idx]++;               // Bump count in bin
    // Track actual lower/upper bound per-bin
    if( d < _mins[idx] ) _mins[idx] = d;
    if( d > _maxs[idx] ) _maxs[idx] = d;
    // Recursive mean & variance
    //    http://www.johndcook.com/standard_deviation.html
    long k = _bins[idx];
    double oldM = _Ms[idx], newM = oldM + (y-oldM)/k;
    double oldS = _Ss[idx], newS = oldS + (y-oldM)*(y-newM);
    _Ms[idx] = newM;
    _Ss[idx] = newS;
  }

  // Same as incr, but compute mean-square-error - which requires
  // the mean as computed on the 1st pass above.
  void incr2( double d, double y ) {
    int idx = bin(d);           // Compute bin# via linear interpolation
    double z = y-mean(idx);     // Error for this prediction
    double e = z*z;             // Squared error
    // Recursive mean of squared error
    //    http://www.johndcook.com/standard_deviation.html
    double oldMSE = _MSEs[idx];
    _MSEs[idx] = oldMSE + (e-oldMSE)/_bins[idx];
  }

  // Interpolate d to find bin#
  int bin( double d ) {
    int nbins = _bins .length;
    int idx1  = _step <= 0.0 ? 0 : (int)((d-_mins[0])/_step);
    int idx2  = Math.max(Math.min(idx1,nbins-1),0); // saturate at bounds
    return idx2;
  }
  double mean( int bin ) { return _Ms[bin]; }
  double var ( int bin ) { 
    return _bins[bin] > 1 ? _Ss[bin]/(_bins[bin]-1) : 0; 
  }
  double mse( int bin ) { return _MSEs[bin]; }

  // Compute a "score" for a column; lower score "wins" (is a better split).
  // CURRENTLY COMPUTING THE WRONG THING...
  // Score is related to variance; a lower variance is better.  For now
  // return the sum of variance across the column, divided by the mean.
  // Dividing normalizes the column to other columns.
  // THE RIGHT THING: minimize the sum of squared errors.  Equivalent to mean
  // of squared errors, which can be done with the online mean algorithm.
  double score( ) {
    double sum = 0;
    double sum2 = 0;
    int nbins = _bins.length;
    for( int i=0; i<nbins; i++ ) {
      double m = mean(i);
      double x = m==0.0 ? 0 : var(i)/m;
      sum += x;
      sum2 += mse(i)*_bins[i];
    }
    //Log.unwrap(System.out,"var="+sum+" mse="+sum2);
    return sum2;
  }

  // Pretty-print a histogram
  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(_name).append("\n");
    for( int i=0; i<_bins.length; i++ )
      sb.append(String.format("cnt=%d, min=%f, max=%f, mean=%f, var=%f\n",
                              _bins[i],_mins[i],_maxs[i],mean(i),var(i)));
      
    return sb.toString();
  }
}
