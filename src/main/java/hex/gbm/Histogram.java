package hex.gbm;

import water.*;
import water.fvec.Frame;
import water.fvec.Vec;
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

   If we are successively splitting rows (e.g. in a decision tree), then a
   fresh {@code Histogram} for each split will dynamically re-bin the data.
   Each successive split then, will logarithmically divide the data.  At the
   first split, outliers will end up in their own bins - but perhaps some
   central bins may be very full.  At the next split(s), the full bins will get
   split, and again until (with a log number of splits) each bin holds roughly
   the same amount of data.

   @author Cliff Click
*/

class Histogram extends Iced implements Cloneable {
  public static final int BINS=4;
  transient final String   _name;        // Column name, for pretty-printing
  public    final boolean  _isInt;       // Column only holds integers
  public    final double   _step;        // Linear interpolation step per bin
  public    final double   _min, _max;   // Lower-end of binning
  public    final int      _nbins;       // Number of bins
  public          long  [] _bins;        // Bin counts
  public          double[] _mins, _maxs; // Min, Max, per-bin
  // For Classification trees, use class-counts
  public          long[][] _clss;        // Class counts, per-bin
  // For Regression trees, using mean & variance
  public          double[] _Ms;          // Rolling mean, per-bin
  public          double[] _Ss;          // Rolling var , per-bin
  public          double[] _MSEs;        // Rolling mean-square-error, per-bin; requires 2nd pass

  // Fill in read-only sharable values
  public Histogram( String name, long nelems, double min, double max, boolean isInt ) {
    assert nelems > 0;
    assert max > min : "Caller ensures "+max+">"+min+", since if max==min== the column "+name+" is all constants";
    _name = name;
    _min = min;  _max=max;  _isInt = isInt;
    int xbins = Math.max((int)Math.min(BINS,nelems),1); // Default bin count
    // See if we can show there are fewer unique elements than nbins.
    // Common for e.g. boolean columns, or near leaves.
    int nbins = xbins;      // Default size for most columns        
    if( isInt && max-min < xbins )
      nbins = (int)((long)max-(long)min+1L); // Shrink bins
    _nbins = nbins;
    _step = (max-min)/_nbins;   // Step size for linear interpolation
  }

  // Copy from the original Histogram, but then allocate private arrays
  public Histogram copy( int numClasses ) {
    assert _bins==null && _maxs == null; // Nothing filled-in yet
    Histogram h=(Histogram)clone();
    // Build bin stats
    h._bins = MemoryManager.malloc8 (_nbins);
    h._mins = MemoryManager.malloc8d(_nbins);
    h._maxs = MemoryManager.malloc8d(_nbins);
    // Set step & min/max for each bin
    for( int j=0; j<_nbins; j++ ) { // Set bad bounds for min/max
      h._mins[j] =  Double.MAX_VALUE;
      h._maxs[j] = -Double.MAX_VALUE;
    }
    h._mins[       0] = _min; // Know better bounds for whole column min/max
    h._maxs[_nbins-1] = _max;
    if( numClasses == 0 ) {
      h._Ms = MemoryManager.malloc8d(_nbins);
      h._Ss = MemoryManager.malloc8d(_nbins);
    } else {
      h._clss = new long[_nbins][];
      for( int i=0; i<_nbins; i++ ) h._clss[i] = MemoryManager.malloc8(numClasses);
    }
    return h;
  }

  // Add 1 count to bin specified by double.  Simple linear interpolation to
  // specify bin.  Also passed in the response variable; add to the variance
  // per-bin using the recursive strategy.
  //   http://www.johndcook.com/standard_deviation.html
  void incr( double d, double y ) {
    assert !Double.isNaN(y);
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
  // Add 1 count to bin specified by double.  Simple linear interpolation to
  // specify bin.  Also passed in the response variable, which is a class.
  void incr( double d, int y ) {
    assert !Double.isNaN(y);
    int idx = bin(d);           // Compute bin# via linear interpolation
    _bins[idx]++;               // Bump count in bin
    // Track actual lower/upper bound per-bin
    if( d < _mins[idx] ) _mins[idx] = d;
    if( d > _maxs[idx] ) _maxs[idx] = d;
    _clss[idx][y]++;            // Bump class count
  }

  // Same as incr, but compute mean-square-error - which requires the mean as
  // computed on the 1st pass above, meaning this requires a 2nd pass.
  void incr2( double d, double y ) {
    if( _MSEs == null ) _MSEs = new double[_bins.length];
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
    int idx1  = _step <= 0.0 ? 0 : (int)((d-_min)/_step);
    int idx2  = Math.max(Math.min(idx1,_nbins-1),0); // saturate at bounds
    return idx2;
  }
  double mean( int bin ) { return _Ms[bin]; }
  double var ( int bin ) { 
    return _bins[bin] > 1 ? _Ss[bin]/(_bins[bin]-1) : 0; 
  }
  double mse( int bin ) { return _MSEs[bin]; }

  // Compute a "score" for a column; lower score "wins" (is a better split).
  double score( ) {
    return _clss == null ? scoreRegression() : scoreClassification();
  }
  // Compute a "score" for a column; lower score "wins" (is a better split).
  // Score for a Regression tree is the sum of variances.
  private double scoreRegression( ) {
    double sum = 0;
    for( int i=0; i<_bins.length; i++ )
      sum += var(i)*_bins[i];
    return sum;
  }
  // Compute a "score" for a column; lower score "wins" (is a better split).
  // Score for a Classification tree is sum of the errors per-class.  We
  // predict a row using the ratio of the response class to all the rows in the
  // split.  
  //
  // Example: we have 10 rows, classed as 8 C0's, 1 C1's and 1 C2's.  The C0's
  // are all predicted as "80% C0".  We have 8 rows which are 80% correctly C0,
  // and 2 rows which are 10% correct.  The total error is 8*(1-.8)+2*(1-.1) =
  // 3.4.
  //
  // Example: we have 10 rows, classed as 6 C0's, 2 C1's and 2 C2's.  Total
  // error is: 6*(1-.6)+4*(1-.2) = 5.6
  private double scoreClassification( ) {
    double sum = 0;
    int numClasses = _clss[0].length;
    for( int i=0; i<_bins.length; i++ ) {
      if( _bins[i] <= 1 ) continue;
      // A little algebra, and the math we need is:
      //    N - (sum(clss^2)/N)
      int err=0;
      for( int j=0; j<numClasses; j++ )
        err += _clss[i][j]*_clss[i][j];
      sum += (double)_bins[i] - ((double)err/_bins[i]);
    }
    return sum;
  }


  // Compute a "score" for a column; lower score "wins" (is a better split).
  // Score is the squared-error for the column.  Requires a 2nd pass, as the
  // mean is computed on the 1st pass.
  double scoreMSE( ) {
    assert _MSEs != null : "Need to call incr2 to use MSE";
    double sum = 0;
    for( int i=0; i<_bins.length; i++ )
      sum += mse(i)*_bins[i];
    return sum;
  }

  // Remove old histogram data, but keep enough info to predict.  Cuts down
  // the size of data to move over the wires
  public void clean() {  
    if( _clss == null ) _bins = null; // Not needed for regression trees
    _Ss = _mins = _maxs = _MSEs = null; 
  }

  // After having filled in histogram bins, compute tighter min/max bounds.
  // Store tighter bounds into the first and last bins.
  public void tightenMinMax() {
    int n = 0;
    while( _bins[n]==0 ) n++;   // First non-empty bin
    if( n > 0 ) _mins[0] = _mins[n]; // Take min from  1st non-empty bin into bin 0
    if( _mins[0] > _maxs[0] ) _maxs[0] = _mins[0];
    int l = _bins.length-1;     // Last bin
    int x = l;  
    while( _bins[x]==0 ) x--;   // Last non-empty bin
    if( x < l ) _maxs[l] = _maxs[x]; // Take max from last non-empty bin into bin last
    if( _maxs[l] < _mins[l] ) _mins[l] = _maxs[x];
  }

  // Split bin 'i' of this Histogram.  Return null if there is no point in
  // splitting this bin further (such as there's only 1 element, or zero
  // variance in the response column).  Return an array of Histograms (one per
  // column), which are bounded by the split bin-limits.  If the column has
  // constant data, or was not being tracked by a prior Histogram (for being
  // constant data from a prior split), then that column will be null in the
  // returned array.
  public Histogram[] split( int col, int i, Histogram hs[], String[] names, int ncols ) {
    assert hs[col] == this;
    if( _bins[i] <= 1 ) return null; // Zero or 1 elements
    if( _clss == null ) {            // Regresion?
      if( var(i) == 0.0 ) return null; // No point in splitting a perfect prediction
    } else {                         // Classification
      long cls[] = _clss[i];         // See if we got a perfect prediction
      for( int j=0; j<cls.length; j++ )
        if( cls[j] == _bins[i] )     // Some class has all the bin counts?
          return null;
    }

    // Build a next-gen split point from the splitting bin
    Histogram nhists[] = new Histogram[ncols]; // A new histogram set
    for( int j=0; j<ncols; j++ ) { // For every column in the new split
      Histogram h = hs[j];         // Old histogram of column
      if( h == null ) continue;    // Column was not being tracked?
      // min & max come from the original column data, since splitting on an
      // unrelated column will not change the j'th columns min/max.
      double min = h._mins[0], max = h._maxs[h._maxs.length-1];
      // Tighter bounds on the column getting split: exactly each new
      // Histogram's bound are the bins' min & max.
      if( col==j ) { min=h._mins[i]; max=h._maxs[i]; }
      if( min == max ) continue; // This column will not split again
      nhists[j] = new Histogram(names[j],_bins[i],min,max,hs[j]._isInt);
    }
    return nhists;
  }

  // An initial set of Histograms (one per column) for this column set
  public static Histogram[] initialHist( Frame fr, int ncols ) {
    Histogram hists[] = new Histogram[ncols];
    Vec[] vs = fr._vecs;
    for( int j=0; j<ncols; j++ ) {
      Vec v = vs[j];
      hists[j] = v.min()==v.max() ? null 
        : new Histogram(fr._names[j],v.length(),v.min(),v.max(),v._isInt);
    }
    return hists;
  }

  // "reduce" 'h' into 'this'.  Combine mean & variance using the
  // recursive-mean technique.  Compute min-of-mins and max-of-maxes, etc.
  public void add( Histogram h ) {
    assert _nbins == h._nbins;
    for( int i=0; i<_bins.length; i++ ) {
      long k1 = _bins[i], k2 = h._bins[i];
      if( k1==0 && k2==0 ) continue;
      _bins[i]=k1+k2;
      if( _clss == null ) {     // Regression roll-up
        // Recursive mean & variance
        //    http://www.johndcook.com/standard_deviation.html
        //    http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
        double m1 = _Ms[i], m2 = h.  _Ms[i];
        double s1 = _Ss[i], s2 = h.  _Ss[i];
        double delta=m2-m1;
        _Ms[i] = (k1*m1+k2*m2)/(k1+k2);           // Mean
        _Ss[i] = s1+s2+delta*delta*k1*k2/(k1+k2); // 2nd moment
      } else {               // Classification: just sum up the class histogram
        for( int j=0; j<_clss[i].length; j++ )
          _clss[i][j] += h._clss[i][j];
      }
    }

    for( int i=0; i<_mins.length; i++ ) if( h._mins[i] < _mins[i] ) _mins[i] = h._mins[i];
    for( int i=0; i<_maxs.length; i++ ) if( h._maxs[i] > _maxs[i] ) _maxs[i] = h._maxs[i];
    if( _MSEs != null ) throw H2O.unimpl();
  }

  // Pretty-print a histogram
  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(_name).append("\n");
    if( _clss == null ) {
      for( int i=0; i<_bins.length; i++ )
        sb.append(String.format("cnt=%d, min=%f, max=%f, mean=%f, var=%f\n",
                                _bins[i],_mins[i],_maxs[i],
                                _Ms==null?Double.NaN:mean(i),
                                _Ms==null?Double.NaN:var (i)));
    } else {
      int numClasses = _clss[0].length;
      for( int i=0; i<_bins.length; i++ ) {
        sb.append(String.format("cnt=%d, min=%f, max=%f, ", _bins[i],_mins[i],_maxs[i]));
        for( int c=0; c<numClasses; c++ )
          sb.append("c").append(c).append("=").append(_clss[i][c]).append(",");
        sb.append("\n");
      }
    }
    return sb.toString();
  }

  private int byteSize(double[]ds) { return ds==null ? 0 : 24+ds.length*8; }
  private int byteSize(long  []ls) { return ls==null ? 0 : 24+ls.length*8; }
  private int byteSize(short []ss) { return ss==null ? 0 : 20+ss.length*2; }
  public long byteSize() {
    int sum = 14*8;             // Rough size of empty Histogram
    sum += byteSize(_bins);
    sum += byteSize(_mins);
    sum += byteSize(_maxs);
    sum += byteSize(_Ms);
    sum += byteSize(_Ss);
    sum += byteSize(_MSEs);
    if( _clss != null ) {
      sum += (_clss.length+3)*8;
      for( short[] ss : _clss ) sum += byteSize(ss);
    }
    if( _clss2 != null ) {
      sum += (_clss2.length+3)*8;
      for( long[] ss : _clss2 ) sum += byteSize(ss);
    }
    return sum;
  }
}
