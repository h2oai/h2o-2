package hex.gbm;

import water.*;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log;

/**
   A DBinHistogram, computed in parallel over a Vec.
   <p>
   A {@code DBinHistogram} bins (by default into {@value BINS} bins) every
   value added to it, and computes a the min, max, and either class
   distribution or mean & variance for each bin.  {@code DBinHistogram}s are
   initialized with a min, max and number-of-elements to be added (all of which
   are generally available from a Vec).  Bins normally run from min to max in
   uniform sizes, but if the {@code DBinHistogram} can determine that fewer
   bins are needed (e.g. boolean columns run from 0 to 1, but only ever take on
   2 values, so only 2 bins are needed), then fewer bins are used.
   <p>
   If we are successively splitting rows (e.g. in a decision tree), then a
   fresh {@code DBinHistogram} for each split will dynamically re-bin the data.
   Each successive split then, will logarithmically divide the data.  At the
   first split, outliers will end up in their own bins - but perhaps some
   central bins may be very full.  At the next split(s), the full bins will get
   split, and again until (with a log number of splits) each bin holds roughly
   the same amount of data.
   <p>
   @author Cliff Click
*/
public class DBinHistogram extends DHistogram<DBinHistogram> {
  public final float   _step;        // Linear interpolation step per bin
  public final float   _bmin;        // Linear interpolation step per bin
  public final char    _nbins;       // Number of bins
  public final short   _nclass;      // Number of classes
  public       long [] _bins;        // Bin counts
  public       float[] _mins, _maxs; // Min, Max, per-bin
  // For Regression trees, using mean & variance
  public       float[] _Ms;          // Rolling mean, per-bin
  public       float[] _Ss;          // Rolling var , per-bin
  public       float[] _MSEs;        // Rolling mean-square-error, per-bin; requires 2nd pass
  // For Classification trees, use class-counts - fractional estimates of a
  // class.  For RF, class-counts will always be 1.0 for the response variable
  // and zero otherwise.  For GBM, class-counts are the residuals by class.
  private      float[/*bin*/][/*class*/] _clss; // Class counts, per-bin

  // Fill in read-only sharable values
  public DBinHistogram( String name, short nclass, boolean isInt, float min, float max, long nelems ) {
    super(name,isInt,min,max);
    assert nelems > 0;
    assert max > min : "Caller ensures "+max+">"+min+", since if max==min== the column "+name+" is all constants";
    int xbins = Math.max((int)Math.min(BINS,nelems),1); // Default bin count
    // See if we can show there are fewer unique elements than nbins.
    // Common for e.g. boolean columns, or near leaves.
    int nbins = xbins;      // Default size for most columns        
    if( isInt && max-min < xbins )
      nbins = (int)((long)max-(long)min+1L); // Shrink bins
    _nbins = (char)nbins;
    _nclass = nclass;
    _bmin = min;                // Bin-Min
    _step = (max-min)/_nbins;   // Step size for linear interpolation
  }
  boolean isRegression() { return _nclass==1; }

  // Copy from the original DBinHistogram, but return a smaller non-binning DHistogram.
  // The new DHistogram will only collect more refined min/max values.
  @Override public DHistogram smallCopy( ) {
    return new DHistogram(_name,_isInt/*,_min,_max*/);
  }

  // Copy from the original DBinHistogram, but then allocate private arrays
  @Override public DBinHistogram bigCopy( ) {
    assert _bins==null && _maxs == null; // Nothing filled-in yet
    DBinHistogram h=(DBinHistogram)clone();
    // Build bin stats
    h._bins = MemoryManager.malloc8 (_nbins);
    h._mins = MemoryManager.malloc4f(_nbins);
    h._maxs = MemoryManager.malloc4f(_nbins);
    // Set step & min/max for each bin
    for( int j=0; j<_nbins; j++ ) { // Set bad bounds for min/max
      h._mins[j] =  Float.MAX_VALUE;
      h._maxs[j] = -Float.MAX_VALUE;
    }
    if( isRegression() ) {
      h._Ms = MemoryManager.malloc4f(_nbins);
      h._Ss = MemoryManager.malloc4f(_nbins);
    } else {
      h._clss = new float[_nbins][];
    }
    return h;
  }

  // Interpolate d to find bin#
  int bin( float d ) {
    int idx1  = _step <= 0.0 ? 0 : (int)((d-_bmin)/_step);
    int idx2  = Math.max(Math.min(idx1,_nbins-1),0); // saturate at bounds
    return idx2;
  }

  @Override int nbins() { return _nbins; }
  @Override long bins(int i) { return _bins[i]; }
  @Override float mins(int i) { return _mins[i]; }
  @Override float maxs(int i) { return _maxs[i]; }

  // Compute a "score" for a column; lower score "wins" (is a better split).
  float score( ) {
    return isRegression() ? scoreRegression() : scoreClassification();
  }

  // Add 1 count to bin specified by float.  Simple linear interpolation to
  // specify bin.  Also passed in the response variable, which is a class.
  void incr( float d, int y ) {
    assert !Float.isNaN(y);
    int idx = bin(d);           // Compute bin# via linear interpolation
    _bins[idx]++;               // Bump count in bin
    // Track actual lower/upper bound per-bin
    if( d < _mins[idx] ) _mins[idx] = d;
    if( d > _maxs[idx] ) _maxs[idx] = d;
    add_class(idx,y,1);         // Bump class count
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
  float scoreClassification( ) {
    float sum = 0;
    for( int i=0; i<_bins.length; i++ ) {
      if( _bins[i] <= 1 ) continue;
      // A little algebra, and the math we need is:
      //    N - (sum(clss^2)/N)
      float err=0;
      for( int j=0; j<_nclass; j++ ) {
        float c = clss(i,j);
        err += c*c;
      }
      sum += (float)_bins[i] - (err/_bins[i]);
    }
    return sum;
  }

  @Override float mean( int bin ) { return _Ms[bin]; }
  @Override float var ( int bin ) { 
    return _bins[bin] > 1 ? _Ss[bin]/(_bins[bin]-1) : 0; 
  }
  float mse( int bin ) { return _MSEs[bin]; }

  // Add 1 count to bin specified by float.  Simple linear interpolation to
  // specify bin.  Also passed in the response variable; add to the variance
  // per-bin using the recursive strategy.
  //   http://www.johndcook.com/standard_deviation.html
  void incr( float d, float y ) {
    assert !Float.isNaN(y);
    int idx = bin(d);           // Compute bin# via linear interpolation
    _bins[idx]++;               // Bump count in bin
    // Track actual lower/upper bound per-bin
    if( d < _mins[idx] ) _mins[idx] = d;
    if( d > _maxs[idx] ) _maxs[idx] = d;
    // Recursive mean & variance
    //    http://www.johndcook.com/standard_deviation.html
    long k = _bins[idx];
    float oldM = _Ms[idx], newM = oldM + (y-oldM)/k;
    float oldS = _Ss[idx], newS = oldS + (y-oldM)*(y-newM);
    _Ms[idx] = newM;
    _Ss[idx] = newS;
  }

  // Same as incr, but compute mean-square-error - which requires the mean as
  // computed on the 1st pass above, meaning this requires a 2nd pass.
  void incr2( float d, float y ) {
    if( _MSEs == null ) _MSEs = new float[_bins.length];
    int idx = bin(d);           // Compute bin# via linear interpolation
    float z = y-mean(idx);     // Error for this prediction
    float e = z*z;             // Squared error
    // Recursive mean of squared error
    //    http://www.johndcook.com/standard_deviation.html
    float oldMSE = _MSEs[idx];
    _MSEs[idx] = oldMSE + (e-oldMSE)/_bins[idx];
  }

  // Compute a "score" for a column; lower score "wins" (is a better split).
  // Score for a Regression tree is the sum of variances.
  float scoreRegression( ) {
    float sum = 0;
    for( int i=0; i<_bins.length; i++ )
      sum += var(i)*_bins[i];
    return sum;
  }

  // Compute a "score" for a column; lower score "wins" (is a better split).
  // Score is the squared-error for the column.  Requires a 2nd pass, as the
  // mean is computed on the 1st pass.
  float scoreMSE( ) {
    assert _MSEs != null : "Need to call incr2 to use MSE";
    float sum = 0;
    for( int i=0; i<_bins.length; i++ )
      sum += mse(i)*_bins[i];
    return sum;
  }

  // After having filled in histogram bins, compute tighter min/max bounds.
  // Store tighter bounds into the first and last bins.
  @Override public void tightenMinMax() {
    int n = 0;
    while( _bins[n]==0 ) n++;   // First non-empty bin
    _min = _mins[n];    // Take min from 1st non-empty bina
    int x = _bins.length-1;     // Last bin
    while( _bins[x]==0 ) x--;   // Last non-empty bin
    _max = _maxs[x];    // Take max from last non-empty bin
  }

  // Split bin 'i' of this DBinHistogram.  Return null if there is no point in
  // splitting this bin further (such as there's only 1 element, or zero
  // variance in the response column).  Return an array of DBinHistograms (one per
  // column), which are bounded by the split bin-limits.  If the column has
  // constant data, or was not being tracked by a prior DBinHistogram (for being
  // constant data from a prior split), then that column will be null in the
  // returned array.
  public DBinHistogram[] split( int col, int i, DHistogram hs[], String[] names, int ncols ) {
    assert hs[col] == this;
    if( _bins[i] <= 1 ) return null; // Zero or 1 elements
    if( _nclass == 0 ) {             // Regression?
      if( var(i) == 0.0 ) return null; // No point in splitting a perfect prediction
    } else {                         // Classification
      for( int j=0; j<_nclass; j++ ) // See if we got a perfect prediction
        if( clss(i,j) == _bins[i] )  // Some class has all the bin counts?
          return null;
    }

    // Build a next-gen split point from the splitting bin
    DBinHistogram nhists[] = new DBinHistogram[ncols]; // A new histogram set
    for( int j=0; j<ncols; j++ ) { // For every column in the new split
      DHistogram h = hs[j];        // Old histogram of column
      if( h == null ) continue;    // Column was not being tracked?
      // min & max come from the original column data, since splitting on an
      // unrelated column will not change the j'th columns min/max.
      float min = h._min, max = h._max;
      // Tighter bounds on the column getting split: exactly each new
      // DBinHistogram's bound are the bins' min & max.
      if( col==j ) { min=h.mins(i); max=h.maxs(i); }
      if( min == max ) continue; // This column will not split again
      nhists[j] = new DBinHistogram(names[j],_nclass,hs[j]._isInt,min,max,_bins[i]);
    }
    return nhists;
  }

  // An initial set of DBinHistograms (one per column) for this column set
  public static DBinHistogram[] initialHist( Frame fr, int ncols, short nclass ) {
    DBinHistogram hists[] = new DBinHistogram[ncols];
    Vec[] vs = fr._vecs;
    for( int j=0; j<ncols; j++ ) {
      Vec v = vs[j];
      hists[j] = v.min()==v.max() ? null 
        : new DBinHistogram(fr._names[j],nclass,v._isInt,(float)v.min(),(float)v.max(),v.length());
    }
    return hists;
  }

  // "reduce" 'h' into 'this'.  Combine mean & variance using the
  // recursive-mean technique.  Compute min-of-mins and max-of-maxes, etc.
  @Override void add( DBinHistogram h ) {
    assert _nbins == h._nbins;
    super.add(h);
    for( int i=0; i<_bins.length; i++ ) {
      long k1 = _bins[i], k2 = h._bins[i];
      if( k1==0 && k2==0 ) continue;
      _bins[i]=k1+k2;
      if( _nclass == 0 ) {      // Regression roll-up
        // Recursive mean & variance
        //    http://www.johndcook.com/standard_deviation.html
        //    http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
        float m1 = _Ms[i], m2 = h.  _Ms[i];
        float s1 = _Ss[i], s2 = h.  _Ss[i];
        float delta=m2-m1;
        _Ms[i] = (k1*m1+k2*m2)/(k1+k2);           // Mean
        _Ss[i] = s1+s2+delta*delta*k1*k2/(k1+k2); // 2nd moment
      } else {               // Classification: just sum up the class histogram
        for( int j=0; j<_nclass; j++ )
          add_class(i,j,h.clss(i,j));
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
    if( _nclass==0 ) {
      for( int i=0; i<nbins(); i++ )
        sb.append(String.format("cnt=%d, min=%f, max=%f, mean=%f, var=%f\n",
                                _bins[i],_mins[i],_maxs[i],
                                _Ms==null?Float.NaN:mean(i),
                                _Ms==null?Float.NaN:var (i)));
    } else {
      for( int i=0; i<nbins(); i++ ) {
        sb.append(String.format("cnt=%d, min=%f, max=%f, ", _bins[i],_mins[i],_maxs[i]));
        for( int c=0; c<_nclass; c++ )
          sb.append("c").append(c).append("=").append(clss(i,c)).append(",");
        sb.append("\n");
      }
    }
    return sb.toString();
  }

  @Override long byteSize() {
    long sum = super.byteSize();
    sum += 8+byteSize(_bins);
    sum += 8+byteSize(_mins);
    sum += 8+byteSize(_maxs);
    sum += 8+byteSize(_Ms);
    sum += 8+byteSize(_Ss);
    sum += 8+byteSize(_MSEs);
    sum += 8+byteSize(_clss);
    if( _clss != null )         // Have class data at all?
      for( int i=0; i<_clss.length; i++ )
        sum += byteSize(_clss[i]);
    return sum;
  }

  // ------------------------------------------
  // For Classification trees, use class-counts
  //
  // But class-counts are big with a wide dynamic range, we use floats.
  @Override float clss(int bin, int cls) { 
    float fs[] = _clss[bin];
    return fs==null ? 0 : fs[cls];
  }
  // Increment a class-count, inflating the array flavor as needed.
  void add_class( int bin, int cls, float v ) {
    float fs[] = _clss[bin];
    if( fs == null ) _clss[bin]= fs = new float[_nclass];
    fs[cls] += v;
  }
}
