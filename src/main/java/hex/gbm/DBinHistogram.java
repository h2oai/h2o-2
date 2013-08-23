package hex.gbm;

import java.util.Arrays;
import water.*;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log;

/**
   A Histogram, computed in parallel over a Vec.
   <p>
   A {@code DBinHistogram} bins (by default into {@value BINS} bins) every
   value added to it, and computes a the vec min & max (for use in the next
   split), and response-Vec mean & variance for each bin.  {@code DBinHistogram}s
   are initialized with a min, max and number-of-elements to be added (all of
   which are generally available from a Vec).  Bins run from min to max in
   uniform sizes.  If the {@code DBinHistogram} can determine that fewer bins
   are needed (e.g. boolean columns run from 0 to 1, but only ever take on 2
   values, so only 2 bins are needed), then fewer than {@value BINS} bins are
   used.
   <p>
   If we are successively splitting rows (e.g. in a decision tree), then a
   fresh {@code DBinHistogram} for each split will dynamically re-bin the data.
   Each successive split then, will logarithmically divide the data.  At the
   first split, outliers will end up in their own bins - but perhaps some
   central bins may be very full.  At the next split(s), the full bins will get
   split, and again until (with a log number of splits) each bin holds roughly
   the same amount of data.  This dynamic binning resolves a lot of problems
   with picking the proper bin count or limits - generally a few more tree
   levels will be the equal any fancy but fixed-size binning strategy.
   <p>
   If the response-Vec is actually a collection of Vecs (i.e., a vector) - as
   is the case when building a classification tree - then the Histogram
   actually collects the vector-mean & variance.
   <p>
   @author Cliff Click
*/
public class DBinHistogram extends DHistogram<DBinHistogram> {
  public final float   _step;        // Linear interpolation step per bin
  public final float   _bmin;        // Linear interpolation min  per bin
  public final char    _nbins;       // Number of bins
  public final short   _nclass;      // Number of classes
  public       long [] _bins;        // Number of rows in each bin
  public       float[] _mins, _maxs; // Min, Max, per-bin
  // Average response-vector for the rows in this split.
  // For RF, this will be 1.0 for the response variable and zero otherwise.
  // For GBM, these are the residuals by class.  
  // For Regression trees, there is but one "class".
  // For Classification trees, there can be many classes.
  // At points during data gather, this data is the sum of responses instead of
  // the mean, but it gets divided before numerical overflow issues arise.
  private      float[/*bin*/][/*class*/] _Ms; // Mean response, per-bin per-class
  private      float[/*bin*/][/*class*/] _Ss; // Variance, per-bin per-class

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
    float step = (max-min)/_nbins; // Step size for linear interpolation
    if( step == 0 ) { assert max==min; step = 1.0f; }
    assert step > 0;
    _step = step;
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
    Arrays.fill(h._mins, Float.MAX_VALUE);
    Arrays.fill(h._maxs,-Float.MAX_VALUE);
    h._Ms = new float[_nbins][];
    h._Ss = new float[_nbins][];
    return h;
  }

  // Interpolate d to find bin#
  int bin( float d ) {
    int idx1  = (int)((d-_bmin)/_step);
    int idx2  = Math.max(Math.min(idx1,_nbins-1),0); // saturate at bounds
    return idx2;
  }

  @Override int  nbins(     ) { return _nbins  ; }
  @Override long  bins(int b) { return _bins[b]; }
  @Override float mins(int b) { return _mins[b]; }
  @Override float maxs(int b) { return _maxs[b]; }
  float mean(int b, int cls) { 
    if( _Ms[b] == null ) return 0;
    return _Ms[b][cls]; 
  }
  float var (int b, int cls) { 
    return _bins[b] > 1 ? _Ss[b][cls]/(_bins[b]-1) : 0; 
  }
  // Mean of response-vector.  Since vector values are already normalized we
  // just average the vector contents.
  float mean(int b) { 
    if( _Ms[b] == null ) return 0;
    float sum=0;
    for( int c=0; c<_nclass; c++ )
      sum += _Ms[b][c];
    return sum/_nclass;
  }

  // Variance of response-vector.  Sum of variances of the vector elements.
  float var( int b ) {
    float sum=0;
    for( int i=0; i<_nclass; i++ )
      sum += var(b,i);
    return sum;
  }

  // Compute a "score" for a column; lower score "wins" (is a better split).
  // Score is the sum of variances.  Another good "score" is Mean Squared
  // Error, but this requires another pass.
  float score( ) {
    float sum = 0;
    for( int i=0; i<_bins.length; i++ )
      sum += var(i)*_bins[i];
    return sum;
  }

  // Add one row to a bin found via simple linear interpolation.
  // Compute bin min/max.
  // Compute response-vector mean & variance.
  // Response-vector is specified as _nclass values in Chunks, from ychk to
  // ychk+_nclass-1.
  void incr( int row, float d, Chunk[] chks, int ychk ) {
    int b = bin(d);             // Compute bin# via linear interpolation
    // Lazily allocate storage the first time a bin recieves any counts.
    if( _Ms[b] == null ) {
      _Ms[b] = MemoryManager.malloc4f(_nclass);
      _Ss[b] = MemoryManager.malloc4f(_nclass);
    }
    _bins[b]++;                 // Bump count in bin
    long k = _bins[b];          // Row count for bin b
    // Track actual lower/upper bound per-bin
    if( d < _mins[b] ) _mins[b] = d;
    if( d > _maxs[b] ) _maxs[b] = d;
    // Recursive mean & variance of response vector
    //    http://www.johndcook.com/standard_deviation.html
    for( int c=0; c<_nclass; c++ ) {
      float y = (float)chks[ychk+c].at0(row);
      float oldM = _Ms[b][c];   // Old mean
      float newM = _Ms[b][c] = oldM + (y-oldM)/k;
      _Ss[b][c] += (y-oldM)*(y-newM);
    }
  }


  // After having filled in histogram bins, compute tighter min/max bounds.
  @Override public void tightenMinMax() {
    int n = 0;
    while( _bins[n]==0 ) n++;   // First non-empty bin
    _min = _mins[n];    // Take min from 1st  non-empty bin
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
  public DBinHistogram[] split( int col, int b, DHistogram hs[], String[] names, int ncols ) {
    assert hs[col] == this;
    if( _bins[b] <= 1 ) return null; // Zero or 1 elements
    if( var(b) == 0.0 ) return null; // No point in splitting a perfect prediction

    // Build a next-gen split point from the splitting bin
    int cnt=0;                  // Count of possible splits
    DBinHistogram nhists[] = new DBinHistogram[ncols]; // A new histogram set
    for( int j=0; j<ncols; j++ ) { // For every column in the new split
      DHistogram h = hs[j];        // Old histogram of column
      if( h == null ) continue;    // Column was not being tracked?
      // min & max come from the original column data, since splitting on an
      // unrelated column will not change the j'th columns min/max.
      float min = h._min, max = h._max;
      // Tighter bounds on the column getting split: exactly each new
      // DBinHistogram's bound are the bins' min & max.
      if( col==j ) { min=h.mins(b); max=h.maxs(b); }
      if( min == max ) continue; // This column will not split again
      nhists[j] = new DBinHistogram(names[j],_nclass,hs[j]._isInt,min,max,_bins[b]);
      cnt++;                    // At least some chance of splitting
    }
    return cnt == 0 ? null : nhists;
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
    for( int b=0; b<_bins.length; b++ ) {
      long k1 = _bins[b], k2 = h._bins[b];
      if( k1==0 && k2==0 ) continue;
      _bins[b]=k1+k2;
      if( h._mins[b] < _mins[b] ) _mins[b] = h._mins[b];
      if( h._maxs[b] > _maxs[b] ) _maxs[b] = h._maxs[b];
      // Recursive mean & variance
      //    http://www.johndcook.com/standard_deviation.html
      //    http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
      for( int c = 0; c<_nclass; c++ ) {
        assert _Ms[b] != null || k1 == 0;
        if( h._Ms[b] == null ) { assert k2 == 0; }
        else if( _Ms[b] == null ) { assert k1==0;  _Ms[b] = h._Ms[b];  _Ss[b] = h._Ss[b]; }
        else {
          float m1 = _Ms[b][c],  m2 = h._Ms[b][c];
          float s1 = _Ss[b][c],  s2 = h._Ss[b][c];
          float delta=m2-m1;
          _Ms[b][c] = (k1*m1+k2*m2)/(k1+k2); // Mean
          _Ss[b][c] = s1+s2+delta*delta*k1*k2/(k1+k2); // 2nd moment
        }
      }
    }
  }

  // Pretty-print a histogram
  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(_name).append(", ").append(_min).append("-").append(_max).append("\n");
    if( _bins != null ) for( int b=0; b<_nbins; b++ ) {
        sb.append(String.format("cnt=%d, min=%f, max=%f, mean/var=", _bins[b],_mins[b],_maxs[b]));
        for( int c=0; c<_nclass; c++ )
          sb.append(String.format(" %d - %6.2f/%6.2f,", c,
                                  _Ms[b]==null?Float.NaN:mean(b,c),
                                  _Ss[b]==null?Float.NaN:var (b,c)));
        sb.append('\n');
      }
    return sb.toString();
  }

  @Override long byteSize() {
    long sum = super.byteSize();
    sum += 8+byteSize(_bins);
    sum += 8+byteSize(_mins);
    sum += 8+byteSize(_maxs);
    sum += 8+byteSize(_Ms);
    if( _Ms != null )           // Have class data at all?
      for( int i=0; i<_Ms.length; i++ )
        sum += byteSize(_Ms[i]);
    if( _Ss != null )           // Have class data at all?
      for( int i=0; i<_Ss.length; i++ )
        sum += byteSize(_Ss[i]);
    return sum;
  }
}
