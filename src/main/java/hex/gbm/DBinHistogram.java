package hex.gbm;

import java.util.Arrays;
import water.*;
import water.fvec.*;
import water.util.Log;

/**
   A Histogram, computed in parallel over a Vec.
   <p>
   A {@code DBinHistogram} bins every value added to it, and computes a the vec
   min & max (for use in the next split), and response-Vec mean & variance for
   each bin.  {@code DBinHistogram}s are initialized with a min, max and
   number-of-elements to be added (all of which are generally available from a
   Vec).  Bins run from min to max in uniform sizes.  If the {@code
   DBinHistogram} can determine that fewer bins are needed (e.g. boolean
   columns run from 0 to 1, but only ever take on 2 values, so only 2 bins are
   needed), then fewer bins are used.
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
  public final char    _nclass;      // Number of classes
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
  public DBinHistogram( String name, final char nbins, char nclass, boolean isInt, float min, float max, long nelems ) {
    super(name,isInt,min,max);
    assert nelems > 0;
    assert max > min : "Caller ensures "+max+">"+min+", since if max==min== the column "+name+" is all constants";
    char xbins = (char)Math.max((char)Math.min(nbins,nelems),1); // Default bin count
    // See if we can show there are fewer unique elements than nbins.
    // Common for e.g. boolean columns, or near leaves.
    float step;
    if( isInt && max-min < xbins ) {
      xbins = (char)((long)max-(long)min+1L); // Shrink bins
      step = 1.0f;                            // Fixed stepsize
    } else {
      step = (max-min)/xbins; // Step size for linear interpolation
      if( step == 0 ) { assert max==min; step = 1.0f; }
      assert step > 0;
    }
    _nbins = xbins;
    _nclass = nclass;
    _bmin = min;                // Bin-Min
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
  float binAt( int b ) { return _bmin+b*_step; }

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

  // Mean Squared Error: sum(X^2)-Mean^2*n = Var + Mean^2*n*(n-1)
  // But predicting the Mean, so MSE is Var.
  double mse   ( int b, int c ) { return _Ss[b] == null ? 0 : _Ss[b][c]/_bins[b]; }
  double mseVar( double mean, double var, long n ) { return n==0 ? 0 : var*(n-1)/n; }
  double mseSQ ( double sum , double ssq, long n ) { return ssq - sum*sum/n; }

  float mse( float Ms[], float Ss[], long n ) {
    float sum=0;
    for( int i=0; i<_nclass; i++ )
      if( n > 0 ) sum += Ss[i]/n;
    return sum;
  }


  // Variance of response-vector.  Sum of variances of the vector elements.
  float var( int b ) {
    float sum=0;
    for( int i=0; i<_nclass; i++ )
      sum += var(b,i);
    return sum;
  }

  // MSE of response-vector.  Sum of MSE of the vector elements.
  float mse( int b ) {
    float sum=0;
    for( int c=0; c<_nclass; c++ )
      sum += mse(b,c);
    return sum;
  }

  // Compute a "score" for a column; lower score "wins" (is a better split).
  // Score is the sum of MSE.
  float score( ) {
    float sum = 0;
    for( int i=0; i<_bins.length; i++ )
      sum += mse(i);
    return sum;
  }

  // Compute a "score" for a column; lower score "wins" (is a better split).
  // Score is the sum of the MSEs when the data is split at a single point.
  // mses[1] == MSE for splitting between bins  0  and 1.
  // mses[n] == MSE for splitting between bins n-1 and n.
  // Returns index of smallest MSE, ranges from 1 to nbins-1.
  //
  // Mean-Squared-Error of a prediction vector: the error is the Euclidean
  // distance (square-root of sum of distances squared), so the MSE is the mean
  // of the sum of distances-squared.  This is almost exactly the variance.  We
  // compute the squared error per class and sum them.
  DTree.Split scoreMSE( int col, String name ) {
    assert _nbins > 1;

    // Split zero bins to the left, all bins to the right
    // Left stack of bins
    float M0[] = new float[_nclass], S0[] = new float[_nclass];
    long n0 = 0;
    // Right stack of bins
    float M1[] = new float[_nclass], S1[] = new float[_nclass];
    long n1 = 0;
    for( int b=0; b<_nbins; b++ )
      n1 = add(M1,S1,n1,_Ms[b],_Ss[b],_bins[b]);

    // Make private hackable copies
    float M2[] = M1.clone();
    float S2[] = S1.clone();

    // Now roll the split-point across the bins.  There are 2 ways to do this:
    // split left/right based on being less than some value, or being equal/
    // not-equal to some value.  Equal/not-equal makes sense for catagoricals
    // but both splits could work for any integral datatype.  Do the less-than
    // splits first.
    double mseAll=0;
    assert (mseAll = mse(M1,S1,n1))==mseAll || true;
    DTree.Split best = new DTree.Split(col,-1,false,0L,0L,Double.MAX_VALUE,Double.MAX_VALUE,null,null);
    for( int b=0; b<_nbins; b++ ) {
      double mse0 = mse(M0,S0,n0);
      double mse1 = mse(M1,S1,n1);
      double mse = (mse0*n0+mse1*n1)/(n0+n1);
      if( mse < best.mse() || (best._bin<((_nbins+1)/2) && mse==best.mse()) )
        best = new DTree.Split(col,b,false,n0,n1,mse0,mse1,M0.clone(), M1.clone());
      // Move mean/var across split point
      n0 = add( M0, S0, n0, _Ms[b], _Ss[b], _bins[b]);
      n1 = sub( M1, S1, n1, _Ms[b], _Ss[b], _bins[b]);
    }
    // MSE for the final "split" should equal the first "split" - as both are
    // non-splits: either ALL data to the left or ALL data to the right.
    assert n1==0;
    assert mseAll == mse(M0,S0,n0);

    // Now look at equal/not-equal splits.  At each loop, remove the current
    // bin from M2/S2/n2 & check MSE - then restore M2/S2/n2.
    if( _isInt && _step == 1.0f ) { // Only for ints & enums
      long n2 = n0;
      for( int b=1; b<_nbins-1; b++ ) { // Notice tigher endpoints: ignore splits that are repeats of above
        long n3 = _bins[b];
        if( n3 == 0 ) continue; // Ignore zero-bin splits
        if( n3 == n2 ) {        // Bad split: all or nothing.
          best = new DTree.Split(col,b,true,0,n3,0,mseAll,new float[M2.length], M2);
          break;
        }
        // Subtract out the chosen bin from the totals
        float M3[] = _Ms[b], S3[] = _Ss[b];
        n2 = sub(M2,S2,n2,M3,S3,n3);
        double mse2 = mse(M2,S2,n2);
        double mse3 = mse(M3,S3,n3);
        double mse = (mse2*n2+mse3*n3)/(n2+n3);
        if( mse < best.mse() )
          best = new DTree.Split(col,b,true,n2,n3,mse2,mse3,M2.clone(), M3.clone());
        // Restore the total bins
        n2 = add(M2,S2,n2,M3,S3,n3);
        assert Math.abs(mseAll - mse(M2,S2,n2)) < 0.00001 : "mseAll="+mseAll+", mse at end="+mse(M2,S2,n2)+", bin="+b+", "+this;
      }
    }

    return best;
  }

  // Add one row to a bin found via simple linear interpolation.
  // Compute bin min/max.
  // Compute response-vector mean & variance.
  // Response-vector is specified as _nclass values in Chunks, from ychk to
  // ychk+_nclass-1.
  void incr( int row, float d, Chunk[] chks, int ychk ) {
    int b = bin(d);             // Compute bin# via linear interpolation
    // Lazily allocate storage the first time a bin recieves any counts.
    float Ms[] = _Ms[b];
    float Ss[] = _Ss[b];
    if( Ms == null ) {
      _Ms[b] = Ms = MemoryManager.malloc4f(_nclass);
      _Ss[b] = Ss = MemoryManager.malloc4f(_nclass);
    }
    _bins[b]++;                 // Bump count in bin
    long k = _bins[b];          // Row count for bin b
    // Track actual lower/upper bound per-bin
    if( d < _mins[b] ) _mins[b] = d;
    if( d > _maxs[b] ) _maxs[b] = d;
    // Recursive mean & variance of response vector
    //    http://www.johndcook.com/standard_deviation.html
    for( int c=0; c<_nclass; c++ ) {
      Chunk chk = chks[ychk+c];
      float y;
      if( chk instanceof C4FChunk ) { // Help inline common case
        y = (float)((C4FChunk)chk).at0(row);
      } else {
        y = (float)chk.at0(row);
      }
      float oldM = Ms[c];   // Old mean
      float newM = Ms[c] = oldM + (y-oldM)/k;
      Ss[c] += (y-oldM)*(y-newM);
    }
  }


  // After having filled in histogram bins, compute tighter min/max bounds.
  @Override public void tightenMinMax() {
    int n = 0;
    while( n < _bins.length && _bins[n]==0 ) n++;   // First non-empty bin
    if( n == _bins.length ) return;                 // All bins are empty???
    _min = _mins[n];    // Take min from 1st  non-empty bin
    int x = _bins.length-1;     // Last bin
    while( _bins[x]==0 ) x--;   // Last non-empty bin
    _max = _maxs[x];    // Take max from last non-empty bin
  }

  // An initial set of DBinHistograms (one per column) for this column set
  public static DBinHistogram[] initialHist( Frame fr, int ncols, char nbins, char nclass ) {
    DBinHistogram hists[] = new DBinHistogram[ncols];
    Vec[] vs = fr._vecs;
    for( int j=0; j<ncols; j++ ) {
      Vec v = vs[j];
      hists[j] = v.min()==v.max() ? null
        : new DBinHistogram(fr._names[j],nbins,nclass,v.isInt(),(float)v.min(),(float)v.max(),v.length());
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
      assert _Ms[b] != null || k1 == 0;
      if( h._Ms[b] == null ) { assert k2 == 0; }
      else if( _Ms[b] == null ) { assert k1==0;  _Ms[b] = h._Ms[b];  _Ss[b] = h._Ss[b]; }
      add(_Ms[b],_Ss[b],k1,h._Ms[b],h._Ss[b],k2);
    }
  }

  // Recursive mean & variance
  //    http://www.johndcook.com/standard_deviation.html
  //    http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
  long add( float Ms0[], float Ss0[], long n0, float Ms1[], float Ss1[], long n1 ) {
    if( Ms1 != null ) 
      for( int c = 0; c<_nclass; c++ ) {
        float m0 = Ms0[c],  m1 = Ms1[c];
        float s0 = Ss0[c],  s1 = Ss1[c];
        float delta=m1-m0;
        Ms0[c] = (n0*m0+n1*m1)/(n0+n1); // Mean
        Ss0[c] = s0+s1+delta*delta*n0*n1/(n0+n1); // 2nd moment
      }
    return n0+n1;
  }
  long sub( float Ms0[], float Ss0[], long n0, float Ms1[], float Ss1[], long n1 ) {
    if( Ms1 != null ) 
      for( int c = 0; c<_nclass; c++ ) {
        float m0 = Ms0[c],  m1 = Ms1[c];
        float s0 = Ss0[c],  s1 = Ss1[c];
        float delta=m1-m0;
        Ms0[c] = (n0*m0-n1*m1)/(n0-n1); // Mean
        Ss0[c] = s0-s1-delta*delta*n0*n1/(n0-n1); // 2nd moment
      }
    return n0-n1;
  }

  // Pretty-print a histogram
  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(_name).append(":").append(_min).append("-").append(_max);
    if( _bins != null ) {
      for( int b=0; b<_nbins; b++ ) {
        sb.append(String.format("\ncnt=%d, min=%f, max=%f, mean/var=", _bins[b],_mins[b],_maxs[b]));
        for( int c=0; c<_nclass; c++ )
          sb.append(String.format(" %d - %6.2f/%6.2f,", c,
                                  _Ms[b]==null?Float.NaN:mean(b,c),
                                  _Ss[b]==null?Float.NaN:var (b,c)));
      }
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
