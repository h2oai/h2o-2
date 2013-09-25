package hex.gbm;

import java.util.Arrays;
import water.*;
import water.fvec.*;
import water.util.Log;

/**
   A Histogram, computed in parallel over a Vec.
   <p>
   A {@code DBinHistogram} bins every value added to it, and computes a the vec
   min & max (for use in the next split), and response mean & variance for each
   bin.  {@code DBinHistogram}s are initialized with a min, max and number-of-
   elements to be added (all of which are generally available from a Vec).
   Bins run from min to max in uniform sizes.  If the {@code DBinHistogram} can
   determine that fewer bins are needed (e.g. boolean columns run from 0 to 1,
   but only ever take on 2 values, so only 2 bins are needed), then fewer bins
   are used.
   <p>
   If we are successively splitting rows (e.g. in a decision tree), then a
   fresh {@code DBinHistogram} for each split will dynamically re-bin the data.
   Each successive split will logarithmically divide the data.  At the first
   split, outliers will end up in their own bins - but perhaps some central
   bins may be very full.  At the next split(s), the full bins will get split,
   and again until (with a log number of splits) each bin holds roughly the
   same amount of data.  This dynamic binning resolves a lot of problems with
   picking the proper bin count or limits - generally a few more tree levels
   will be the equal any fancy but fixed-size binning strategy.
   <p>
   @author Cliff Click
*/
public class DBinHistogram extends DHistogram<DBinHistogram> {
  public final float   _step;        // Linear interpolation step per bin
  public final float   _bmin;        // Linear interpolation min  per bin
  public final char    _nbins;       // Number of bins
  public       long [] _bins;        // Number of rows in each bin
  public       float[] _mins, _maxs; // Min, Max, per-bin
  private      double[] _MSs;  // Mean response & 2nd moment, per-bin per-class

  // Fill in read-only sharable values
  public DBinHistogram( String name, final char nbins, boolean isInt, float min, float max, long nelems ) {
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
    _bmin = min;                // Bin-Min
    _step = step;
  }

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
    h._MSs = new double[_nbins*2];
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
  double mean(int b) { return _MSs[b*2+0]; }
  double seco(int b) { return _MSs[b*2+1]; }
  double var (int b) { return _bins[b] > 1 ? seco(b)/(_bins[b]-1) : 0; }
  
  //// Mean Squared Error: sum(X^2)-Mean^2*n = Var + Mean^2*n*(n-1)
  //// But predicting the Mean, so MSE is Var.
  //double mse   ( int b, int c ) { return _Ss[b] == null ? 0 : _Ss[b][c]/_bins[b]; }
  //double mseVar( double mean, double var, long n ) { return n==0 ? 0 : var*(n-1)/n; }
  //double mseSQ ( double sum , double ssq, long n ) { return ssq - sum*sum/n; }
  //
  //double mse( float Ms[], float Ss[], long n ) {
  //  double sum=0;
  //  for( int i=0; i<_nclass; i++ )
  //    if( n > 0 ) sum += (double)Ss[i]/n;
  //  return sum;
  //}
  //double mse( double Ms[], double Ss[], long n ) {
  //  double sum=0;
  //  for( int i=0; i<_nclass; i++ )
  //    if( n > 0 ) sum += Ss[i]/n;
  //  return sum;
  //}
  //
  //
  //// Variance of response-vector.  Sum of variances of the vector elements.
  //float var( int b ) {
  //  float sum=0;
  //  for( int i=0; i<_nclass; i++ )
  //    sum += var(b,i);
  //  return sum;
  //}
  //
  //// MSE of response-vector.  Sum of MSE of the vector elements.
  //float mse( int b ) {
  //  float sum=0;
  //  for( int c=0; c<_nclass; c++ )
  //    sum += mse(b,c);
  //  return sum;
  //}
  //
  //// Compute a "score" for a column; lower score "wins" (is a better split).
  //// Score is the sum of MSE.
  //float score( ) {
  //  float sum = 0;
  //  for( int i=0; i<_bins.length; i++ )
  //    sum += mse(i);
  //  return sum;
  //}

  // Compute a "score" for a column; lower score "wins" (is a better split).
  // Score is the sum of the MSEs when the data is split at a single point.
  // mses[1] == MSE for splitting between bins  0  and 1.
  // mses[n] == MSE for splitting between bins n-1 and n.
  DTree.Split scoreMSE( int col, String name ) {
    assert _nbins > 1;
    throw H2O.unimpl();

    //// Split zero bins to the left, all bins to the right
    //// Left stack of bins
    //double M0[] = new double[_nclass], S0[] = new double[_nclass];
    //long n0 = 0;
    //// Right stack of bins
    //double M1[] = new double[_nclass], S1[] = new double[_nclass];
    //long n1 = 0;
    //for( int b=0; b<_nbins; b++ )
    //  n1 = add(M1,S1,n1,_Ms[b],_Ss[b],_bins[b]);
    //
    //// Make private hackable copies
    //double M2[] = M1.clone();
    //double S2[] = S1.clone();
    //
    //// Now roll the split-point across the bins.  There are 2 ways to do this:
    //// split left/right based on being less than some value, or being equal/
    //// not-equal to some value.  Equal/not-equal makes sense for catagoricals
    //// but both splits could work for any integral datatype.  Do the less-than
    //// splits first.
    //double mseAll=0;
    //assert (mseAll = mse(M1,S1,n1))==mseAll || true;
    //DTree.Split best = DTree.Split.make(col,-1,false,0L,0L,Double.MAX_VALUE,Double.MAX_VALUE,(float[])null,null);
    //for( int b=0; b<_nbins; b++ ) {
    //  double mse0 = mse(M0,S0,n0);
    //  double mse1 = mse(M1,S1,n1);
    //  double mse = (mse0*n0+mse1*n1)/(n0+n1);
    //  if( mse < best.mse() || (best._bin<((_nbins+1)/2) && mse==best.mse()) )
    //    best = DTree.Split.make(col,b,false,n0,n1,mse0,mse1,M0,M1);
    //  // Move mean/var across split point
    //  n0 = add( M0, S0, n0, _Ms[b], _Ss[b], _bins[b]);
    //  n1 = sub( M1, S1, n1, _Ms[b], _Ss[b], _bins[b]);
    //}
    //// MSE for the final "split" should equal the first "split" - as both are
    //// non-splits: either ALL data to the left or ALL data to the right.
    //assert n1==0;
    //assert mseAll == mse(M0,S0,n0);
    //
    //// Now look at equal/not-equal splits.  At each loop, remove the current
    //// bin from M2/S2/n2 & check MSE - then restore M2/S2/n2.
    //double M2orig[] = M2.clone();
    //double S2orig[] = S2.clone();
    //if( _isInt && _step == 1.0f ) { // Only for ints & enums
    //  long n2 = n0;
    //  for( int b=1; b<_nbins-1; b++ ) { // Notice tigher endpoints: ignore splits that are repeats of above
    //    long n3 = _bins[b];
    //    if( n3 == 0 ) continue; // Ignore zero-bin splits
    //    if( n3 == n2 ) {        // Bad split: all or nothing.
    //      best = DTree.Split.make(col,b,true,0L,n3,0.0,mseAll,null, M2);
    //      break;
    //    }
    //    // Subtract out the chosen bin from the totals
    //    n2 = sub(M2,S2,n2,_Ms[b],_Ss[b],n3);
    //    double mse2 = mse( M2   , S2   ,n2);
    //    double mse3 = mse(_Ms[b],_Ss[b],n3);
    //    double mse = (mse2*n2+mse3*n3)/(n2+n3);
    //    if( mse < best.mse() )
    //      best = DTree.Split.make(col,b,true,n2,n3,mse2,mse3,M2, _Ms[b]);
    //    // Restore M2/S2/n2
    //    n2 += n3;
    //    System.arraycopy(M2orig,0,M2,0,M2.length);
    //    System.arraycopy(S2orig,0,S2,0,S2.length);
    //    assert Math.abs(mseAll - mse(M2,S2,n2)) < 0.00001 : "mseAll="+mseAll+", mse at end="+mse(M2,S2,n2)+", bin="+b+", "+this;
    //  }
    //}
    //assert best._bin > 0 : "Must actually pick a split "+best;
    //return best;
  }

  // Recursive mean & variance
  //    http://www.johndcook.com/standard_deviation.html
  //    http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
  //long add( double Ms0[], double Ss0[], long n0, float Ms1[], float Ss1[], long n1 ) {
  //  if( Ms1 != null ) 
  //    for( int c = 0; c<_nclass; c++ ) {
  //      double m0 = Ms0[c],  m1 = Ms1[c];
  //      double s0 = Ss0[c],  s1 = Ss1[c];
  //      double delta=m1-m0;
  //      Ms0[c] = (n0*m0+n1*m1)/(n0+n1); // Mean
  //      Ss0[c] = s0+s1+delta*delta*n0*n1/(n0+n1); // 2nd moment
  //    }
  //  return n0+n1;
  //}
  //long sub( double Ms0[], double Ss0[], long n0, float Ms1[], float Ss1[], long n1 ) {
  //  if( Ms1 != null ) 
  //    for( int c = 0; c<_nclass; c++ ) {
  //      double m0 = Ms0[c],  m1 = Ms1[c];
  //      double s0 = Ss0[c],  s1 = Ss1[c];
  //      double delta=m1-m0;
  //      Ms0[c] = (n0*m0-n1*m1)/(n0-n1); // Mean
  //      Ss0[c] = s0-s1-delta*delta*n0*n1/(n0-n1); // 2nd moment
  //    }
  //  return n0-n1;
  //}

  // Add one row to a bin found via simple linear interpolation.
  // Compute bin min/max.
  // Compute response mean & variance.
  void incr( int row, float col_data, double y ) {
    int b = bin(col_data);      // Compute bin# via linear interpolation
    _bins[b]++;                 // Bump count in bin
    long k = _bins[b];          // Row count for bin b
    // Track actual lower/upper bound per-bin
    if( col_data < _mins[b] ) _mins[b] = col_data;
    if( col_data > _maxs[b] ) _maxs[b] = col_data;
    // Recursive mean & variance of response vector
    //    http://www.johndcook.com/standard_deviation.html
    double oldM = _MSs[2*b+0];  // Old mean
    double newM = _MSs[2*b+0] = oldM + (y-oldM)/k;
    _MSs[2*b+1] += (y-oldM)*(y-newM);
  }


  // After having filled in histogram bins, compute tighter min/max bounds.
  @Override public void tightenMinMax() {
    throw H2O.unimpl();
    //int n = 0;
    //while( n < _bins.length && _bins[n]==0 ) n++;   // First non-empty bin
    //if( n == _bins.length ) return;                 // All bins are empty???
    //_min = _mins[n];    // Take min from 1st  non-empty bin
    //int x = _bins.length-1;     // Last bin
    //while( _bins[x]==0 ) x--;   // Last non-empty bin
    //_max = _maxs[x];    // Take max from last non-empty bin
  }

  // An initial set of DBinHistograms (one per column) for this column set
  public static DBinHistogram[] initialHist( Frame fr, int ncols, char nbins ) {
    DBinHistogram hists[] = new DBinHistogram[ncols];
    Vec[] vs = fr.vecs();
    for( int j=0; j<ncols; j++ ) {
      Vec v = vs[j];
      hists[j] = (v.naCnt()==v.length() || v.min()==v.max()) ? null
        : new DBinHistogram(fr._names[j],nbins,v.isInt(),(float)v.min(),(float)v.max(),v.length());
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
      double m0 = _MSs[2*b+0],  m1 = h._MSs[2*b+0];
      double s0 = _MSs[2*b+1],  s1 = h._MSs[2*b+1];
      double delta=m1-m0;
      _MSs[2*b+0] = (k1*m0+k2*m1)/(k1+k2); // Mean
      _MSs[2*b+1] = s0+s1+delta*delta*k1*k2/(k1+k2); // 2nd moment
    }
  }

  // Pretty-print a histogram
  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(_name).append(":").append(_min).append("-").append(_max);
    if( _bins != null ) {
      for( int b=0; b<_nbins; b++ ) {
        sb.append(String.format("\ncnt=%d, min=%f, max=%f, mean/var=", _bins[b],_mins[b],_maxs[b]));
        sb.append(String.format("%6.2f/%6.2f,", mean(b), var(b)));
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
    sum += 8+byteSize(_MSs);
    return sum;
  }
}
