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
  public DBinHistogram( String name, final char nbins, byte isInt, float min, float max, long nelems ) {
    super(name,isInt,min,max);
    assert nelems > 0;
    assert nbins >= 1;
    assert max > min : "Caller ensures "+max+">"+min+", since if max==min== the column "+name+" is all constants";
    // See if we can show there are fewer unique elements than nbins.
    // Common for e.g. boolean columns, or near leaves.
    char xbins = nbins;
    float step;
    if( isInt>0 && max-min <= nbins ) {
      assert ((long)min)==min;
      xbins = (char)((long)max-(long)min+1L); // Shrink bins
      step = 1.0f;                            // Fixed stepsize
    } else {
      step = (max-min)/nbins; // Step size for linear interpolation
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
    h._MSs = MemoryManager.malloc8d(_nbins*2);
    return h;
  }

  // Interpolate d to find bin#
  int bin( float col_data ) {
    if( Float.isNaN(col_data) ) return 0; // Always NAs to bin 0
    assert col_data <= _max : "Coldata out of range "+col_data+" "+this;
    int idx1  = (int)((col_data-_bmin)/_step);
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

  // Compute a "score" for a column; lower score "wins" (is a better split).
  // Score is the sum of the MSEs when the data is split at a single point.
  // mses[1] == MSE for splitting between bins  0  and 1.
  // mses[n] == MSE for splitting between bins n-1 and n.
  // Recursive mean & variance:
  //    http://www.johndcook.com/standard_deviation.html
  //    http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
  DTree.Split scoreMSE( int col ) {
    assert _nbins > 1;

    // Compute mean/var for cumulative bins from 0 to nbins inclusive.
    double MS0[] = new double[2*(_nbins+1)];
    long   ns0[] = new long  [  (_nbins+1)];
    long   tot   = 0;
    for( int b=1; b<=_nbins; b++ ) {
      double m0 = MS0[2*(b-1)+0],  m1 =  _MSs[2*(b-1)+0];
      double s0 = MS0[2*(b-1)+1],  s1 =  _MSs[2*(b-1)+1];
      long   k0 = ns0[   b-1   ],  k1 = _bins[   b-1   ];
      if( k0==0 && k1==0 ) continue;
      double delta=m1-m0;
      MS0[2*b+0] = (k0*m0+k1*m1)/(k0+k1); // Mean
      MS0[2*b+1] = s0+s1+delta*delta*k0*k1/(k0+k1); // 2nd moment
      ns0[  b  ] = k0+k1;
      tot += k1;
    }
    // If we see zero variance, we must have a constant response in this
    // column.  Normally this situation is cut out before we even try to split, but we might
    // have NA's in THIS column... 
    if( MS0[2*_nbins+1] == 0 ) { assert isConstantResponse(); return null; }
    //assert Math.abs(MS0[2*_nbins+1]) > 1e-8 : "No variance, why split? "+Arrays.toString(MS0)+" col "+_name+" "+this;

    // Compute mean/var for cumulative bins from nbins to 0 inclusive.
    double MS1[] = new double[2*(_nbins+1)];
    long   ns1[] = new long  [  (_nbins+1)];
    for( int b=_nbins-1; b>=0; b-- ) {
      double m0 = MS1[2*(b+1)+0],  m1 =  _MSs[2*(b+0)+0];
      double s0 = MS1[2*(b+1)+1],  s1 =  _MSs[2*(b+0)+1];
      long   k0 = ns1[   b+1   ],  k1 = _bins[   b+0   ];
      if( k0==0 && k1==0 ) continue;
      double delta=m1-m0;
      MS1[2*(b-0)+0] = (k0*m0+k1*m1)/(k0+k1); // Mean
      MS1[2*(b-0)+1] = s0+s1+delta*delta*k0*k1/(k0+k1); // 2nd moment
      ns1[  (b-0)  ] = k0+k1;
      assert ns0[b]+ns1[b]==tot;
    }

    // Assert we computed the variance in both directions to some near-equal amount
    double last_var_left_side = MS0[2*_nbins+1];
    double frst_var_rite_side = MS1[2*  0   +1];
    double abs_err = Math.abs(frst_var_rite_side-last_var_left_side);
    double rel_err = abs_err/last_var_left_side;
    assert abs_err < 1e-19 || rel_err < 1e-9 : Arrays.toString(MS0)+":"+Arrays.toString(MS1)+", "+
      Arrays.toString(ns0)+":"+Arrays.toString(ns1)+", var relative error="+rel_err+", var absolute error="+abs_err;

    // Now roll the split-point across the bins.  There are 2 ways to do this:
    // split left/right based on being less than some value, or being equal/
    // not-equal to some value.  Equal/not-equal makes sense for catagoricals
    // but both splits could work for any integral datatype.  Do the less-than
    // splits first.
    int best=0;                         // The no-split
    double best_se0=Double.MAX_VALUE;   // Best squared error
    double best_se1=Double.MAX_VALUE;   // Best squared error
    boolean equal=false;                // Ranged check
    for( int b=1; b<=_nbins-1; b++ ) {
      if( _bins[b] == 0 ) continue;        // Ignore empty splits
      double se = (MS0[2*b+1]+MS1[2*b+1]); // Squared Error (not MSE)
      if( (se < best_se0+best_se1) || // Strictly less error?
          // Or tied MSE, then pick split towards middle bins
          se == (best_se0+best_se1) && best < (_nbins>>1) ) {
        best_se0 = MS0[2*b+1];   best_se1 = MS1[2*b+1]; 
        best = b;
      }
    }

    // If the min==max, we can also try an equality-based split
    if( _isInt > 0 && _step == 1.0f ) { // For any integral (not float) column
      for( int b=1; b<=_nbins-1; b++ ) {
        if( _bins[b] == 0 ) continue;       // Ignore empty splits
        assert _mins[b] == _maxs[b] : "int col, step of 1.0 "+_mins[b]+".."+_maxs[b]+" "+this+" "+Arrays.toString(MS0)+":"+Arrays.toString(ns0);
        double m0 = MS0[2*(b+0)+0],  m1 = MS1[2*(b+1)+0];
        double s0 = MS0[2*(b+0)+1],  s1 = MS1[2*(b+1)+1];
        long   k0 = ns0[   b+0   ],  k1 = ns1[   b+1   ];
        if( k0==0 && k1==0 ) continue;
        double delta=m1-m0;
        double mi = (k0*m0+k1*m1)/(k0+k1); // Mean of included set
        double si = s0+s1+delta*delta*k0*k1/(k0+k1); // 2nd moment of included set
        double sx = _MSs[2*(b+0)+1];                 // The excluded single bin
        if( si+sx < best_se0+best_se1 ) { // Strictly less error?
          best_se0 = si;   best_se1 = sx;
          best = b;        equal = true; // Equality check
        }
      }
    }

    if( best==0 ) return null;  // No place to split
    assert best > 0 : "Must actually pick a split "+best;
    long n0 = !equal ? ns0[best] : ns0[best]+ns1[best+1];
    long n1 = !equal ? ns1[best] : _bins[best]          ;
    return new DTree.Split(col,best,equal,best_se0,best_se1,n0,n1);
  }

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
    int n = 0;
    while( n < _bins.length && _bins[n]==0 ) n++;   // First non-empty bin
    if( n == _bins.length ) return;                 // All bins are empty???
    _min = _mins[n];    // Take min from 1st  non-empty bin
    int x = _bins.length-1;     // Last bin
    while( _bins[x]==0 ) x--;   // Last non-empty bin
    _max = _maxs[x];    // Take max from last non-empty bin
  }

  // An initial set of DBinHistograms (one per column) for this column set
  public static DBinHistogram[] initialHist( Frame fr, int ncols, char nbins ) {
    DBinHistogram hists[] = new DBinHistogram[ncols];
    Vec[] vs = fr.vecs();
    for( int j=0; j<ncols; j++ ) {
      Vec v = vs[j];
      hists[j] = (v.naCnt()==v.length() || v.min()==v.max()) ? null
        : new DBinHistogram(fr._names[j],nbins,(byte)(v.isEnum() ? 2 : (v.isInt()?1:0)),(float)v.min(),(float)v.max(),v.length());
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

  // Check for a constant response variable
  public boolean isConstantResponse() {
    double m = Double.NaN;
    for( int b=0; b<_nbins; b++ ) {
      if( _bins[b] == 0 ) continue;
      if( _MSs[2*b+1] != 0 ) return false;
      if( _MSs[2*b+0] != m ) 
        if( Double.isNaN(m) ) m=_MSs[2*b+0]; 
        else return false;
    }
    return true;
  }

  // Pretty-print a histogram
  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(_name).append(":").append(_min).append("-").append(_max+", bmin="+_bmin+" step="+_step+" nbins="+(int)_nbins);
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
    sum += 4+4+2+2;/*step,bmin,nbins,pad*/
    sum += 8+byteSize(_bins);
    sum += 8+byteSize(_mins);
    sum += 8+byteSize(_maxs);
    sum += 8+byteSize(_MSs);
    return sum;
  }
}
