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

  // Compute a "score" for a column; lower score "wins" (is a better split).
  // Score is the sum of the MSEs when the data is split at a single point.
  // mses[1] == MSE for splitting between bins  0  and 1.
  // mses[n] == MSE for splitting between bins n-1 and n.
  // Recursive mean & variance:
  //    http://www.johndcook.com/standard_deviation.html
  //    http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
  DTree.Split scoreMSE( int col, String name ) {
    assert _nbins > 1;

    // Compute mean/var for cumulative bins from 0 to nbins inclusive.
    double MS0[] = new double[2*(_nbins+1)];
    long   ns0[] = new long  [  (_nbins+1)];
    long   tot   = 0;
    for( int b=1; b<=_nbins; b++ ) {
      double m0 = MS0[2*(b-1)+0],  m1 =  _MSs[2*(b-1)+0];
      double s0 = MS0[2*(b-1)+1],  s1 =  _MSs[2*(b-1)+1];
      long   k0 = ns0[   b-1   ],  k1 = _bins[   b-1   ];
      double delta=m1-m0;
      MS0[2*b+0] = (k0*m0+k1*m1)/(k0+k1); // Mean
      MS0[2*b+1] = s0+s1+delta*delta*k0*k1/(k0+k1); // 2nd moment
      ns0[  b  ] = k0+k1;
      tot += k1;
    }

    // Compute mean/var for cumulative bins from nbins to 0 inclusive.
    double MS1[] = new double[2*(_nbins+1)];
    long   ns1[] = new long  [  (_nbins+1)];
    for( int b=_nbins-1; b>=0; b-- ) {
      double m0 = MS1[2*(b+1)+0],  m1 =  _MSs[2*(b+0)+0];
      double s0 = MS1[2*(b+1)+1],  s1 =  _MSs[2*(b+0)+1];
      long   k0 = ns1[   b+1   ],  k1 = _bins[   b+0   ];
      double delta=m1-m0;
      MS1[2*(b-0)+0] = (k0*m0+k1*m1)/(k0+k1); // Mean
      MS1[2*(b-0)+1] = s0+s1+delta*delta*k0*k1/(k0+k1); // 2nd moment
      ns1[  (b-0)  ] = k0+k1;
      assert ns0[b]+ns1[b]==tot;
    }

    // Now roll the split-point across the bins.  There are 2 ways to do this:
    // split left/right based on being less than some value, or being equal/
    // not-equal to some value.  Equal/not-equal makes sense for catagoricals
    // but both splits could work for any integral datatype.  Do the less-than
    // splits first.
    assert Math.abs(MS0[2*_nbins+1]-MS1[2*0+1]) < 1e-8; // Endpoints have same Var
    int best=0;                         // The no-split
    double best_se=Double.MAX_VALUE;    // Best squared error
    for( int b=1; b<=_nbins-1; b++ ) {
      double se = (MS0[2*b+1]+MS1[2*b+1]); // Squared Error (not MSE)
      if( (se < best_se) || // Strictly less error?
          // Or tied MSE, then pick split towards middle bins
          best_se == se && best < (_nbins>>1) ) {
        best_se = se; best = b;
      }
    }
    assert best > 0 : "Must actually pick a split "+best;
    return new DTree.Split(col,best,false,best_se,ns0[best],ns1[best]);
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
      if( v.isEnum() ) throw H2O.unimpl(); // wrong scoreMSE
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
