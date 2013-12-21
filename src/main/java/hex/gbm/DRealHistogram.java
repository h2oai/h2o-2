package hex.gbm;

import java.util.Arrays;
import java.util.concurrent.atomic.*;
import sun.misc.Unsafe;
import water.*;
import water.nbhm.UtilUnsafe;
import water.util.SB;
import water.util.Utils;
import water.fvec.Frame;
import water.fvec.Vec;

/**
   A Histogram, computed in parallel over a Vec.
   <p>
   A {@code DRealHistogram} bins every value added to it, and computes a the
   vec min & max (for use in the next split), and response mean & variance for
   each bin.  {@code DRealHistogram}s are initialized with a min, max and
   number-of- elements to be added (all of which are generally available from a
   Vec).  Bins run from min to max in uniform sizes.  If the {@code
   DRealHistogram} can determine that fewer bins are needed (e.g. boolean
   columns run from 0 to 1, but only ever take on 2 values, so only 2 bins are
   needed), then fewer bins are used.
   <p>
   {@code DRealHistogram} are shared per-node, and atomically updated.
   There's an {@code add} call to help cross-node reductions.  The data is
   stored in primitive arrays, so it can be sent over the wire.  The {@code
   AtomicXXXArray} classes are local utility classes for atomically updating
   primitive arrays.
   <p>
   If we are successively splitting rows (e.g. in a decision tree), then a
   fresh {@code DRealHistogram} for each split will dynamically re-bin the data.
   Each successive split will logarithmically divide the data.  At the first
   split, outliers will end up in their own bins - but perhaps some central
   bins may be very full.  At the next split(s), the full bins will get split,
   and again until (with a log number of splits) each bin holds roughly the
   same amount of data.  This dynamic binning resolves a lot of problems with
   picking the proper bin count or limits - generally a few more tree levels
   will equal any fancy but fixed-size binning strategy.
   <p>
   @author Cliff Click
*/
public class DRealHistogram extends Iced {
  public final transient String _name; // Column name (for debugging)
  public final byte _isInt;       // 0: float col, 1: int col, 2: enum & int col
  public final char _nbin;        // Bin count
  public final float  _step;      // Linear interpolation step per bin
  public final float  _min, _max; // Conservative Min/Max over whole collection
  public       long   _bins[];    // Bins, shared, atomically incremented
  private      float  _mins[], _maxs[]; // Min/Max, shared, atomically updated
  private      double _sums[], _ssqs[]; // Sums & square-sums, shared, atomically incremented

  public DRealHistogram( String name, final int nbins, byte isInt, float min, float max, long nelems ) {
    assert nelems > 0;
    assert nbins >= 1;
    assert max > min : "Caller ensures "+max+">"+min+", since if max==min== the column "+name+" is all constants";
    _isInt = isInt;
    _name = name;
    _min=min;
    _max=max;
    // See if we can show there are fewer unique elements than nbins.
    // Common for e.g. boolean columns, or near leaves.
    int xbins = nbins;
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
    _step = 1.0f/step;
    _nbin = (char)xbins;
    // Do not allocate the big arrays here; wait for scoreCols to pick which cols will be used.
  }

  // Interpolate d to find bin#
  int bin( float col_data ) {
    if( Float.isNaN(col_data) ) return 0; // Always NAs to bin 0
    assert col_data <= _max : "Coldata out of range "+col_data+" "+this;
    int idx1  = (int)((col_data-_min)*_step);
    int idx2  = Math.max(Math.min(idx1,_bins.length-1),0); // saturate at bounds
    return idx2;
  }
  float binAt( int b ) { return _min+b/_step; }

  public int nbins() { return _nbin; }
  public long  bins(int b) { return _bins[b]; }
  public float mins(int b) { return _mins[b]; }
  public float maxs(int b) { return _maxs[b]; }
  public double mean(int b) {
    long n = _bins[b];
    return n>0 ? _sums[b]/n : 0;
  }
  public double var (int b) {
    long n = _bins[b];
    if( n<=1 ) return 0;
    return (_ssqs[b] - _sums[b]*_sums[b]/n)/(n-1);
  }

  // Big allocation of arrays
  final void init() {
    assert _bins == null;
    _bins = MemoryManager.malloc8 (_nbin);
    _mins = MemoryManager.malloc4f(_nbin);
    Arrays.fill(_mins, Float.MAX_VALUE);
    _maxs = MemoryManager.malloc4f(_nbin);
    Arrays.fill(_maxs,-Float.MAX_VALUE);
    _sums = MemoryManager.malloc8d(_nbin);
    _ssqs = MemoryManager.malloc8d(_nbin);
  }

  // Add one row to a bin found via simple linear interpolation.
  // Compute bin min/max.
  // Compute response mean & variance.
  final void incr( float col_data, double y ) {
    int b = bin(col_data);      // Compute bin# via linear interpolation
    AtomicLongArray.incr(_bins,b); // Bump count in bin
    // Track actual lower/upper bound per-bin
    AtomicFloatArray.setMin(_mins,b,col_data);
    AtomicFloatArray.setMax(_maxs,b,col_data);
    if( y != 0 ) {
      AtomicDoubleArray.add(_sums,b,y);
      AtomicDoubleArray.add(_ssqs,b,y*y);
    }
  }

  // Merge two equal histograms together.  Done in a F/J reduce, so no
  // synchronization needed.
  void add( DRealHistogram dsh ) {
    assert _isInt == dsh._isInt && _nbin == dsh._nbin && _step == dsh._step &&
      _min == dsh._min && _max == dsh._max;
    assert (_bins == null && dsh._bins == null) || (_bins != null && dsh._bins != null);
    if( _bins == null ) return;
    Utils.add(_bins,dsh._bins);
    Utils.add(_sums,dsh._sums);
    Utils.add(_ssqs,dsh._ssqs);
    for( int i=0; i<_nbin; i++ ) if( dsh._mins[i] < _mins[i] ) _mins[i] = dsh._mins[i];
    for( int i=0; i<_nbin; i++ ) if( dsh._maxs[i] > _maxs[i] ) _maxs[i] = dsh._maxs[i];
  }

  public float find_min() {
    if( _bins == null ) return Float.NaN;
    int n = 0;
    while( n < _nbin && _bins[n]==0 ) n++; // First non-empty bin
    if( n == _nbin ) return Float.NaN;     // All bins are empty???
    return _mins[n];            // Take min from 1st non-empty bin
  }
  public float find_max() {
    int x = _nbin-1;            // Last bin
    while( _bins[x]==0 ) x--;   // Last non-empty bin
    return _maxs[x];            // Take max from last non-empty bin
  }

  // Compute a "score" for a column; lower score "wins" (is a better split).
  // Score is the sum of the MSEs when the data is split at a single point.
  // mses[1] == MSE for splitting between bins  0  and 1.
  // mses[n] == MSE for splitting between bins n-1 and n.
  public DTree.Split scoreMSE( int col ) {
    final int nbins = nbins();
    assert nbins > 1;

    // Compute mean/var for cumulative bins from 0 to nbins inclusive.
    double sums0[] = MemoryManager.malloc8d(nbins+1);
    double ssqs0[] = MemoryManager.malloc8d(nbins+1);
    long     ns0[] = MemoryManager.malloc8 (nbins+1);
    for( int b=1; b<=nbins; b++ ) {
      double m0 = sums0[b-1],  m1 = _sums[b-1];
      double s0 = ssqs0[b-1],  s1 = _ssqs[b-1];
      long   k0 = ns0  [b-1],  k1 = _bins[b-1];
      if( k0==0 && k1==0 ) continue;
      sums0[b] = m0+m1;
      ssqs0[b] = s0+s1;
      ns0  [b] = k0+k1;
    }
    long tot = ns0[nbins];
    // If we see zero variance, we must have a constant response in this
    // column.  Normally this situation is cut out before we even try to split, but we might
    // have NA's in THIS column...
    if( ssqs0[nbins] == 0 ) { assert isConstantResponse(); return null; }

    // Compute mean/var for cumulative bins from nbins to 0 inclusive.
    double sums1[] = MemoryManager.malloc8d(nbins+1);
    double ssqs1[] = MemoryManager.malloc8d(nbins+1);
    long     ns1[] = MemoryManager.malloc8 (nbins+1);
    for( int b=nbins-1; b>=0; b-- ) {
      double m0 = sums1[b+1],  m1 = _sums[b];
      double s0 = ssqs1[b+1],  s1 = _ssqs[b];
      long   k0 = ns1  [b+1],  k1 = _bins[b];
      if( k0==0 && k1==0 ) continue;
      sums1[b] = m0+m1;
      ssqs1[b] = s0+s1;
      ns1  [b] = k0+k1;
      assert ns0[b]+ns1[b]==tot;
    }

    // Now roll the split-point across the bins.  There are 2 ways to do this:
    // split left/right based on being less than some value, or being equal/
    // not-equal to some value.  Equal/not-equal makes sense for catagoricals
    // but both splits could work for any integral datatype.  Do the less-than
    // splits first.
    int best=0;                         // The no-split
    double best_se0=Double.MAX_VALUE;   // Best squared error
    double best_se1=Double.MAX_VALUE;   // Best squared error
    boolean equal=false;                // Ranged check
    for( int b=1; b<=nbins-1; b++ ) {
      if( _bins[b] == 0 ) continue; // Ignore empty splits
      // We're making an unbiased estimator, so that MSE==Var.
      // Then Squared Error = MSE*N = Var*N
      //                    = (ssqs/N - mean^2)*N
      //                    = ssqs - N*mean^2
      //                    = ssqs - N*(sum/N)(sum/N)
      //                    = ssqs - sum^2/N
      double se0 = ssqs0[b] - sums0[b]*sums0[b]/ns0[b];
      double se1 = ssqs1[b] - sums1[b]*sums1[b]/ns1[b];
      if( (se0+se1 < best_se0+best_se1) || // Strictly less error?
          // Or tied MSE, then pick split towards middle bins
          (se0+se1 == best_se0+best_se1 &&
           Math.abs(b -(nbins>>1)) < Math.abs(best-(nbins>>1))) ) {
        best_se0 = se0;   best_se1 = se1;
        best = b;
      }
    }

    // If the min==max, we can also try an equality-based split
    if( _isInt > 0 && _step == 1.0f &&    // For any integral (not float) column
        _max-_min+1 > 2 ) { // Also need more than 2 (boolean) choices to actually try a new split pattern
      for( int b=1; b<=nbins-1; b++ ) {
        if( _bins[b] == 0 ) continue; // Ignore empty splits
        assert _mins[b] == _maxs[b] : "int col, step of 1.0 "+_mins[b]+".."+_maxs[b]+" "+this+" "+Arrays.toString(sums0)+":"+Arrays.toString(ns0);
        long N =        ns0[b+0] + ns1[b+1];
        double sums = sums0[b+0]+sums1[b+1];
        double ssqs = ssqs0[b+0]+ssqs1[b+1];
        if( N == 0 ) continue;
        double si =  ssqs    -  sums   * sums   /   N    ; // Left+right, excluding 'b'
        double sx = _ssqs[b] - _sums[b]*_sums[b]/_bins[b]; // Just 'b'
        if( si+sx < best_se0+best_se1 ) { // Strictly less error?
          best_se0 = si;   best_se1 = sx;
          best = b;        equal = true; // Equality check
        }
      }
    }

    if( best==0 ) return null;  // No place to split
    assert best > 0 : "Must actually pick a split "+best;
    long   n0 = !equal ?   ns0[best] :   ns0[best]+  ns1[best+1];
    long   n1 = !equal ?   ns1[best] : _bins[best]              ;
    double p0 = !equal ? sums0[best] : sums0[best]+sums1[best+1];
    double p1 = !equal ? sums1[best] : _sums[best]              ;
    return new DTree.Split(col,best,equal,best_se0,best_se1,n0,n1,p0/n0,p1/n1);
  }

  // The initial histogram bins are setup from the Vec rollups.
  static public DRealHistogram[] initialHist(Frame fr, int ncols, int nbins, DRealHistogram hs[]) {
    Vec vecs[] = fr.vecs();
    for( int c=0; c<ncols; c++ ) {
      Vec v = vecs[c];
      hs[c] = (v.naCnt()==v.length() || v.min()==v.max()) ? null
        : new DRealHistogram(fr._names[c],nbins,(byte)(v.isEnum() ? 2 : (v.isInt()?1:0)),(float)v.min(),(float)v.max(),v.length());
    }
    return hs;
  }

  // Check for a constant response variable
  public boolean isConstantResponse() {
    double m = Double.NaN;
    for( int b=0; b<_bins.length; b++ ) {
      if( _bins[b] == 0 ) continue;
      if( var(b) > 1e-16 ) return false;
      double mean = mean(b);
      if( mean != m )
        if( Double.isNaN(m) ) m=mean;
        else return false;
    }
    return true;
  }

  // Pretty-print a histogram
  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(_name).append(":").append(_min).append("-").append(_max).append(" step="+(1/_step)+" nbins="+_bins.length);
    if( _bins != null ) {
      for( int b=0; b<_bins.length; b++ ) {
        sb.append(String.format("\ncnt=%d, min=%f, max=%f, mean/var=", _bins[b],_mins[b],_maxs[b]));
        sb.append(String.format("%6.2f/%6.2f,", mean(b), var(b)));
      }
      sb.append('\n');
    }
    return sb.toString();
  }

  public long byteSize() {
    long sum = 8+8;             // Self header
    sum += 1+2;                 // enum; nbin
    sum += 4+4+4;               // step,min,max
    sum += 8*5;                 // 5 internal arrays
    if( _bins == null ) return sum;
    // + 20(array header) + len<<2 (array body)
    sum += 24+_bins.length<<3;
    sum += 20+_mins.length<<2;
    sum += 20+_maxs.length<<2;
    sum += 24+_sums.length<<3;
    sum += 24+_ssqs.length<<3;
    return sum;
  }

  // Atomically-updated float array
  private static class AtomicFloatArray {
    private static final Unsafe _unsafe = UtilUnsafe.getUnsafe();
    private static final int _Fbase  = _unsafe.arrayBaseOffset(float[].class);
    private static final int _Fscale = _unsafe.arrayIndexScale(float[].class);
    private static long rawIndex(final float[] ary, final int idx) {
      assert idx >= 0 && idx < ary.length;
      return _Fbase + idx * _Fscale;
    }
    static void setMin( float fs[], int i, float min ) {
      float old = fs[i];
      while( min < old && !_unsafe.compareAndSwapInt(fs,rawIndex(fs,i), Float.floatToRawIntBits(old), Float.floatToRawIntBits(min) ) )
        old = fs[i];
    }
    static void setMax( float fs[], int i, float max ) {
      float old = fs[i];
      while( max > old && !_unsafe.compareAndSwapInt(fs,rawIndex(fs,i), Float.floatToRawIntBits(old), Float.floatToRawIntBits(max) ) )
        old = fs[i];
    }
    static public String toString( float fs[] ) {
      SB sb = new SB();
      sb.p('[');
      for( float f : fs )
        sb.p(f==Float.MAX_VALUE ? "max": (f==-Float.MAX_VALUE ? "min": Float.toString(f))).p(',');
      return sb.p(']').toString();
    }
  }

  // Atomically-updated double array
  private static class AtomicDoubleArray {
    private static final Unsafe _unsafe = UtilUnsafe.getUnsafe();
    private static final int _Dbase  = _unsafe.arrayBaseOffset(double[].class);
    private static final int _Dscale = _unsafe.arrayIndexScale(double[].class);
    private static long rawIndex(final double[] ary, final int idx) {
      assert idx >= 0 && idx < ary.length;
      return _Dbase + idx * _Dscale;
    }
    static void add( double ds[], int i, double y ) {
      double old = ds[i];
      while( !_unsafe.compareAndSwapLong(ds,rawIndex(ds,i), Double.doubleToRawLongBits(old), Double.doubleToRawLongBits(old+y) ) )
        old = ds[i];
    }
  }

  // Atomically-updated long array.  Instead of using the similar JDK pieces,
  // allows the bare array to be exposed for fast readers.
  private static class AtomicLongArray {
    private static final Unsafe _unsafe = UtilUnsafe.getUnsafe();
    private static final int _Lbase  = _unsafe.arrayBaseOffset(long[].class);
    private static final int _Lscale = _unsafe.arrayIndexScale(long[].class);
    private static long rawIndex(final long[] ary, final int idx) {
      assert idx >= 0 && idx < ary.length;
      return _Lbase + idx * _Lscale;
    }
    static void incr( long ls[], int i ) {
      long old = ls[i];
      while( !_unsafe.compareAndSwapLong(ls,rawIndex(ls,i), old, old+1) )
        old = ls[i];
    }
  }
}
