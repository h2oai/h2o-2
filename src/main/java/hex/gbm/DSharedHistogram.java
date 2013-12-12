package hex.gbm;

import water.*;
import water.util.SB;
import java.util.concurrent.atomic.*;
import java.util.Arrays;
//import water.*;
//import water.fvec.*;
//import water.util.Log;
//import water.util.Utils;

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
   will equal any fancy but fixed-size binning strategy.
   <p>
   @author Cliff Click
*/
public class DSharedHistogram extends Iced {
  public final String _name;      // Column name (for debugging)
  public final byte _isInt;       // 0: float col, 1: int col, 2: enum & int col
  public final float  _step;      // Linear interpolation step per bin
  public final float  _min, _max; // Conservative Min/Max over whole collection
  public final AtomicLongArray _bins;// Bins, shared, atomically incremented
  public final AtomicFloatArray _mins, _maxs; // Min/Max, shared, atomically updated
  public final AtomicDoubleArray _sums, _ssqs;// Sums & square-sums, shared, atomically incremented

  public DSharedHistogram( String name, final int nbins, byte isInt, float min, float max, long nelems ) {
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
    int [] mins = MemoryManager.malloc4(xbins);
    int [] maxs = MemoryManager.malloc4(xbins);
    Arrays.fill(mins,Float.floatToRawIntBits( Float.MAX_VALUE));
    Arrays.fill(maxs,Float.floatToRawIntBits(-Float.MAX_VALUE));
    _step = 1.0f/step;
    _bins = new AtomicLongArray(xbins);
    _mins = new AtomicFloatArray(mins);
    _maxs = new AtomicFloatArray(maxs);
    _sums = new AtomicDoubleArray(xbins);
    _ssqs = new AtomicDoubleArray(xbins);
  }

  // Interpolate d to find bin#
  int bin( float col_data ) {
    if( Float.isNaN(col_data) ) return 0; // Always NAs to bin 0
    assert col_data <= _max : "Coldata out of range "+col_data+" "+this;
    int idx1  = (int)((col_data-_min)*_step);
    int idx2  = Math.max(Math.min(idx1,_bins.length()-1),0); // saturate at bounds
    return idx2;
  }
  float binAt( int b ) { return _min+b/_step; }

  public int nbins() { return _bins.length(); }
  public long bins(int b) { return _bins.get(b); }
  public float mins(int b) { return _mins.getf(b); }
  public float maxs(int b) { return _maxs.getf(b); }
  public double mean(int b) { return _sums.getd(b)/_bins.get(b); }
  public double var (int b) { 
    long n = _bins.get(b);
    return n>1 ? _ssqs.getd(b)/(n-1) : 0;
  }

  // Add one row to a bin found via simple linear interpolation.
  // Compute bin min/max.
  // Compute response mean & variance.
  void incr( float col_data, double y ) {
    int b = bin(col_data);      // Compute bin# via linear interpolation
    _bins.incrementAndGet(b);   // Bump count in bin
    // Track actual lower/upper bound per-bin
    _mins.setMin(b,col_data);
    _maxs.setMax(b,col_data);
    if( y != 0 ) {
      _sums.add(b,y);
      _ssqs.add(b,y*y);
    }
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
      double m0 = sums0[b-1],  m1 = _sums.getd(b-1);
      double s0 = ssqs0[b-1],  s1 = _ssqs.getd(b-1);
      long   k0 = ns0  [b-1],  k1 = _bins.get (b-1);
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
      double m0 = sums1[b+1],  m1 = _sums.getd(b);
      double s0 = ssqs1[b+1],  s1 = _ssqs.getd(b);
      long   k0 = ns1  [b+1],  k1 = _bins.get (b);
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
      if( _bins.get(b) == 0 ) continue;    // Ignore empty splits
      double se0 = ssqs0[b] - sums0[b]*sums0[b]/ns0[b];
      double se1 = ssqs1[b] - sums1[b]*sums1[b]/ns1[b];
      if( (se0+se1 < best_se0+best_se1) || // Strictly less error?
          // Or tied MSE, then pick split towards middle bins
          (se0+se1 == best_se0+best_se1 && best < (nbins>>1)) ) {
        best_se0 = se0;   best_se1 = se1;
        best = b;
      }
    }

    // If the min==max, we can also try an equality-based split
    if( _isInt > 0 && _step == 1.0f ) { // For any integral (not float) column
      for( int b=1; b<=nbins-1; b++ ) {
        if( _bins.get(b) == 0 ) continue; // Ignore empty splits
        assert _mins.get(b) == _maxs.get(b) : "int col, step of 1.0 "+_mins.get(b)+".."+_maxs.get(b)+" "+this+" "+Arrays.toString(sums0)+":"+Arrays.toString(ns0);
        throw H2O.unimpl();
        //double m0 = MS0[2*(b+0)+0],  m1 = MS1[2*(b+1)+0];
        //double s0 = MS0[2*(b+0)+1],  s1 = MS1[2*(b+1)+1];
        //long   k0 = ns0[   b+0   ],  k1 = ns1[   b+1   ];
        //if( k0==0 && k1==0 ) continue;
        //double delta=m1-m0;
        //double mi = (k0*m0+k1*m1)/(k0+k1); // Mean of included set
        //double si = s0+s1+delta*delta*k0*k1/(k0+k1); // 2nd moment of included set
        //double sx = _MSs[2*(b+0)+1];                 // The excluded single bin
        //if( si+sx < best_se0+best_se1 ) { // Strictly less error?
        //  best_se0 = si;   best_se1 = sx;
        //  best = b;        equal = true; // Equality check
        //}
      }
    }
    
    if( best==0 ) return null;  // No place to split
    assert best > 0 : "Must actually pick a split "+best;
    long n0 = !equal ? ns0[best] : ns0[best]+ns1[best+1];
    long n1 = !equal ? ns1[best] : _bins.get(best)      ;
    return new DTree.Split(col,best,equal,best_se0,best_se1,n0,n1);
  }

  // Check for a constant response variable
  public boolean isConstantResponse() {
    double m = Double.NaN;
    for( int b=0; b<_bins.length(); b++ ) {
      if( _bins.get(b) == 0 ) continue;
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
    sb.append(_name).append(":").append(_min).append("-").append(_max).append(" step="+(1/_step)+" nbins="+_bins.length());
    if( _bins != null ) {
      for( int b=0; b<_bins.length(); b++ ) {
        sb.append(String.format("\ncnt=%d, min=%f, max=%f, mean/var=", _bins.get(b),_mins.get(b),_maxs.get(b)));
        sb.append(String.format("%6.2f/%6.2f,", mean(b), var(b)));
      }
      sb.append('\n');
    }
    return sb.toString();
  }

  long byteSize() {
    long sum = 8+8;             // Self header
    sum += 4+4+4;               //step,min,max
    // + 8(ptr) + 32(AtomicXXXArray) + 20(array header) + len<<2 (array body)
    sum += 8+32+20+_bins.length()<<2;
    sum += 8+32+20+_mins.length()<<2;
    sum += 8+32+20+_maxs.length()<<2;
    sum += 8+32+24+_sums.length()<<3;
    sum += 8+32+24+_ssqs.length()<<3;
    return sum;
  }

  // --------------------------------------------------------------------------
  // Atomically-updated float array, backs over int array
  private static class AtomicFloatArray extends AtomicIntegerArray {
    public AtomicFloatArray( int[] is ) { super(is); }
    float getf( int i ) { return Float.intBitsToFloat(get(i)); }
    boolean compareAndSet( int i, float expect, float update ) {  
      return compareAndSet( i, Float.floatToRawIntBits(expect), Float.floatToRawIntBits(update)); 
    }
    void setMin( int i, float min ) {
      float old = getf(i);
      while( min < old && !compareAndSet(i,old,min) )
        old = getf(i);
    }
    void setMax( int i, float max ) {
      float old = getf(i);
      while( max > old && !compareAndSet(i,old,max) )
        old = getf(i);
    }
    @Override public String toString() {
      SB sb = new SB();
      sb.p('[');
      final int len = length();
      for( int i=0; i<len; i++ ) {
        float f= getf(i);
        sb.p(f==Float.MAX_VALUE ? "max": (f==-Float.MAX_VALUE ? "min": Float.toString(f))).p(',');
      }
      return sb.p(']').toString();
    }
  }

  // Atomically-updated double array, backs over long array
  private static class AtomicDoubleArray extends AtomicLongArray {
    public AtomicDoubleArray( int len ) { super(len); }
    double getd( int i ) { return Double.longBitsToDouble(get(i)); }
    boolean compareAndSet( int i, double expect, double update ) {  
      return compareAndSet( i, Double.doubleToRawLongBits(expect), Double.doubleToRawLongBits(update)); 
    }
    void add( int i, double y ) {
      double old = getd(i);
      while( !compareAndSet(i,old,old+y) )
        old = getd(i);
    }
    @Override public String toString() {
      SB sb = new SB();
      sb.p('[');
      final int len = length();
      for( int i=0; i<len; i++ ) {
        double d= getd(i);
        sb.p(d==Double.MAX_VALUE ? "max": (d==-Double.MAX_VALUE ? "min": Double.toString(d))).p(',');
      }
      return sb.p(']').toString();
    }
  }

  @Override public AutoBuffer write(AutoBuffer bb) { throw H2O.unimpl(); }
  @Override public <T extends Freezable> T read(AutoBuffer bb) { throw H2O.unimpl(); }
  @Override public <T extends Freezable> T newInstance() { throw H2O.unimpl(); }
}
