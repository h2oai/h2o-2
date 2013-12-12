package hex.gbm;

import water.H2O;
import java.util.concurrent.atomic.*;
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
public class DSharedHistogram extends DHistogram<DSharedHistogram> {
  public final float   _step;        // Linear interpolation step per bin
  public final float   _bmin;        // Linear interpolation min  per bin
  public final AtomicLongArray _bins;// Bins, shared, atomically incremented
  public final AtomicFloatArray _mins, _maxs; // Min/Max, shared, atomically updated
  public final AtomicDoubleArray _sums, _ssqs;// Sums & square-sums, shared, atomically incremented

  public DSharedHistogram( String name, final char nbins, byte isInt, float min, float max, long nelems ) {
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
    _bmin = min;                // Bin-Min
    _step = 1.0f/step;          // Step is multiplicative
    _bins = new AtomicLongArray(xbins);
    _mins = new AtomicFloatArray(xbins);
    _maxs = new AtomicFloatArray(xbins);
    _sums = new AtomicDoubleArray(xbins);
    _ssqs = new AtomicDoubleArray(xbins);
  }

  // Interpolate d to find bin#
  int bin( float col_data ) {
    if( Float.isNaN(col_data) ) return 0; // Always NAs to bin 0
    assert col_data <= _max : "Coldata out of range "+col_data+" "+this;
    int idx1  = (int)((col_data-_bmin)*_step);
    int idx2  = Math.max(Math.min(idx1,_bins.length()-1),0); // saturate at bounds
    return idx2;
  }
  float binAt( int b ) { return _bmin+b/_step; }

  @Override public double mean(int b) { return _sums.get(b)/_bins.get(b); }
  @Override public double var (int b) { 
    long n = _bins.get(b);
    return n>1 ? _ssqs.get(b)/(n-1) : 0;
  }
  

  // Add one row to a bin found via simple linear interpolation.
  // Compute bin min/max.
  // Compute response mean & variance.
  void incr( int row, float col_data, double y ) {
    int b = bin(col_data);      // Compute bin# via linear interpolation
    _bins.incrementAndGet(b);   // Bump count in bin
    // Track actual lower/upper bound per-bin
    _mins.setMin(b,col_data);
    _maxs.setMax(b,col_data);
    _sums.add(b,y);
    _ssqs.add(b,y*y);
  }

  // Pretty-print a histogram
  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(_name).append(":").append(_min).append("-").append(_max+", bmin="+_bmin+" step="+(1/_step)+" nbins="+(int)_bins.length());
    if( _bins != null ) {
      for( int b=0; b<_bins.length(); b++ ) {
        sb.append(String.format("\ncnt=%d, min=%f, max=%f, mean/var=", _bins.get(b),_mins.get(b),_maxs.get(b)));
        sb.append(String.format("%6.2f/%6.2f,", mean(b), var(b)));
      }
      sb.append('\n');
    }
    return sb.toString();
  }

  @Override long byteSize() {
    long sum = super.byteSize();
    sum += 4+4+2+2;/*step,bmin,nbins,pad*/
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
    public AtomicFloatArray( int len ) { super(len); }
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
  }

  // Atomically-updated double array, backs over long array
  private static class AtomicDoubleArray extends AtomicLongArray {
    public AtomicDoubleArray( int len ) { super(len); }
    double getd( int i ) { return Double.longBitsToDouble(get(i)); }
    boolean compareAndSet( long i, double expect, double update ) {  
      return compareAndSet( i, Double.doubleToRawLongBits(expect), Double.doubleToRawLongBits(update)); 
    }
    void add( int i, double y ) {
      double old = getd(i);
      while( !compareAndSet(i,old,old+y) )
        old = getd(i);
    }
  }



  
}
