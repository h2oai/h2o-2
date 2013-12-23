package hex.gbm;

import hex.drf.DRF;
import java.util.Arrays;
import java.util.concurrent.atomic.*;
import water.*;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.SB;
import water.util.Utils;

/**
   A Histogram, computed in parallel over a Vec.
   <p>
   A {@code DHistogram} bins every value added to it, and computes a the vec
   min & max (for use in the next split), and response mean & variance for each
   bin.  {@code DHistogram}s are initialized with a min, max and number-of-
   elements to be added (all of which are generally available from a Vec).
   Bins run from min to max in uniform sizes.  If the {@code DHistogram} can
   determine that fewer bins are needed (e.g. boolean columns run from 0 to 1,
   but only ever take on 2 values, so only 2 bins are needed), then fewer bins
   are used.
   <p>
   {@code DHistogram} are shared per-node, and atomically updated.  There's an
   {@code add} call to help cross-node reductions.  The data is stored in
   primitive arrays, so it can be sent over the wire.  
   <p>
   If we are successively splitting rows (e.g. in a decision tree), then a
   fresh {@code DHistogram} for each split will dynamically re-bin the data.
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
public abstract class DHistogram<TDH extends DHistogram> extends Iced {
  public final transient String _name; // Column name (for debugging)
  public final byte _isInt;       // 0: float col, 1: int col, 2: enum & int col
  public final char _nbin;        // Bin count
  public final float  _step;      // Linear interpolation step per bin
  public final float  _min, _max; // Conservative Min/Max over whole collection
  public       long   _bins[];    // Bins, shared, atomically incremented
  protected    float  _mins[], _maxs[]; // Min/Max, shared, atomically updated

  public DHistogram( String name, final int nbins, final byte isInt, final float min, final float max, long nelems ) {
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
  abstract boolean isBinom();

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
  abstract public double mean(int b);
  abstract public double var (int b);

  // Big allocation of arrays
  abstract void init0();
  final void init() {
    assert _bins == null;
    _bins = MemoryManager.malloc8 (_nbin);
    if( DRF._optflags == 0 ) {
      _mins = MemoryManager.malloc4f(_nbin);
      Arrays.fill(_mins, Float.MAX_VALUE);
      _maxs = MemoryManager.malloc4f(_nbin);
      Arrays.fill(_maxs,-Float.MAX_VALUE);
    }
    init0();
  }

  // Add one row to a bin found via simple linear interpolation.
  // Compute bin min/max.
  // Compute response mean & variance.
  abstract void incr0( int b, double y );
  final void incr( float col_data, double y ) {
    assert _min <= col_data && col_data <= _max;
    int b = bin(col_data);      // Compute bin# via linear interpolation
    Utils.AtomicLongArray.incr(_bins,b); // Bump count in bin
    // Track actual lower/upper bound per-bin
    if( DRF._optflags == 0 ) {
      Utils.AtomicFloatArray.setMin(_mins,b,col_data);
      Utils.AtomicFloatArray.setMax(_maxs,b,col_data);
    }
    if( y != 0 ) incr0(b,y);
  }

  // Merge two equal histograms together.  Done in a F/J reduce, so no
  // synchronization needed.
  abstract void add0( TDH dsh );
  void add( TDH dsh ) {
    assert _isInt == dsh._isInt && _nbin == dsh._nbin && _step == dsh._step &&
      _min == dsh._min && _max == dsh._max;
    assert (_bins == null && dsh._bins == null) || (_bins != null && dsh._bins != null);
    if( _bins == null ) return;
    Utils.add(_bins,dsh._bins);
    if( DRF._optflags == 0 ) {
      for( int i=0; i<_nbin; i++ ) if( dsh._mins[i] < _mins[i] ) _mins[i] = dsh._mins[i];
      for( int i=0; i<_nbin; i++ ) if( dsh._maxs[i] > _maxs[i] ) _maxs[i] = dsh._maxs[i];
    }
    add0(dsh);
  }

  public float find_min() {
    if( _bins == null ) return Float.NaN;
    int n = 0;
    while( n < _nbin && _bins[n]==0 ) n++; // First non-empty bin
    if( n == _nbin ) return Float.NaN;     // All bins are empty???
    if( DRF._optflags==0 ) return _mins[n];// Take min from 1st non-empty bin
    if( n==0 ) return _min;
    float min = binAt(n);
    if( _isInt > 0 ) min = (float)Math.ceil(min);
    return min;
  }
  public float find_max() {
    int x = _nbin-1;            // Last bin
    while( _bins[x]==0 ) x--;   // Last non-empty bin
    if( DRF._optflags==0 ) return _maxs[x];// Take min from 1st non-empty bin
    if( x== _nbin-1 ) return _max;
    float max = binAt(x+1);
    if( _isInt > 0 ) max = (float)Math.floor(max);
    return max;
  }

  // Compute a "score" for a column; lower score "wins" (is a better split).
  // Score is the sum of the MSEs when the data is split at a single point.
  // mses[1] == MSE for splitting between bins  0  and 1.
  // mses[n] == MSE for splitting between bins n-1 and n.
  abstract public DTree.Split scoreMSE( int col );

  // The initial histogram bins are setup from the Vec rollups.
  static public DHistogram[] initialHist(Frame fr, int ncols, int nbins, DHistogram hs[], boolean isBinom) {
    Vec vecs[] = fr.vecs();
    for( int c=0; c<ncols; c++ ) {
      Vec v = vecs[c];
      hs[c] = v.naCnt()==v.length() || v.min()==v.max() ? null :
        make(fr._names[c],nbins,(byte)(v.isEnum() ? 2 : (v.isInt()?1:0)),(float)v.min(),(float)v.max(),v.length(),isBinom);
    }
    return hs;
  }

  static public DHistogram make( String name, final int nbins, byte isInt, float min, float max, long nelems, boolean isBinom ) {
    return isBinom 
      ? new DBinomHistogram(name,nbins,isInt,min,max,nelems)
      : new  DRealHistogram(name,nbins,isInt,min,max,nelems);
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
    sb.append(_name).append(":").append(_min).append("-").append(_max).append(" step="+(1/_step)+" nbins="+nbins()+" isInt="+_isInt);
    if( _bins != null ) {
      for( int b=0; b<_bins.length; b++ ) {
        sb.append(String.format("\ncnt=%d, min=%f, max=%f, mean/var=", _bins[b],_mins[b],_maxs[b]));
        sb.append(String.format("%6.2f/%6.2f,", mean(b), var(b)));
      }
      sb.append('\n');
    }
    return sb.toString();
  }

  abstract public long byteSize0();
  public long byteSize() {
    long sum = 8+8;             // Self header
    sum += 1+2;                 // enum; nbin
    sum += 4+4+4;               // step,min,max
    sum += 8*3;                 // 3 internal arrays
    if( _bins == null ) return sum;
    // + 20(array header) + len<<2 (array body)
    sum += 24+_bins.length<<3;
    sum += 20+_mins.length<<2;
    sum += 20+_maxs.length<<2;
    sum += byteSize0();
    return sum;
  }
}
