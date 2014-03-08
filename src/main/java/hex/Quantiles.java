package hex;

import water.*;
import water.api.*;
import water.api.Request.API;
import water.fvec.*;
import water.util.Utils;
import water.util.Log;

/**
 * Quantile of a column.
 */
public class Quantiles extends Iced {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Returns a quantile of a fluid-vec frame";

  public static final int    MAX_ENUM_SIZE = water.parser.Enum.MAX_ENUM_SIZE;
  // just use [0] here?
  public final double QUANTILES_TO_DO[];

  long                       _gprows;    // non-empty rows per group
  // FIX! not sure if I need to save these here from vec
  final transient double     _max;
  final transient double     _min;
  final transient double     _mean;
  final transient double     _sigma;
  final transient long       _naCnt;
  final transient boolean    _isInt;
  final transient boolean    _isEnum;
  final transient String[]   _domain;

  // used in approxQuantilesOnePass only
  final transient double     _start2;
  final transient double     _binsz2;    // 2nd finer grained histogram used for quantile estimates for numerics

  // used to feed the next iteration for multipass?
  // used in exactQuantilesMultiPass only
  public transient double     _valStart = 0; // FIX! I shouldn't need init here
  public transient double     _valEnd = 0; // FIX! I shouldn't need init here
  public transient double     _valMaxBinCnt = 0; // FIX! I shouldn't need init here
  // just for info on current pass?
  public transient double     _valRange = 0; // FIX! I shouldn't need init here
  public transient double     _valBinSize = 0; // FIX! I shouldn't need init here

  public transient double[]  _pctile;

  // OUTPUTS
  // Basic info
  @API(help="name"        ) public String    colname;

  public long[]  hcnt2; // finer histogram. not visible
  public double[]  hcnt2_min; // min actual for each bin
  public double[]  hcnt2_max; // max actual for each bin
  public long  hcnt2_low; // count below current binning
  public long  hcnt2_high; // count above current binning
  public double hcnt2_high_min; // min above current binning

  public static class BinTask2 extends MRTask2<BinTask2> {
    private final double _quantile;
    private final int _max_qbins;
    private final double _valStart;
    private final double _valEnd;
    private final boolean _multiPass;

    public Quantiles _qbins[];

    public BinTask2 (double quantile, int max_qbins, double valStart, double valEnd, boolean multiPass)
      { 
        _quantile = quantile; 
        _max_qbins = max_qbins; 
        _valStart = valStart; 
        _valEnd = valEnd; 
        _multiPass = multiPass; 
      }

    @Override public void map(Chunk[] cs) {
      _qbins = new Quantiles[cs.length];
      for (int i = 0; i < cs.length; i++)
        _qbins[i] = new Quantiles(_fr.vecs()[i], _fr.names()[i], _quantile, _max_qbins,
          _valStart, _valEnd, _multiPass).add(cs[i]);
    }

    @Override public void reduce(BinTask2 other) {
      for (int i = 0; i < _qbins.length; i++)
        _qbins[i].add(other._qbins[i]);
    }
  }

  public void finishUp(Vec vec, long max_qbins) {
    // below, we force it to ignore length and only do [0]
    // need to figure out if we need to do a list and how that's returned
    _pctile = new double[QUANTILES_TO_DO.length];

    if ( _isEnum ) {
      ;
    } else {
      if ( false ) {
        // FIX! how to pass this corectly
        long desiredBinCnt = max_qbins + 1;
        exactQuantilesMultiPass(_pctile, QUANTILES_TO_DO, desiredBinCnt);
      } else {
        approxQuantilesOnePass(_pctile, QUANTILES_TO_DO);
      }
    }
  }

  public Quantiles(Vec vec, String name, double quantile, int max_qbins, 
        double valStart, double valEnd, boolean multiPass) {

    colname = name;
    _isEnum = vec.isEnum();
    _isInt = vec.isInt();
    _domain = vec.isEnum() ? vec.domain() : null;
    _max = vec.max();
    _min = vec.min();
    _mean = vec.mean();
    _sigma = vec.sigma();
    _naCnt = vec.naCnt();

    _gprows = 0;
    QUANTILES_TO_DO = new double[1];
    QUANTILES_TO_DO[0] = quantile;

    _valStart = valStart;
    _valEnd = valEnd;
    _valRange = valEnd - valStart;

    int desiredBinCnt = max_qbins;
    int maxBinCnt = desiredBinCnt + 1;
    _valBinSize = _valRange / (desiredBinCnt + 0.0);
    _valMaxBinCnt = maxBinCnt;

    if( vec.isEnum() && _domain.length < MAX_ENUM_SIZE ) {
      // do we even care here? don't want to think about whether multiPass is disabled
      _start2 = 0;
      _binsz2 = 1;
      hcnt2 = new long[_domain.length];
      hcnt2_min = new double[_domain.length];
      hcnt2_max = new double[_domain.length];
    } 
    else if ( !Double.isNaN(_min) ) {
      assert max_qbins > 0 && max_qbins <= 1000000 : "max_qbins must be >0 and <= 1000000";
      // only used on single pass
      _binsz2 = (_max + (vec.isInt()?.5:0) - _min) / max_qbins;
      _start2 = _binsz2 * Math.floor(_min/_binsz2);

      if ( multiPass ) {
        assert maxBinCnt > 0;
        // Log.info("Finer histogram has "+nbin2+" bins. Visible histogram has "+nbin);
        // Log.info("Finer histogram starts at "+_start2+" Visible histogram starts at "+_start);
        // Log.info("_min "+_min+" _max "+_max);
        // can't make any assertion about _start2 vs _start  (either can be smaller due to fp issues)
        hcnt2 = new long[maxBinCnt];
        hcnt2_min = new double[maxBinCnt];
        hcnt2_max = new double[maxBinCnt];
      }
      else {
        // okay if 1 more than max_qbins gets created
        // _binsz2 = _binsz / (max_qbins / nbin);
        int nbin2 = (int)(Math.round((_max + (vec.isInt()?.5:0) - _start2)*1000000.0/_binsz2)/1000000L) + 1;
        assert nbin2 > 0;
        // Log.info("Finer histogram has "+nbin2+" bins. Visible histogram has "+nbin);
        // Log.info("Finer histogram starts at "+_start2+" Visible histogram starts at "+_start);
        // Log.info("_min "+_min+" _max "+_max);
        // can't make any assertion about _start2 vs _start  (either can be smaller due to fp issues)
        hcnt2 = new long[nbin2];
        hcnt2_min = new double[nbin2];
        hcnt2_max = new double[nbin2];
      }
    } 
    else { // vec does not contain finite numbers
      // do we care here? have to think about whether multiPass is disabled/
      _start2 = vec.min();
      _binsz2 = Double.POSITIVE_INFINITY;
      hcnt2 = new long[1];
      hcnt2_min = new double[1];
      hcnt2_max = new double[1];
    }
    // these longs are used (see above)
    // hcnt2_low
    // hcnt2_high
    // hcnt2_high_min

    // Implicit on new?
    //  init to zero for each pass
    //  for (int i = 0; i < hcnt2.length; i++) hcnt2[i] = 0;
    //  hcnt2_low = 0;
    //  hcnt2_high = 0;
  }

  public Quantiles(Vec vec, String name) {
    // defaults to single pass median approximation?
    this(vec, name, 0.5, 1000, vec.min(), vec.max(), false);
  }

  public Quantiles add(Chunk chk) {
    for (int i = 0; i < chk._len; i++)
      add(chk.at0(i));
    return this;
  }
  public void add(double val) {
    if ( Double.isNaN(val) ) return;
    _gprows++;
    if ( _isEnum ) return;

    if ( true ) { // single pass approx
      long binIdx2;
      if (hcnt2.length==1) {
        binIdx2 = 0; // not used
      }
      else {
        // FIX! why is this round not floor? 
        binIdx2 = Math.round(((val - _start2) * 1000000.0) / _binsz2) / 1000000;
      }

      int binIdx2Int = (int) binIdx2;
      assert (binIdx2Int >= 0 && binIdx2Int < hcnt2.length) : 
        "binIdx2Int too big for hcnt2 "+binIdx2Int+" "+hcnt2.length;

      if ( (hcnt2[binIdx2Int] == 0) || (val < hcnt2_min[binIdx2Int]) ) {
        hcnt2_min[binIdx2Int] = val;
      }
      if ( (hcnt2[binIdx2Int] == 0) || (val > hcnt2_max[binIdx2Int]) ) {
        hcnt2_max[binIdx2Int] = val;
      }
      ++hcnt2[binIdx2Int];
    }
    else { // multi pass exact. Should be able to do this for both, if the valStart param is correct

      //  Need to count the stuff outside the bin-gathering, 
      //  since threshold compare is based on total row compare
      double valOffset = val - _valStart;
      if ( valOffset < 0 ) {
        ++hcnt2_low;
      }
      else if ( val > _valEnd ) {
        if ( (hcnt2_high==0) || (val < hcnt2_high_min) ) hcnt2_high_min = val;
        ++hcnt2_high;
      } 
      else {
        long binIdx2;
        if (hcnt2.length==1) {
          binIdx2 = 0; // not used
        }
        else {
          // FIX! talks about precision loss if I use Math.floor() here. Want floor
          binIdx2 = Math.round((valOffset * 1000000.0) / _valBinSize) / 1000000;
        }

        int binIdx2Int = (int) binIdx2;
        assert (binIdx2Int >= 0 && binIdx2Int < hcnt2.length) : 
          "binIdx2Int too big for hcnt2 "+binIdx2Int+" "+hcnt2.length;

        //  where are we zeroing in? (start)
        //  Log.info(valOffset, valBinSize
        assert (binIdx2Int>=0) && (binIdx2Int<=_valMaxBinCnt) : "binIdx2Int "+binIdx2Int+" out of range";
        if ( hcnt2[binIdx2Int]==0 || (val < hcnt2_min[binIdx2Int]) ) hcnt2_min[binIdx2Int] = val;
        if ( hcnt2[binIdx2Int]==0 || (val > hcnt2_max[binIdx2Int]) ) hcnt2_max[binIdx2Int] = val;
        ++hcnt2[binIdx2Int];
      }
    }
  } 

  public Quantiles add(Quantiles other) {
    _gprows += other._gprows;
    if ( _isEnum ) return this;

    // merge hcnt2 per-bin mins 
    // other must be same length, but use it's length for safety
    // could add assert on lengths?
    for (int k = 0; k < other.hcnt2_min.length; k++) {
      // for now..die on NaNs
      assert !Double.isNaN(other.hcnt2_min[k]) : "NaN in other.hcnt2_min merging";
      assert !Double.isNaN(other.hcnt2[k]) : "NaN in hcnt2_min merging";
      assert !Double.isNaN(hcnt2_min[k]) : "NaN in hcnt2_min merging";
      assert !Double.isNaN(hcnt2[k]) : "NaN in hcnt2_min merging";

      // cover the initial case (relying on initial min = 0 to work is wrong)
      // Only take the new max if it's hcnt2 is non-zero. like a valid bit
      // can hcnt2 ever be null here?
      if (other.hcnt2[k] > 0) {
        if ( hcnt2[k]==0 || ( other.hcnt2_min[k] < hcnt2_min[k] )) {
          hcnt2_min[k] = other.hcnt2_min[k];
        }
      }
    }

    // merge hcnt2 per-bin maxs
    // other must be same length, but use it's length for safety
    for (int k = 0; k < other.hcnt2_max.length; k++) {
      // for now..die on NaNs
      assert !Double.isNaN(other.hcnt2_max[k]) : "NaN in other.hcnt2_max merging";
      assert !Double.isNaN(other.hcnt2[k]) : "NaN in hcnt2_min merging";
      assert !Double.isNaN(hcnt2_max[k]) : "NaN in hcnt2_max merging";
      assert !Double.isNaN(hcnt2[k]) : "NaN in hcnt2_max merging";

      // cover the initial case (relying on initial min = 0 to work is wrong)
      // Only take the new max if it's hcnt2 is non-zero. like a valid bit
      // can hcnt2 ever be null here?
      if (other.hcnt2[k] > 0) {
        if ( hcnt2[k]==0 || ( other.hcnt2_max[k] > hcnt2_max[k] )) {
          hcnt2_max[k] = other.hcnt2_max[k];
        }
      }
    }

    // can hcnt2 ever be null here?. Inc last, so the zero case is detected above
    // seems like everything would fail if hcnt2 doesn't exist here
    assert hcnt2 != null;
    Utils.add(hcnt2, other.hcnt2);
    return this;
  }

  // need to count >4B rows
  private long htot2(long low, long high) {
    long cnt = 0;
    for (int i = 0; i < hcnt2.length; i++) cnt+=hcnt2[i];
    // add the stuff outside the bins, 0,0 for single pass
    cnt = cnt + low + high;
    return cnt;
  }

  private void exactQuantilesMultiPass(double[] qtiles, double[] thres, long desiredBinCnt) {
    // do we need all of these as output?
    double newValStart, newValEnd, newValRange, newValBinSize;
    // FIX! figure out where unitialized can be used
    newValStart = Double.NaN; 
    newValEnd = Double.NaN;
    newValRange = Double.NaN;
    newValBinSize = Double.NaN;
    long newValLowCnt;

    long maxBinCnt = desiredBinCnt + 1;

    assert !_isEnum;
    if( hcnt2.length == 0 ) return;
     // playing with creating relative NUDGE values to make sure bin range
    // is always inclusive of target.
    // ratio it down from valBinSize?  It doesn't need to be as big as valBinSize.
    // can't seem to make it work yet. leave NUDGE=0
    double NUDGE = 0;
    //  everything should either be in low, the bins, or high
    double threshold = thres[0];
    long totalRows = _gprows;
    long totalBinnedRows = htot2(hcnt2_low, hcnt2_high);
    assert totalRows==totalBinnedRows : totalRows+" "+totalBinnedRows;

    //  now walk thru and find out what bin to look inside
    long currentCnt = hcnt2_low;
    double targetCntFull = threshold * (totalRows-1);  //  zero based indexing
    long targetCntInt = (long) (Math.floor(threshold * (totalRows-1)));
    double targetCntFract = targetCntFull  - targetCntInt;
    assert (targetCntFract>=0) && (targetCntFract<=1);
    Log.info("targetCntInt: "+targetCntInt+" targetCntFract: "+targetCntFract);

    int k = 0;
    while((currentCnt + hcnt2[k]) <= targetCntInt) {
      currentCnt += hcnt2[k];
      ++k;
      assert k<=maxBinCnt : "k too large, k:"+k+" maxBinCnt: "+maxBinCnt;
    }

    assert hcnt2[k]!=1 || hcnt2_min[k]==hcnt2_max[k];

    // FIX!  here, we currently just use mean for interpolation, which is Type 2.
    // R is Type 7 linear interpolation, so can get difference (worse on small datasets)
    // We should select between Type 2 and Type 7 here when we interpolate
    // the linear interpolation for k between row a (vala) and row b (valb) is
    //    pctDiff = (k-a)/(b-a)
    //    dDiff = pctDiff * (valb - vala)
    //    result = vala + dDiff

    boolean done = false;
    //  some possibily interpolating guesses first, in guess we have to iterate (best guess)
    // don't really need or want this
    double guess = (hcnt2_max[k] - hcnt2_min[k]) / 2;

    if ( currentCnt==targetCntInt ) {
      if ( hcnt2[k]>2 ) {
        guess = hcnt2_min[k];
        done = true;
        Log.info("Guess A "+guess);

      } else if ( hcnt2[k]==2 ) {
        //  no mattter what size the fraction it would be on this number
        guess = (hcnt2_max[k] + hcnt2_min[k]) / 2.0;
        done = true;
        Log.info("Guess B"+guess);

      } else if ( (hcnt2[k]==1) && (targetCntFract==0) ) {
        assert hcnt2_min[k]==hcnt2_max[k];
        guess = hcnt2_min[k];
        done = true;
        Log.info("k"+k);
        Log.info("Guess C"+guess);

      } else if ( hcnt2[k]==1 && targetCntFract!=0 ) {
        assert hcnt2_min[k]==hcnt2_max[k];
        Log.info("Single value in this bin, but fractional means we need to interpolate to next non-zero");
        int nextK;
        if ( k<maxBinCnt ) nextK = k + 1; //  could put it over maxBinCnt
        else nextK = k;

        while ( (nextK<maxBinCnt) && (hcnt2[nextK]==0) ) ++nextK;

        //  have the "extra bin" for this
        double nextVal;
        if ( nextK >= maxBinCnt ) {
          assert hcnt2_high!=0;
          Log.info("Using hcnt2_high_min for interpolate:"+hcnt2_high_min);
          nextVal = hcnt2_high_min;
        } else {
          Log.info("Using nextK for interpolate:"+nextK);
          assert hcnt2[nextK]!=0;
          nextVal = hcnt2_min[nextK];
        }

        guess = (hcnt2_max[k] + nextVal) / 2.0;
        done = true; //  has to be one above us when needed. (or we're at end)

        Log.info("k"+"hcnt2_max[k]"+"nextVal");
        Log.info("hello3:"+k+hcnt2_max[k]+nextVal);
        Log.info("\nInterpolating result using nextK: "+nextK+ " nextVal: "+nextVal);
      }
    }
    if ( !done ) {
      newValStart = hcnt2_min[k] - NUDGE; //  FIX! should we nudge a little?
      newValEnd   = hcnt2_max[k] + NUDGE; //  FIX! should we nudge a little?
      newValRange = newValEnd - newValStart ;

      //  maxBinCnt is always binCount + 1, since we might cover over due to rounding/fp issues?
      newValBinSize = newValRange / (desiredBinCnt + 0.0);
      newValLowCnt = currentCnt - 1; // is this right? don't use for anything (debug?)
      if ( newValBinSize==0 ) {
        //  assert done or newValBinSize!=0 and live with current guess
        Log.info("Assuming done because newValBinSize is 0.");
        Log.info("newValRange: "+newValRange+
          " hcnt2[k]: "+hcnt2[k]+
          " hcnt2_min[k]: "+hcnt2_min[k]+
          " hcnt2_max[k]: "+hcnt2_max[k]);
        guess = newValStart;
        Log.info("Guess E "+guess);
        done = true;
      }
      //  if we have to interpolate
      //  if it falls into this bin, interpolate to this bin means one answer?
      //  cover the case above with multiple entris in a bin, all the same value
      //  will be zero on the last pass?
      //  assert newValBinSize != 0 or done
      //  need the count up to but not including newValStart
    }

    // ++iteration;

    // Log.info("Ending Pass "+iteration);
    Log.info("guess: "+guess+" done: "+done+" hcnt2[k]: "+hcnt2[k]);
    Log.info("currentCnt: "+currentCnt+" targetCntInt: "+targetCntInt+" hcnt2_low: "+hcnt2_low+"hcnt2_high: "+hcnt2_high);
    Log.info("was "+_valStart+" "+_valEnd+" "+_valRange+" "+_valBinSize);
    Log.info("next "+newValStart+" "+newValEnd+" "+newValRange+" "+newValBinSize);

    qtiles[0] = guess;
    // might have fp tolerance issues here? but fp numbers should be exactly same?
    // Log.info(]: hcnt2[k]: "+hcnt2[k]+" hcnt2_min[k]: "+hcnt2_min[k]+
    //  " hcnt2_max[k]: "+hcnt2_max[k]+" _binsz2: "+_binsz2+" guess: "+guess+" k: "+k+"\n");

    // Don't need these any more
    hcnt2 = null;
    hcnt2_min = null;
    hcnt2_max = null;

  }

  private void approxQuantilesOnePass(double[] qtiles, double[] thres){
    // not called for enums
    assert !_isEnum;
    if( hcnt2.length == 0 ) return;

    int k = 0; long s = 0;
    double guess = 0;
    double actualBinWidth = 0;
    assert _gprows==htot2(0, 0) : "_gprows: "+_gprows+" htot2(): "+htot2(0, 0);

    // A 'perfect' quantile definition, for comparison. 
    // Given a set of N ordered values {v[1], v[2], ...} and a requirement to 
    // calculate the pth percentile, do the following:
    // Calculate l = p(N-1) + 1
    // Split l into integer and decimal components i.e. l = k + d
    // Compute the required value as V = v[k] + d(v[k+1] - v[k])

    // we do zero-indexed list, so slightly different eqns.
    // walk up until we're at the bin that starts with the threshold, or right before
    // only do thres[0]. how do we make a list of thresholds work?
    // for(int j = 0; j < thres.length; ++j) {
    for(int j = 0; j <=0; ++j) {
      // 0 okay for threshold?
      assert 0 <= thres[j] && thres[j] <= 1;
      double s1 = Math.floor(thres[j] * (double) _gprows); 
      if ( s1 == 0 ) {
        s1 = 1; // always need at least one row
      }
      // what if _gprows is 0?. just return above?. Is it NAs?
      // assert _gprows > 0 : _gprows;
      if( _gprows == 0 ) return;
      assert 1 <= s1 && s1 <= _gprows : s1+" "+_gprows;
      // how come first bins can be 0? Fixed. problem was _start. Needed _start2. still can get some
      while( (s+hcnt2[k]) < s1) { // important to be < here. case: 100 rows, getting 50% right.
        s += hcnt2[k];
        k++;
      }
      // Log.info("Found k: "+k+" "+s+" "+s1+" "+_gprows+" "+hcnt2[k]+" "+hcnt2_min[k]+" "+hcnt2_max[k]);

      // All possible bin boundary issues 
      if ( s==s1 || hcnt2[k]==0 ) {
        if ( hcnt2[k]!=0 ) {
          guess = hcnt2_min[k];
          Log.info("Guess A: "+guess+" "+s+" "+s1);
        }
        else {
          if ( k==0 ) { 
            assert hcnt2[k+1]!=0 : "Unexpected state of starting hcnt2 bins";
            guess = hcnt2_min[k+1];
            // Log.info("Guess B: "+guess+" "+s+" "+s1);
          }
          else {
            if ( hcnt2[k-1]!=0 ) {
              guess = hcnt2_max[k-1];
              // Log.info("Guess C: "+guess+" "+s+" "+s1);
            }
            else {
              assert false : "Unexpected state of adjacent hcnt2 bins";
            }
          }
        }
      }
      else {
        // nonzero hcnt2[k] guarantees these are valid
        actualBinWidth = hcnt2_max[k] - hcnt2_min[k];

        // interpolate within the populated bin, assuming linear distribution
        // since we have the actual min/max within a bin, we can be more accurate
        // compared to using the bin boundaries
        // Note actualBinWidth is 0 when all values are the same in a bin
        // Interesting how we have a gap that we jump between max of one bin, and min of another.
        guess = hcnt2_min[k] + actualBinWidth * ((s1 - s) / hcnt2[k]);
        // Log.info("Guess D: "+guess+" "+k+" "+hcnt2_min[k]+" "+actualBinWidth+" "+s+" "+s1+" "+hcnt2[k]);
      }

      qtiles[j] = guess;

      // Don't need these any more
      hcnt2 = null;
      hcnt2_min = null;
      hcnt2_max = null;

      // might have fp tolerance issues here? but fp numbers should be exactly same?
      // Log.info(]: hcnt2[k]: "+hcnt2[k]+" hcnt2_min[k]: "+hcnt2_min[k]+
      //  " hcnt2_max[k]: "+hcnt2_max[k]+" _binsz2: "+_binsz2+" guess: "+guess+" k: "+k+"\n");
    }
  }

}
