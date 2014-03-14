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

  public long                _totalRows;    // non-empty rows per group
  // FIX! not sure if I need to save these here from vec
  // why were these 'transient' ? doesn't make sense if hcnt2 stuff wasn't transient
  // they're not very big. are they serialized in the map/reduce?
  final double     _max;
  final double     _min;
  final double     _mean;
  final double     _sigma;
  final long       _naCnt;
  final boolean    _isInt;
  final boolean    _isEnum;
  final String[]   _domain;

  // used in approxQuantilesOnePass only
  final double     _start2;
  final double     _binsz2;    // 2nd finer grained histogram used for quantile estimates for numerics

  // used to feed the next iteration for multipass?
  // used in exactQuantilesMultiPass only
  final double     _valStart;
  final double     _valEnd;
  final long       _valMaxBinCnt;
  final boolean    _multiPass;
  public int       _interpolationType; // shown in output 

  // just for info on current pass?
  public double    _valRange;
  public double    _valBinSize;

  public double    _newValStart;
  public double    _newValEnd;
  public double[]  _pctile;
  public boolean   _interpolated = false; // FIX! do I need this?
  public boolean   _done = false; // FIX! do I need this?

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
    private final int _interpolationType;

    public Quantiles _qbins[];

    public BinTask2 (double quantile, int max_qbins, double valStart, double valEnd, 
      boolean multiPass, int interpolationType)
      { 
        _quantile = quantile; 
        _max_qbins = max_qbins; 
        _valStart = valStart; 
        _valEnd = valEnd; 
        _multiPass = multiPass; 
        _interpolationType = interpolationType; 
      }

    @Override public void map(Chunk[] cs) {
      _qbins = new Quantiles[cs.length];
      for (int i = 0; i < cs.length; i++)
        _qbins[i] = new Quantiles(_fr.vecs()[i], _fr.names()[i], _quantile, _max_qbins,
          _valStart, _valEnd, _multiPass, _interpolationType).add(cs[i]);
    }

    @Override public void reduce(BinTask2 other) {
      for (int i = 0; i < _qbins.length; i++)
        _qbins[i].add(other._qbins[i]);
    }
  }

  // FIX! should use _max_qbins if available?
  public void finishUp(Vec vec) {
    // below, we force it to ignore length and only do [0]
    // need to figure out if we need to do a list and how that's returned
    _pctile = new double[QUANTILES_TO_DO.length];
    if ( _isEnum ) {
      ;
    } 
    else {
      if ( !_multiPass ) {
        _done = approxQuantilesOnePass(_pctile, QUANTILES_TO_DO);
      } 
      else {
        _done = exactQuantilesMultiPass(_pctile, QUANTILES_TO_DO);
      }
    }
  }

  public Quantiles(Vec vec, String name, double quantile, int max_qbins, 
        double valStart, double valEnd, boolean multiPass, int interpolationType) {

    colname = name;
    _isEnum = vec.isEnum();
    _isInt = vec.isInt();
    _domain = vec.isEnum() ? vec.domain() : null;
    _max = vec.max();
    _min = vec.min();
    _mean = vec.mean();
    _sigma = vec.sigma();
    _naCnt = vec.naCnt();

    _totalRows = 0;
    QUANTILES_TO_DO = new double[1];
    QUANTILES_TO_DO[0] = quantile;

    _valStart = valStart;
    _valEnd = valEnd;
    _valRange = valEnd - valStart;
    _multiPass = multiPass;
    _interpolationType = interpolationType;

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
        // Log.info("Q_ Multiple pass histogram starts at "+_valStart);
        // Log.info("Q_ _min "+_min+" _max "+_max);
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
        // Log.info("Q_ Single pass histogram has "+nbin2+" bins");
        // Log.info("Q_ Single pass histogram starts at "+_start2);
        // Log.info("Q_ _min "+_min+" _max "+_max);
        // can't make any assertion about _start2 vs _min (either can be slightly smaller: fp)
        hcnt2 = new long[nbin2];
        hcnt2_min = new double[nbin2];
        hcnt2_max = new double[nbin2];
      }
    } 
    else { // vec does not contain finite numbers
      // do we care here? have to think about whether multiPass is disabled/
      // okay this one entry hcnt2 stuff is making the algo die ( I guess the min was nan above)
      // for now, just make it length 2
      _start2 = vec.min();
      _binsz2 = Double.POSITIVE_INFINITY;
      hcnt2 = new long[2];
      hcnt2_min = new double[2];
      hcnt2_max = new double[2];
    }
    hcnt2_low = 0;
    hcnt2_high = 0;
    hcnt2_high_min = 0;
    // hcnt2 implicitly zeroed on new 
  }

  public Quantiles(Vec vec, String name) {
    // default to single pass median approximation?
    this(vec, name, 0.5, 1000, vec.min(), vec.max(), false, 7);
  }

  public Quantiles add(Chunk chk) {
    for (int i = 0; i < chk._len; i++)
      add(chk.at0(i));
    return this;
  }
  public void add(double val) {
    if ( Double.isNaN(val) ) return;
    // can get infinity due to bad enum parse to real
    // histogram is sized ok, but the index calc below will be too big
    // just drop them. not sure if something better to do?
    if( val==Double.POSITIVE_INFINITY ) return;
    if( val==Double.NEGATIVE_INFINITY ) return;

    _totalRows++;
    if ( _isEnum ) return;

    long maxBinCnt = _valMaxBinCnt;

    if ( !_multiPass  ) { // single pass approx
      long binIdx2;
      if (hcnt2.length==1) {
        binIdx2 = 0; // not used
      }
      else {
        binIdx2 = (int) Math.floor((val - _start2) / _binsz2);
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
      // don't care about bin edge leaks on the one pass algo
      // I suppose the hcnt2.length must be big enough?
    }
    else { // multi pass exact. Should be able to do this for both, if the valStart param is correct
      long binIdx2;
      // Need to count the stuff outside the bin-gathering, 
      // since threshold compare is based on total row compare
      double valOffset = val - _valStart;

      // FIX! do we really need this special case? Not hurting.
      if (hcnt2.length==1) {
        binIdx2 = 0;
      }
      else {
        binIdx2 = (int) Math.floor(valOffset / _valBinSize);
      }
      int binIdx2Int = (int) binIdx2;

      // we always need the start condition in the bins?
      // if ( valOffset < 0 ) {
      // if ( binIdx2Int < 0 ) { // works 3/10/14
      // maybe some redundancy in two compares
      if ( valOffset < 0 || binIdx2Int<0 ) { 
        ++hcnt2_low;
      }
      // we always need the end condition in the bins?
      // else if ( val > _valEnd ) {
      // else if ( binIdx2Int >= maxBinCnt ) { // works 3/10/14
      // would using valOffset here be less accurate? maybe some redundancy in two compares
      // can't use maxBinCnt-1, because the extra bin is used for one value (the bounds)
      else if ( val > _valEnd || binIdx2>=maxBinCnt ) { 
        if ( (hcnt2_high==0) || (val < hcnt2_high_min) ) hcnt2_high_min = val;
        ++hcnt2_high;
      } 
      else {
        assert (binIdx2Int >= 0 && binIdx2Int < hcnt2.length) : 
          "binIdx2Int too big for hcnt2 "+binIdx2Int+" "+hcnt2.length;
        // Log.info("Q_ (multi) val: "+val+" valOffset: "+valOffset+" _valBinSize: "+_valBinSize);
        assert (binIdx2Int>=0) && (binIdx2Int<=maxBinCnt) : "binIdx2Int "+binIdx2Int+" out of range";

        if ( hcnt2[binIdx2Int]==0 || (val < hcnt2_min[binIdx2Int]) ) hcnt2_min[binIdx2Int] = val;
        if ( hcnt2[binIdx2Int]==0 || (val > hcnt2_max[binIdx2Int]) ) hcnt2_max[binIdx2Int] = val;
        ++hcnt2[binIdx2Int];

        // For debug/info, can report when it goes into extra bin needed due to fp fuzziness
        // not an error! should be protected by newValEnd below, and nextK 
        // estimates should go into the extra bin if interpolation is needed
        if ( false && (binIdx2 == (maxBinCnt-1)) ) {
            Log.info("\nQ_ FP! val went into the extra maxBinCnt bin:"+
              binIdx2+" "+hcnt2_high_min+" "+valOffset+" "+
              val+" "+_valStart+" "+hcnt2_high+" "+val+" "+_valEnd,"\n");
        }
      }
    }
  } 

  public Quantiles add(Quantiles other) {
    _totalRows += other._totalRows;
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

    // 3 new things to merge for multipass histgrams (counts above/below the bins, and the min above the bins)
    assert !Double.isNaN(other.hcnt2_high) : "NaN in hcnt2_high merging";
    assert !Double.isNaN(other.hcnt2_low) : "NaN in hcnt2_low merging";
    assert other.hcnt2_high==0 || !Double.isNaN(other.hcnt2_high_min) : "NaN in hcnt2_high_min merging";

    // these are count merges
    hcnt2_low = hcnt2_low + other.hcnt2_low;
    hcnt2_high = hcnt2_high + other.hcnt2_high;

    // hcnt2_high_min validity is hcnt2_high!=0 (count)
    if (other.hcnt2_high > 0) {
      if ( hcnt2_high==0 || ( other.hcnt2_high_min < hcnt2_high_min )) {
        hcnt2_high_min = other.hcnt2_high_min;
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

  private boolean exactQuantilesMultiPass(double[] qtiles, double[] thres) {
    // do we need all of these as output?
    double newValStart, newValEnd, newValRange, newValBinSize;
    // FIX! figure out where unitialized can be used
    newValStart = Double.NaN; 
    newValEnd = Double.NaN;
    newValRange = Double.NaN;
    newValBinSize = Double.NaN;
    long newValLowCnt;
    long maxBinCnt = _valMaxBinCnt;
    assert maxBinCnt>1;
    long desiredBinCnt = maxBinCnt - 1;

    assert !_isEnum;
    if( hcnt2.length == 0 ) return false;
    // Tried creating relative NUDGE values to make sure bin range
    // is always inclusive of target.
    // ratio it down from valBinSize?  It doesn't need to be as big as valBinSize.
    // can't seem to make it work yet. leave NUDGE=0
    // Doesn't seem necessary? getting exact comparisons to other tools with NUDGE=0
    double NUDGE = 0;
    //  everything should either be in low, the bins, or high
    double threshold = thres[0];
    long totalBinnedRows = htot2(hcnt2_low, hcnt2_high);
    Log.info("Q_ totalRows check: "+_totalRows+" "+totalBinnedRows+" "+hcnt2_low+" "+hcnt2_high);
    assert _totalRows==totalBinnedRows : _totalRows+" "+totalBinnedRows+" "+hcnt2_low+" "+hcnt2_high;

    //  now walk thru and find out what bin to look inside
    long currentCnt = hcnt2_low;
    double targetCntFull = threshold * (_totalRows-1);  //  zero based indexing
    long targetCntInt = (long) Math.floor(targetCntFull);
    double targetCntFract = targetCntFull  - (double) targetCntInt;
    assert (targetCntFract>=0) && (targetCntFract<=1);
    Log.info("Q_ targetCntInt: "+targetCntInt+" targetCntFract: "+targetCntFract);

    int k = 0;
    while((currentCnt + hcnt2[k]) <= targetCntInt) {
      // Log.info("Q_ Looping for k (multi): "+k+" "+currentCnt+" "+targetCntInt+" "+_totalRows+" "+hcnt2[k]+" "+hcnt2_min[k]+" "+hcnt2_max[k]);
      currentCnt += hcnt2[k];
      ++k; // goes over in the equal case?
      // have to keep cycling till we get to a non-zero hcnt
      // but need to break if we get to the end (into the extra bin)
      if ( k == maxBinCnt ) break;
      assert k<maxBinCnt : "k too large, k: "+k+" maxBinCnt: "+maxBinCnt+" "+currentCnt+" "+targetCntInt;
    }
    Log.info("Q_ Found k (multi): "+k+" "+currentCnt+" "+targetCntInt+" "+_totalRows+" "+hcnt2[k]+" "+hcnt2_min[k]+" "+hcnt2_max[k]);


    assert hcnt2[k]!=1 || hcnt2_min[k]==hcnt2_max[k];

    // we can do mean and linear interpolation, if we don't land on a row
    // WATCH OUT when comparing results if linear interpolation...it's dependent on 
    // the number of rows in the dataset, not just adjacent values. So if you skipped a row
    // for some reason (header guess?) in a comparison tool, you can get small errors
    // both type 2 and type 7 give exact answers that match alternate tools 
    // (if they do type 2 and 7). scklearn doesn't do type 2 but does do type 7 
    // (but not by default in mquantiles())

    // the linear interpolation for k between row a (vala) and row b (valb) is
    //    pctDiff = (k-a)/(b-a)
    //    dDiff = pctDiff * (valb - vala)
    //    result = vala + dDiff

    boolean done = false;
    double guess = Double.NaN;
    boolean interpolated = false;
    assert (_interpolationType==2) || (_interpolationType==7) : "Unsupported type "+_interpolationType;

    double pctDiff, dDiff;
    if ( currentCnt==targetCntInt ) {
      if ( hcnt2[k]>2 && (hcnt2_min[k]==hcnt2_max[k]) ) {
        guess = hcnt2_min[k];
        done = true;
        Log.info("Q_ Guess A "+guess);
      } 
      else if ( hcnt2[k]==2 ) {
        // no mattter what size the fraction it would be on this number
        if ( _interpolationType==2 ) { // type 2 (mean)
          guess = (hcnt2_max[k] + hcnt2_min[k]) / 2.0;
        }
        else { // default to type 7 (linear interpolation)
          // Unlike mean, which just depends on two adjacent values, this adjustment  
          // adds possible errors related to the arithmetic on the total # of rows.
          dDiff = hcnt2_max[k] - hcnt2_min[k]; // two adjacent..as if sorted!
          pctDiff = targetCntFract; // This is the fraction of total rows
          guess = hcnt2_min[k] + (pctDiff * dDiff);
        }
        done = true;
        Log.info("Q_ Guess B "+guess+" with type "+_interpolationType+" targetCntFract: "+targetCntFract);
      } 
      else if ( (hcnt2[k]==1) && (targetCntFract==0) ) {
        assert hcnt2_min[k]==hcnt2_max[k];
        guess = hcnt2_min[k];
        done = true;
        Log.info("Q_ k"+k);
        Log.info("Q_ Guess C "+guess);
      } 
      else if ( hcnt2[k]==1 && targetCntFract!=0 ) {
        assert hcnt2_min[k]==hcnt2_max[k];
        Log.info("Q_ Single value in this bin, but fractional means we need to interpolate to next non-zero");

        int nextK;
        if ( k<maxBinCnt ) nextK = k + 1; //  could put it over maxBinCnt
        else nextK = k;
        // definitely see stuff going into the extra bin, so search that too!
        while ( (nextK<maxBinCnt) && (hcnt2[nextK]==0) ) ++nextK;

        assert nextK > k : k+" "+nextK;
        //  have the "extra bin" for this
        double nextVal;
        if ( nextK >= maxBinCnt ) {
          assert hcnt2_high!=0;
          Log.info("Q_ Using hcnt2_high_min for interpolate: "+hcnt2_high_min);
          nextVal = hcnt2_high_min;
        } 
        else {
          Log.info("Q_ Using nextK for interpolate: "+nextK);
          assert hcnt2[nextK]!=0;
          nextVal = hcnt2_min[nextK];
        }

        Log.info("Q_         k hcnt2_max[k] nextVal");
        Log.info("Q_ hello3: "+k+" "+hcnt2_max[k]+" "+nextVal);
        Log.info("Q_ \nInterpolating result using nextK: "+nextK+ " nextVal: "+nextVal);

        // OH! fixed bin as opposed to sort. Of course there are gaps between k and nextK

        if ( _interpolationType==2 ) { // type 2 (mean)
          guess = (hcnt2_max[k] + nextVal) / 2.0;
          pctDiff = 0.5;
        }
        else { // default to type 7 (linear interpolation)
          dDiff = nextVal - hcnt2_max[k]; // two adjacent, as if sorted!
          pctDiff = targetCntFract; // This is the fraction of total rows
          guess = hcnt2_max[k] + (pctDiff * dDiff);
        }

        interpolated = true;
        done = true; //  has to be one above us when needed. (or we're at end)
        Log.info("Q_ Guess B "+guess+" with type "+_interpolationType+
          " targetCntFull: "+targetCntFull+" targetCntFract: "+targetCntFract+
          " _totalRows: " + _totalRows);
      }
      else {
        guess = Double.NaN; // don't bother guessing, since we don't use till the end
        done = false;
      }
    }
    if ( !done ) {

      // Possible bin leakage at start/end edges due to fp arith.
      // bin index arith may resolve OVER the boundary created by the compare for 
      // hcnt2_high compare. 
      // I suppose just one value should be in desiredBinCnt+1 bin -> the end value?)

      // To cover possible fp issues:
      // See if there's a non-zero bin below (min) or above (max) you, to avoid shrinking wrong.
      // Just need to check the one bin below and above k, if they exist. 
      // They might have zero entries, but then it's okay to ignore them.
      // update: use the closest edge in the next bin. better forward progress for small bin counts
      // This code may make the practical min bin count around 4 or so (not 2).
      // what has length 1 hcnt2 that makese this fail? Enums? shouldn't get here.
      newValStart = hcnt2_min[k];
      if ( k > 0 ) {
        if ( hcnt2[k-1]>0 && (hcnt2_max[k-1]<hcnt2_min[k]) ) {
          newValStart = hcnt2_max[k-1];
        }
      }

      // subtle. we do sometimes put stuff in the extra end bin (see above)
      // k might be pointing to one less than that (like k=0 for 1 bin case)
      newValEnd = hcnt2_max[k];
      if ( k < (maxBinCnt-1) )  {
        assert k+1 < hcnt2.length : k+" "+hcnt2.length+" "+_valMaxBinCnt+" "+_isEnum+" "+_isInt;
        if ( hcnt2[k+1]>0 && (hcnt2_min[k+1]>hcnt2_max[k]) ) {
          newValEnd = hcnt2_min[k+1];
        }
      }

      newValRange = newValEnd - newValStart ;

      //  maxBinCnt is always binCount + 1, since we might cover over due to rounding/fp issues?
      newValBinSize = newValRange / (desiredBinCnt + 0.0);
      newValLowCnt = currentCnt - 1; // is this right? don't use for anything (debug?)
      if ( newValBinSize==0 ) {
        //  assert done or newValBinSize!=0 and live with current guess
        Log.info("Q_ Assuming done because newValBinSize is 0.");
        Log.info("Q_ newValRange: "+newValRange+
          " hcnt2[k]: "+hcnt2[k]+
          " hcnt2_min[k]: "+hcnt2_min[k]+
          " hcnt2_max[k]: "+hcnt2_max[k]);
        guess = newValStart;
        Log.info("Q_ Guess E "+guess);
        done = true;
      }
    }

    Log.info("Q_ guess: "+guess+" done: "+done+" hcnt2[k]: "+hcnt2[k]);
    Log.info("Q_ currentCnt: "+currentCnt+" targetCntInt: "+targetCntInt+" hcnt2_low: "+hcnt2_low+" hcnt2_high: "+hcnt2_high);
    Log.info("Q_ was "+_valStart+" "+_valEnd+" "+_valRange+" "+_valBinSize);
    Log.info("Q_ next "+newValStart+" "+newValEnd+" "+newValRange+" "+newValBinSize);

    qtiles[0] = guess;
    // Log.info(]: hcnt2[k]: "+hcnt2[k]+" hcnt2_min[k]: "+hcnt2_min[k]+
    //  " hcnt2_max[k]: "+hcnt2_max[k]+" _binsz2: "+_binsz2+" guess: "+guess+" k: "+k+"\n");

    // Don't need these any more
    hcnt2 = null;
    hcnt2_min = null;
    hcnt2_max = null;
    _newValStart = newValStart;
    _newValEnd = newValEnd;
    _interpolated = interpolated;
    return done;
  }

  private boolean approxQuantilesOnePass(double[] qtiles, double[] thres){
    // not called for enums
    assert !_isEnum;
    if( hcnt2.length == 0 ) return false;

    int k = 0; long s = 0;
    double guess = Double.NaN;
    _interpolated = false;
    double actualBinWidth = 0;
    assert _totalRows==htot2(0, 0) : "_totalRows: "+_totalRows+" htot2(): "+htot2(0, 0);

    // A quantile definition. (linear interpolation?)
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
      double s1 = Math.floor(thres[j] * (double) _totalRows); 
      if ( s1 == 0 ) {
        s1 = 1; // always need at least one row
      }
      // what if _totalRows is 0?. just return above?. Is it NAs?
      // assert _totalRows > 0 : _totalRows;
      if( _totalRows == 0 ) return false;
      assert 1 <= s1 && s1 <= _totalRows : s1+" "+_totalRows;
      // how come first bins can be 0? Fixed. problem was _start. Needed _start2. still can get some
      while( (s+hcnt2[k]) < s1) { // important to be < here. case: 100 rows, getting 50% right.
        s += hcnt2[k];
        k++;
      }
      Log.info("Q_ Found k: "+k+" "+s+" "+s1+" "+_totalRows+" "+hcnt2[k]+" "+hcnt2_min[k]+" "+hcnt2_max[k]);

      // All possible bin boundary issues 
      if ( s==s1 || hcnt2[k]==0 ) {
        if ( hcnt2[k]!=0 ) {
          guess = hcnt2_min[k];
          Log.info("Q_ Guess A: "+guess+" "+s+" "+s1);
        }
        else {
          if ( k==0 ) { 
            assert hcnt2[k+1]!=0 : "Unexpected state of starting hcnt2 bins";
            guess = hcnt2_min[k+1];
            // Log.info("Q_ Guess B: "+guess+" "+s+" "+s1);
          }
          else {
            if ( hcnt2[k-1]!=0 ) {
              guess = hcnt2_max[k-1];
              // Log.info("Q_ Guess C: "+guess+" "+s+" "+s1);
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
        _interpolated = true;
        // Log.info("Q_ Guess D: "+guess+" "+k+" "+hcnt2_min[k]+" "+actualBinWidth+" "+s+" "+s1+" "+hcnt2[k]);
      }

      qtiles[j] = guess;

      // might have fp tolerance issues here? but fp numbers should be exactly same?
      Log.info("Q_ hcnt2[k]: "+hcnt2[k]+" hcnt2_min[k]: "+hcnt2_min[k]+
        " hcnt2_max[k]: "+hcnt2_max[k]+" _binsz2: "+_binsz2+" guess: "+guess+" k: "+k+"\n");

      // Don't need these any more
      hcnt2 = null;
      hcnt2_min = null;
      hcnt2_max = null;

    }
    return true;
  }
}
